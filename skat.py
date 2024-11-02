#!/usr/bin/env python3
# coding: UTF-8

""" 
Skatpy is a scatter/gather service that breaks files up into smaller parts, spreads those files to connected nodes and writes a manifest file that
contains the the names and order of the file parts to retrieve and assemble the original file.

It is just a hobby project that I started playing with in 2010 from a curiosity of how P2P file sharing applications work. Updated in 2024 to run on Python 3.

Origin: https://github.com/jyfletcher/skatpy

Copyright (C) 2024 Justin Yates Fletcher

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <https://www.gnu.org/licenses/>.
"""

import os
import time
import random
import shutil
from traceback import print_exc
import gzip
import threading
import logging
from select import select
from socket import socket, AF_INET, SOCK_STREAM, SHUT_RDWR
import hashlib

##
# Verbose output
DEBUG = True

##
# The maximum size of the data store 
MAX_STORE_SIZE = 1024**3 * 5 # 5GB

##
# This is the directory to watch for new .skt or other files
# If it is a .skt file then the contents will be downloaded
# If it is any other file type then it will be inserted
# It also acts as the base folder for the needed sub-folders.
SKAT_DIR = os.path.expanduser(os.path.join("~", "skat"))

##
# This is where finished files will be placed.  Either after inserting or
# downloading.  After an insertion the corresponding .skt will be found here
DONE_DIR = os.path.join(SKAT_DIR, "done")

##
# This is the data store directory... it will be automatically managed
DATA_DIR = os.path.join(SKAT_DIR, "data")

##
# This is for storing temporary files.  If Skat is not running then it is
# safe to delete anything in this directory though Skat will do this
# automatically during startup.
# TODO:  I'm not really using the temp dir.  Might remove it.
TEMP_DIR = os.path.join(SKAT_DIR, "temp")

##
# This is the folder containing peers and peer information
PEER_DIR = os.path.join(SKAT_DIR, "peers")

##
# This is the folder containing keys needed to be retrieved
QUEUE_DIR = os.path.join(SKAT_DIR, "queue")

# Don't touch these unless you know what you are doing
CHUNK_SIZE = 1024 * 1024 // 2 # 512KB
SLEEP = 0.1
DATA_DIR_SPLIT = 1009 # Should be a prime number, but not too large
SKT_SEP = '|'
VERSION = "0.1a"
HOST = ''
PORT = 3471
COMMAND_LEN = 3 # GET, RND, etc...
KEY_LEN = 128 # SHA512
BUFF_SIZE = 2048 # Best as a power of 2


def main():
    # Set up logging to console
    logging.basicConfig()
    logging.root.setLevel(logging.NOTSET)
    logging.basicConfig(level=logging.NOTSET)
    
    # Add or remove from here to include or exclude service threads
    services = {
                DirWatcher  : None,
                #Server      : None,
                #Collector   : None,
                QueueWatcher: None,
                #DataJanitor : None,
                }
    
    # Initialize and start
    for s in services.keys():
        services[s] = s()
        services[s].start()
        
    try:
        # Accept Enter or Ctrl-c to exit
        input("Press Enter to exit.\n")
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Exiting...")
    
        # Stop the threads
        for s in services.values():
            s.stop()

        # And wait for them to exit
        for s in services.values():
            s.join()



class BaseThread(threading.Thread):
    """ A base class for the various threading classes to avoid
    code duplication and maintain consistency... no way to enforce this in
    Python
    
    This class should only be inherited from and never called directly.
    """
    name = "BaseThread"
    
    def __init__(self):
        super(BaseThread, self).__init__()
        self.running = True
        
    def run(self):
        logging.info(self.name + " started.")
        
    def stop(self):
        logging.info(self.name + " stopped.")
        self.running = False

class NetPull(object):
    
    def __init__(self, peer):
        """
        
        """
        self.peer = peer
        self.host = peer['host']
        self.port = int(peer['port'])
        
    def get_random(self):
        try:
            sock = socket(AF_INET, SOCK_STREAM)
            # TODO : This needs a sane timeout...  maybe the default is OK
            sock.connect((self.host, self.port))
            msg = 'RND'
            # Loop over attempting to send the message
            to_send = msg
            while to_send != "":
                bytes_sent = sock.send(to_send)
                # remove what has been sent from to_send
                to_send = to_send[bytes_sent:]
                
            logging.info("Sent RND")
            # Retrieve the data
            data = ""
            while True:
                tmp = sock.recv(BUFF_SIZE)
                if not tmp:
                    logging.info("All data received.")
                    break
                # If we get here we should have data.
                data += tmp
            if data == "":
                logging.info("No actual data received.")
                data = None # Try to always return None when fitting.
            sock.close()
            return data
            
        except:
            logging.info("Unable to connect to peer: %s" % self.peer)
            sock.close()
            # No data.
            return None
            
    def get_key(self, key):
        try:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect((self.host, self.port))
            
            msg = 'GET%s' % key
            to_send = msg
            while to_send != '':
                sent = sock.send(to_send)
                to_send = to_send[sent:]
            logging.info("Sent %s" % msg)
            
            # Retrieve the data
            data = ""
            while True:
                tmp = sock.recv(BUFF_SIZE)
                if not tmp:
                    logging.info("All data received.")
                    break
                # If we get here we should have data.
                data += tmp
            if data == "":
                logging.info("No actual data received.")
                data = None # Try to always return None when fitting.
            sock.close()
            return data
        except:
            logging.info("Unable to connect to peer: %s" % self.peer)
            sock.close()
            return None
            
    def get_port(self, client_socket):
        """
        Using the given client_socket we will attempt to connect and
        get the PORT number used by that client.
        """
        msg = "PRT"
        to_send = msg
        while to_send != "":
            sent = client_socket.send(to_send)
            to_send = to_send[sent:]
        logging.info("Sent %s" % msg)
        
        # Retrieven the data
        data = ""
        while True:
            tmp = client_socket.recv(BUFF_SIZE)
            if not tmp:
                logging.info("Add data recevied.")
                break
            data += tmp
        if data == "":
            logging.info("No actual data recevied.")
            data = None
            return data


class Collector(BaseThread):
    """  The Collector slowly pulls random keys from peers.
    
    Initially there will be only a few peers, but as this node connects
    to these few it will join their peer list.  Then as data is being searched
    for this node will be returned in the results.  As this node sends data
    to others it will gain peers.  The idea is that the longer a node is on
    distributing chunks the more peers it will see and be seen by encouraging
    running a node for a long time.  See the comments in the Peer() class.
    """
    name = 'Collector'
    
    def __init__(self):
        super(Collector, self).__init__()
        self.peerstore = PeerStore()
        self.ds = DataStore()
        
    def run(self):
        super(Collector, self).run()
        while self.running:
            time.sleep(10)
            # we could have been asked to shut down while sleeping
            if not self.running:
                break
            # Get one peer and try to pull data from them.
            # Since this is the Collector we should not focus on one peer
            # but instead just try to connect to many for hopefully
            # better data distribution.
            peer = self.peerstore.get()
            if peer is None:
                logging.info("No available peers.")
                continue
            netpull = NetPull(peer)
            data = netpull.get_random()
            if data is not None:
                self.ds.put(data)

            # return the peer whether we got data or not.
            self.peerstore.put(peer)

        
class Server(BaseThread):
    """ One thread to run the Server.  Connections are handled through select()
    
    The Server portion should never have long term sessions.  Commands should
    be passed in, data returned, and then the session is closed.
    
    TODO:  This thread should handle adding to the peer list.  When a node
    connects it should be assumed that the connecting node is a potential peer
    and be added to the list.  Think about connection verification.  If the
    connecting peer cannot be communicated with then it should be considered
    a leech and dealt with accordingly.  Though it might be better to handle
    leeches in the Collector().
    
    TODO:  The part of adding the potential peer needs to be written. 
    """
    # TODO: Rather than relying on the length of commands and keys it would
    # be best to add a token or something.  I think this would make changes
    # easier.
    name = 'Server'
    
    def __init__(self, host=HOST, port=PORT):
        super(Server, self).__init__()
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((host, port))
        self.sock.listen(5)
        
        self.socks = [] # Client sockets
        self.socks.append(self.sock)
        
        self.peerstore = PeerStore()
        self.data = {}
        
    def run(self):
        super(Server, self).run()
        while self.running:
            # Run a select on the sockets.  Timeout after 3 secs so the 
            # thread can exit, if asked to.
            r, w, x = select(self.socks, self.socks, self.socks, 3) # 3 secs
            
            for sock in r:
                if sock == self.sock:
                    logging.info("Connection Started.  Adding client.")
                    client, addr = sock.accept()
                    # When we get an incoming connection we don't yet know
                    # what their PORT should be.
                    # TODO: Figure out a way to handle that.
                    # Maybe we need a new Command for grabbing port
                    self.socks.append(client)
                elif sock in self.socks:
                    # Retrieve the data
                    command = ""
                    while True:
                        if DEBUG:
                            logging.info("Starting to read command.")
                        tmp = sock.recv(BUFF_SIZE)
                        if len(tmp) < COMMAND_LEN:
                            logging.info("Potential partial command received.")
                            logging.info("  Waiting for more data.")
                        elif len(tmp) == COMMAND_LEN: # Received a command
                            logging.info("Finished reading bare.")
                            command += tmp
                            break
                        elif len(tmp) == COMMAND_LEN + KEY_LEN: # CMD+KEY
                            logging.info("Finished reading command and argument.")
                            command += tmp
                            break
                        elif len(tmp) > COMMAND_LEN + KEY_LEN:
                            logging.info("Command too long!")
                            logging.info("Command: %s" % tmp)
                            break
                        else:
                            logging.info("Why am I here???")
                            break
                            
                    # If we get here we should have data.
                    self.data[sock] = Command(command).execute()
                else:
                    logging.info("Socket ready for reading is not in list.")
                    self._closesock(sock)
                    
            for sock in w:
                if sock in self.socks and sock in self.data.keys():
                    if sock not in self.data:
                        logging.info("No socket key found in data.  Closing.")
                        self._closesock(sock)
                        continue
                    elif self.data[sock] is None:
                        logging.info("Socket found but no data.")
                        self._closesock(sock)
                        continue
                    # If we get here then we should have data to send.
                    logging.info("...Sending...Data...")
                    try:
                        sock.sendall(self.data[sock])
                        logging.info("  All bytes sent.  Closing.")
                        self._closesock(sock)
                        del(self.data[sock])
                    except:
                        # TODO:  Catch the right exception!!
                        # TODO:  I just saw a huge loop over this exception.
                        # Try to reproduce.  A added closing the sock as a
                        # temp fix.
                        logging.info("Error sending data...")
                        print_exc()
                        self._closesock(sock)
                        del(self.data[sock])
                    
            for sock in x:
                if sock in self.socks:
                    self._close(sock)
            
            
        # We've been told to exit, so close out the open sockets
        for sock in self.socks:
            self._closesock(sock)
                    
    def _closesock(self, sock):
        logging.info("Connection Closing.  Removing client.")
        #sock.shutdown(SHUT_RDWR)
        sock.close()
        # Remove the socket from the list.  It should exist, but check first.
        if sock in self.socks:
            self.socks.remove(sock)

class Command(object):
    """ A convenience class for handling the protocol commands.
    Commands are 3 characters long.  Anything after that is junk or perhaps
    a key depending on the command.
    
    The full data read from the socket needs to be passed in because the key
    may also be passed in.
    
    TODO:  It might be good to limit read sizes to a max so that a client
    cannot start sending tons of data and trashing the node/datastore.
    """
    def __init__(self, command):
        # verify that we are just reading the command, in case junk is written
        self.command = command[0:COMMAND_LEN]
        self.data = command[COMMAND_LEN:] # Get the rest of the command.
        logging.info("Command received: " + self.command)
        self.ds = DataStore()
        
        self.commands = {'GET' : self._get, 
                         'RND' : self._rnd,
                         'PRT' : self._prt,
                         }
    
    def execute(self):
        if DEBUG:
            logging.info("Processing command: %s" % self.command)
        if self.command in self.commands.keys():
            return self.commands[self.command]()
        
    def _get(self):
        key = self.data
        logging.info("Get key: %s" % key)
        data = self.ds.get(key)
        return data
        
    def _prt(self):
        if DEBUG:
            logging.info("Returning PORT: %s" % PORT)
        return PORT
    
    def _rnd(self):
        data = self.ds.randomkey()
        return data
        

class QueueWatcher(BaseThread):
    """
    This thread will scan the queue dir and attempt to download whatever
    it find there that needs downloading.
    """
    name = "QueueWatcher"
    
    def __init__(self):
        super(QueueWatcher, self).__init__()
        self.queue = Queue()
        self.ds = DataStore()
        self.peerstore = PeerStore()
    
    def run(self):
        super(QueueWatcher, self).run()
        
        # Assuming that if a peer has one needed file it may have others
        # we will keep a "good_peer" around for a little longer.
        good_peer = None
        
        while self.running:
            time.sleep(5)
            skt_folders = self.queue.get_all()
            for f in skt_folders:
                folder_name = os.path.join(QUEUE_DIR, f)
                keys = self.queue.parse(folder_name)
                if len(keys) == 0:
                    logging.info("We have a finished file.")
                    self.queue.assemble(folder_name)
                    self.queue.delete(folder_name)
                    
                # First try to get the keys from the datastore.
                for k in keys:
                    time.sleep(1)
                    # We might have been asked to exit
                    if not self.running:
                        break
                    logging.info("Getting data from datastore.")
                    data = self.ds.get(k)
                    if data is not None:
                        # Need save data in the corresponding queue folder
                        self.queue.save_data(folder_name, data)
                        continue
                    logging.info("Attempting Net download.")
                    # Not in the datastore.. Need to download.
                    peer = self.peerstore.get(request=good_peer)
                    # If peer is still None then we have no good peers
                    if peer is None:
                        logging.info("No peers available. Sleeping.")
                        continue
                    np = NetPull(peer)
                    data = np.get_key(k)
                    if data == None:  #No data.. bad peer
                        logging.info("No data from peer.")
                        good_peer = None
                        self.peerstore.put(peer)
                        continue
                    # If we are here then we have a good peer and data
                    good_peer = peer
                    self.queue.save_data(folder_name, data)
                    # Also add the data to the datastore
                    self.ds.put(data)
                    # And return the peer
                    self.peerstore.put(peer)



class Queue(object):
    """
    A convenience class for adding/getting .skt files to/from the queue.
    
    In the queue folder a sub-folder will be created.  In this subfolder
    there will be a .skt file that contains the keys.  For each key a file
    will be created matching that key as it is downloaded.  When the 
    QueueWatcher thread sees that all of the keys have been retrieved it will
    assemble them and write the output to the done folder.
    
    """
    
    def add(self, skt_file):
        """
        Add a new skt file to the Queue dir.  The output in the queue will
        be a folder named the same as the skt file, and inside the folder
        is a .skt file containing the contents of the orig skt file, plus
        files named for each key that has been  retrieved.
        
        Note:  skt_file should be the full path, or functional relative
        path, to the file.
        """
        inf = gzip.open(skt_file, 'rb')
        filename = os.path.basename(skt_file)
        os.mkdir(os.path.join(QUEUE_DIR, filename))
        outfname = os.path.join(QUEUE_DIR, filename, ".skt")
        if DEBUG:
            logging.debug("Queue: making new %s" % outfname)
        outf = gzip.open(outfname, 'wb')
        outf.write(inf.read())
        outf.close()
        inf.close()
        
    def assemble(self, skt_path):
        """
        When a queued skt file is finished we need to assemble all the parts
        into a complete file.
        """
        if not os.path.exists(skt_path):
            logging.info("Requests folder for assembly not in queue: %s" % skt_path)
            return
        skt_file = os.path.join(skt_path, ".skt")
        logging.info("Assembling skt: %s" % skt_path)
        outfname = os.path.basename(skt_path).replace(".skt", "")
        logging.info("Assembling file name: %s" % outfname)
        outf = open(os.path.join(DONE_DIR, outfname), 'wb')
        skt = Skat(skt_file)
        keys = skt.read()
        for k in keys:
            k = Key(key=k)
            f = gzip.open(os.path.join(skt_path, k.key), 'rb')
            logging.info("Assembling: %s for %s" % (k.key, outfname))
            outf.write(f.read())
            f.close()
        outf.close()
        
        
    def delete(self, folder_name):
        """
        Delete the skt folder and all contained data (remove from queue).
        """
        # Delete all the data in the folder
        shutil.rmtree(folder_name)
            
    
    def get_all(self):
        """
        Return a list of all skt folders needing processing.
        """
        dirs = os.listdir(os.path.join(QUEUE_DIR))
        ret = []
        for d in dirs:
            full_path = os.path.join(QUEUE_DIR, d)
            if os.path.isfile(full_path):
                continue
            ret.append(d)
        return ret
            
    
    def parse(self, skt_folder):
        """
        Parse the Queue looking for a folder matching the file name, parse
        the .skt file in that folder to get all the keys, determine which
        keys have already been loaded, and return a list of needed keys.
        """
        folder_name = os.path.basename(skt_folder)
        if folder_name not in os.listdir(QUEUE_DIR):
            logging.info("Requested folder not in queue: %s" % folder_name)
            return
        
        if not os.path.exists(os.path.join(QUEUE_DIR, folder_name, ".skt")):
            logging.info("Requested folder does not have a .skt file.")
            return
        
        # Remove any temp files
        for d in os.listdir(skt_folder):
            full_path = os.path.join(folder_name, d)
            if d.split(".")[-1] == "tmp":
                os.unlink(full_path)
        
        curr_skt = os.path.join(QUEUE_DIR, folder_name, ".skt")
        logging.info("Opening queue %s" % curr_skt)
        skt = Skat(curr_skt)
        keys = skt.read()
        existing = [e for e in os.listdir(skt_folder) if e != ".skt"]
        needed = [n for n in keys if n not in existing]
        if DEBUG:
            logging.info("Queue:  Needed keys %s" % needed)
        return needed
    
    def save_data(self, folder_name, data):
        """
        Save the corresponding data under the given folder.
        Generate the key from the data.
        """
        key = Key(data=data)
        final_name = os.path.join(folder_name, key.key)
        temp_name = final_name + ".tmp"
        zf = gzip.open(temp_name, 'wb')
        zf.write(data)
        zf.close()
        os.rename(temp_name, final_name)
        

class DataJanitor(BaseThread):
    """ A thread for watching the size of DATA_DIR.  Oldest files are 
    removed if MAX_STORE_SIZE is reached.
    
    Old Notes:
    This thread would be a good place to watch the size of the DataStore.
    I think it should basically work in a FIFO fashion where the oldest files 
    are deleted first.  Even if the oldest file is a very popular one there is
    no need for this node to hang on to it.  Popular files will by their
    very nature have good distribution so by deleting an old popular file
    from this node we lower local bandwidth stress and let the newer nodes
    holding the file take over the work.  There is an issue of being listed
    as a candidate for having a file if a cache system is implemented.  Some
    thought will need to be put into that... but there is no cache system
    currently.
    """
    name = "DataJanitor"
    
    def __init__(self):
        super(DataJanitor, self).__init__()
        self.delete_time = None
        
        
    def run(self):
        super(DataJanitor, self).run()
        while self.running:
            oldest_time = int(time.time())
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(DATA_DIR):
                if not filenames: continue
                if not self.running: break

                for filename in filenames:
                    if not self.running: break
                    
                    stats =  os.stat(os.path.join(dirpath, filename))
                    size = stats[6]
                    creation_time = stats[7]

                    if self.delete_time and creation_time <= self.delete_time:
                        os.unlink(os.path.join(dirpath, filename))
                        logging.info(filename + " deleted.")
                        continue
                    else:
                        total_size += size
                        if creation_time < oldest_time:
                            oldest_time = creation_time
                    
                    
                
                time.sleep(.2)
            logging.info("Store size: " + str(total_size/1024/1024) + "MB")
            logging.info("Oldest time: " + str(oldest_time))
            
            if total_size > MAX_STORE_SIZE:
                self.delete_time = oldest_time
                if DEBUG: logging.debug("Total Store size exceeded.")
            else:
                self.delete_time = None
            
            if not self.running: break
            time.sleep(10)
    

    
class DirWatcher(BaseThread):
    """ A thread for watching the the SKAT_DIR.  When files are found
    the name is checked for the .skt extension and then either Scatter()
    or Gather() is called. 
    
    """
    name = 'DirWatcher'
    
    def __init__(self):
        super(DirWatcher, self).__init__()
        self.queue = Queue()
        self.found = {}

    def check_nt(self, file_path):
        """
        Set self.found accordingly for a Windows NT (and later versions)
        platform.
        
        Returns True if the file is safe for processing.  False otherwise.
        """
        file_name = os.path.basename(file_path)
        if file_name not in self.found:
            if DEBUG:
                logging.debug("New file found.  Monitoring %s" % file_name)
            self.found[file_name] = False
        try:
            # See if we can rename the file
            os.rename(file_path, file_path + ".tmp")
            os.rename(file_path + ".tmp", file_path)
            del(self.found[file_name])
            return True
        except WindowsError:
            # If not then assume that the file is still being written to
            if DEBUG:
                logging.debug("File %s modified since last run " % file_name)
            return False 
        
    def check_posix(self, file_path):
        """
        Set self.found accordingly for a POSIX platform.
        
        Returns True if the file is safe for processing.  False otherwise.
        """
        file_name = os.path.basename(file_path)
        if file_name not in self.found:
            if DEBUG:
                logging.debug("New file found.  Monitoring %s" % file_name)
            self.found[file_name] = os.path.getsize(file_path)
            return False
        size = os.path.getsize(file_path)
        if size == self.found[file_name]:
            # File has not grown since last check.  Good to process.
            del(self.found[file_name])
            return True
        else:
            # Otherwise the file is still being written to so update size
            if DEBUG:
                logging.debug("File %s modified since last run " % file_name)
            self.found[file_name] = os.path.getsize(file_path)
            return False
        
    def run(self):
        super(DirWatcher, self).run()
        while(self.running):
            time.sleep(10)
            for file_name in os.listdir(SKAT_DIR):
                file_path = os.path.join(SKAT_DIR, file_name)
                if os.path.isdir(file_path):
                    continue
                if os.name == 'nt':
                    # Check the file in the Windows way
                    if not self.check_nt(file_path):
                        continue
                else:
                    # Hopefully all other platforms are more sane
                    if not self.check_posix(file_path):
                        continue
                # If we get here then the file should be good to process
                if file_name.split('.')[-1] == 'skt':
                    self.queue.add(file_path)
                    os.unlink(file_path)
                else:
                    Scatter(file_path).scatter()
                    os.unlink(file_path)
                    

            

class Skat(object):
    """ A convenience class for determining what to do with .skt files 
    Currently the .skt file will have the .skt extension added on to the
    name of the corresponding Gather()'d file.
    
    TODO:  I think it would be a good idea to optionally include a private
    key in the .skt file.  This would, again optionally, allow encrypting
    of data such that only the holders of specific .skt files can 
    retrieve data.  If encryption of data is requested then a key pair is
    generated and the public key is used to encrypt the pieces and the
    private key is included in the .skt file so that it can be used to
    decrypt the data chunks.  The public key is discarded after use.  I need
    to verify that key pairs can be generated without passwords.  That would
    be a pain, but I guess they could also be included in the .skt file.
    The other side of this is that users that want to insert encrypted data
    could just manually encrypt it and then insert the file, and either
    distribute keys manually or encrypt with public keys of the intended
    recipients.  So this doesn't have real high priority, but I think it is
    still a good idea.
    """
    def __init__(self, file_path):
        """
        
        Arguments
        file_path -- The full path to the file
        
        TODO: This should be able to handle being given regular file names
        to turn into skt files, and also existing skt files to be used
        for loading.
        """
        self.keys = []
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        if self.file_name.split(".")[-1] == 'skt':
            # We have a skt file
            self.out_file_name = os.path.join(DONE_DIR, self.file_name)
            self.read()
        else:
            # We have a regular file that needs to be made as skt
            self.out_file_name = os.path.join(DONE_DIR, self.file_name + ".skt")
        
        
    def add_key(self, key):
        self.keys.append(key)
    
    def save(self):
        zf = gzip.open(self.out_file_name, 'wb')
        for key in self.keys:
            line = "%s\n" % key
            zf.write(line.encode('utf-8')) # Unix style line endings by default
        zf.close()
        logging.info("Wrote skt file %s" % self.out_file_name)
        return self.keys

        
    def read(self):
        # Clear out self.keys since we are reading from scratch
        self.keys = []
        zf = gzip.open(self.file_path, 'rb') # should work with \n
        for line in zf.readlines():
            self.keys.append(line.strip())
        zf.close()
        return self.keys


class Scatter(object):
    """ A handler class for breaking up a file into chunks and sending them
    to the DataStore()
    
    """
    
    def __init__(self, file_path):
        
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.ds = DataStore()
        self.skt = Skat(file_path)
        
    def scatter(self):
        
        f = open(self.file_path, 'rb')
        
        logging.info("Inserting: %s" % self.file_name)
        
        data = f.read(CHUNK_SIZE)
        while(data):
            key = self.ds.put(data)
            if key is not None:
                self.skt.add_key(key.key)
                if DEBUG:
                    logging.debug("  Added Chunk: %s" %  key.key)
                
            data = f.read(CHUNK_SIZE)
        
        logging.info("Done inserting.")
        
        return self.skt.save()
        
        
class Gather(object):
    """ This is a handler class for gathering chunks from the data store
    and assembling them in the correct order.
    """
    
    
    def __init__(self, file_name):
        self.file_name, self.keys = Skat(file_name).read()
        self.tmp_file = self.file_name + ".tmp"
        self.ds = DataStore()
    
    def gather(self):
        logging.info("Gathering %s" % self.file_name)
        of = open(os.path.join(DONE_DIR, self.tmp_file), 'wb')
        
        for k in self.keys:
            #if not k: continue
            of.write(self.ds.get(k))
            if DEBUG: logging.debug("Read from chunk: " + k)
        
        of.close()
        oldf = os.path.join(DONE_DIR, self.tmp_file)
        newf = os.path.join(DONE_DIR, self.file_name)
        os.rename(oldf, newf)
        logging.info("Finished Gathering %s" % self.file_name)


class PeerStore(object):
    """  Peer list management
    
    A node will start with a few seed peers.  The Server() class will add
    to this peer list as other nodes connect to it.  This will create a system
    whereby the node is encouraged to stay online longer as it will mean it
    has more online and available peers to connect to.
    See the comments in Server() and Collector()
    
    TODO:  Re-think this good, bad, ugly thing...  it is starting to look 
    more ugly than good.
    TODO:  Need to store the port number inside the peer file.  Colons
    were a bad idea..  Don't work in windows.  

    """
    def __init__(self):
        pass
    
    
    def get(self, request=None):
        """
        Return a random peer dict.
        
        Return is in the form : {'host' : "", 'port' : int(""), ...}
        maybe eventually some other stuff...
        
        Arguments
        request - a dict returned by a previous get() in order to request
                    the same peer previously used.  Perhaps because it was
                    a good one.
        
        Peers are stored in the PEER_DIR as one file for each IP address.
        The first line of the file will contain the port number.
        Eventually more stuff may be added to the file.
        
        Note:  If an available peer cannot be found then None is returned.
        """
        # TODO: if the requested peer is not available...  what to do?
        
        peers = os.listdir(PEER_DIR)
        # Check to see if a specific peer was requested
        if request is not None:
            if request['host'] in peers:
                peer = self._load_peer(request['host'])
                return peer
            else:
                logging.info("Requested peer not available.  Returning random")

        # mix 'em up
        random.Random().shuffle(peers)
        for p in peers:
            f_ext = p.split(".")[-1]
            if f_ext == "inuse" or f_ext == "tmp":
                # Skip the ones marked as inuse
                continue
            # Claim this peer
            peer = self._load_peer(p)
            return peer
            
        # No available peers
        return None
        
    def _load_peer(self, peer_name):
        # TODO:  Need to catch an exception and continue looping
        # in case another thread grabs this peer before we can
        # rename it.  Race condition mitigated by making sure we
        # can rename the file, since a rename is an atomic operation.
        avail_name = os.path.join(PEER_DIR, peer_name)
        inuse_name = "%s%s" % (avail_name, ".inuse")
        os.rename(avail_name, inuse_name)
        peer_file = open(os.path.join(PEER_DIR, inuse_name), 'r')
        port = peer_file.readline().strip()
        peer_file.close()
        ret = {}
        ret['host'] = peer_name
        ret['port'] = int(port)
        return ret
        
    
    
    def put(self, peer):
        """
        Add or return a peer.  If the peer is found as "inuse" then it is
        it is renamed so that it is again available.  If there exists no
        such peer then it is added.
        
        Arguments:
        peer -- a dict() that at least contains the keys 'host' and 'port'
        """
        avail_name = os.path.join(PEER_DIR, peer['host'])
        inuse_name = "%s%s" % (avail_name, ".inuse")
        if os.path.exists(inuse_name):
            logging.info("Making peer %s available for use" % peer['host'])
            os.rename(inuse_name, avail_name)
        elif os.path.exists(avail_name):
            logging.info("Peer %s returned but not marked as inuse" % peer['host'])
            # Do nothing
        else:
            #must be a new peer
            temp_name = "%s%s" % (avail_name, ".tmp")
            f = open(avail_name, 'wb')
            f.write("%s" % peer['port'])
            f.write("\n") # Unix style line endings
            f.close()
            os.rename(temp_name, avail_name)
        

    def _clean_peer_store(self):
        """
        This should only be called once at program start.  It removes temp
        files and inuse files that are part of normal running operation
        but should not exist on a fresh start.
        """
        for f in os.listdir(PEER_DIR):
            f_path = os.path.join(PEER_DIR, f)
            if "tmp" in f:
                os.unlink(f_path)
            if "inuse" in f:
                avail_name = f_path.replace(".inuse", "")
                os.rename(f_path, avail_name) 
        

class DataStore(object):
    """ Class for managing the data store. 
    
    The DataStore() should never be called directly except for the wipe()
    method.  Access to the DataStore() should always be handled by
    other classes that manage the inserted and retrieved data.
    """
    
    # TODO: Think through how empty requests should be handled
    # Empty requests may never come in, but I guess it is possible.
    # TODO:  For some reason on my current test file the very last
    # key in the .skt file is this empty key.  Find out why.
    empty_key = "SHA001da39a3ee5e6b4b0d3255bfef95601890afd80709"
    
    def __init__(self):
        # Check for the existence of the data_dir and create if needed
        # TODO: Make better/More robust/Functional
        try:
            data_ok = True
            dirs = self._list()
            for i in range(0, DATA_DIR_SPLIT):
                if str(i) not in dirs:
                    data_ok = False
                    break
            if not data_ok:
                self.wipe()
                self._init_data_store()
        except:
            if DEBUG: print_exc()
            self.wipe()
            self._init_data_store()
    
    def clean(self):
        """
        This should only be called when the node starts.  .tmp files are
        are a normal part of running operation, but there should be none
        left over after shutdown.
        """
        logging.info("Beginning Data Dir Clean")
        for dp, dn, fn in os.walk(DATA_DIR):
            for f in fn:
                if f.split(".")[-1] == ".tmp":
                    fullpath = os.path.join(dp, f)
                    logging.info("Data Dir Clean:  Removing %s" % fullpath)
                    os.remove(fullpath) 
        logging.info("Finished Data Dir Clean")
    
    def put(self, data):
        key = Key(data=data)
        fname = os.path.join(DATA_DIR, key.node, key.key)
        tmpname = fname + ".tmp"
        # Check to see if the key already exists
        if os.path.exists(fname):
            logging.info("Key: %s already exists." % key.key)
            return None
        f = gzip.open(tmpname, 'wb')
        f.write(data)
        f.close()
        os.rename(tmpname, fname)
        logging.info("Wrote data: %s" % key.key)
        time.sleep(SLEEP)
        return key
        
    def get(self, key):
        key = Key(key=key)
        if DEBUG: logging.debug("Getting digest: %s" % key.key)
        datafile = os.path.join(DATA_DIR, key.node, key.key)
        if not os.path.exists(datafile):
            logging.info("Data not found in store. Returning.")
            return None
        f = gzip.open(datafile, 'rb')
        data = f.read()
        f.close()
        newkey = Key(data=data)
        if newkey.key != key.key: #@UndefinedVariable
            logging.info("Digest and data do not match...")
            return None
        # Sleep to help reduce system load
        time.sleep(SLEEP)
        return data
    
    def randomkey(self):
        # Returns (key, data)
        random.seed()
        node = random.randrange(0,DATA_DIR_SPLIT)
        f = self._list(str(node))
        if len(f):
            rkey = f[random.randrange(0, len(f))]
            return self.get(rkey)
        return None
        
    def wipe(self):
        # TODO: Clean up!
        logging.info("Wiping data store.")
        try:
            for i in self._list():
                for j in self._list(i):
                    os.unlink(os.path.join(DATA_DIR, i, j))
                    if DEBUG: logging.debug("  Deleted: %s/%s" % (i, j))
                    time.sleep(SLEEP)
                os.rmdir(os.path.join(DATA_DIR, i))
                if DEBUG: logging.debug("  Deleted: " + i)
            logging.info("Finished wiping data store.")
        except:
            if DEBUG: print_exc()
            logging.info("Data store wiped")
        
    def _list(self, node=''):
        return os.listdir(os.path.join(DATA_DIR, node))
        
    def _init_data_store(self):
        logging.info("Initializing Data Store")
        # If it does not exist then create the base folder
        if not os.path.exists(DATA_DIR):
            os.mkdir(DATA_DIR)
        # Loop through and create the sub-folders
        for i in range(0, DATA_DIR_SPLIT):
            os.mkdir(os.path.join(DATA_DIR, str(i)))
            if DEBUG: logging.debug("  Created: " + str(i))
        # Create the empty key
        # It may not be necessary to have this, but for now it seems good
        self.put("".encode('utf-8')) # Encode becuse it needs to be bytes


class Key(object):
    """ A convenience class for parsing and making available the 
    information contained in a key or generating a key from given data.
    
    Variables generated and available:
    data = The raw data
    node = The integer value of int(key, 16) % DATA_DIR_SPLIT 
    key = the string result of sha512(data).hexdigest()
    """
    def __init__(self, key=None, data=None):
        self.data = None
        self.node = None
        self.key = None
        if data is not None and key is None:
            self.load_data(data)
        elif key is not None and data is None:
            self.load_key(key)
        else:
            logging.info("Key initialized incorrectly.  Defaulting to empty.")
            
    def load_data(self, data):
        self.data = data
        self.key = hashlib.sha512(self.data).hexdigest()
        self.node = str(int(self.key, 16) % DATA_DIR_SPLIT)
        
    def load_key(self, key):
        self.key = key
        self.node = str(int(self.key, 16) % DATA_DIR_SPLIT)
                
        
def init_dirs():
    """ Make sure the skat base dir and subdirs exist. """
    dirdict = {'DONE_DIR' : DONE_DIR,
               'TEMP_DIR' : TEMP_DIR,
               'DATA_DIR' : DATA_DIR,
               'PEER_DIR' : PEER_DIR,
               'QUEUE_DIR': QUEUE_DIR,
               }
    if not os.path.exists(SKAT_DIR):
        os.mkdir(SKAT_DIR)
    for k in dirdict:
        d = dirdict[k]
        if not os.path.exists(d):
            if DEBUG:
                logging.debug("Creating dir: %s" % d)
            os.mkdir(d)

            
def clean_datadir():
    ds = DataStore()
    ds.clean()
    

def clean_peerstore():
    ps = PeerStore()
    ps._clean_peer_store()

                
if __name__ == '__main__':
    init_dirs()
    clean_datadir()
    clean_peerstore()
    main()

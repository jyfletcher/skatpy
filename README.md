# Skat

---

## Background

Skat is a distributed storage system idea I had many years ago. I never finished it but had some of the core ideas working so my itch was scratched so to speak.

The core idea is to take a large file, break it up into smaller pieces, take hashes of the smaller pieces, save the hash in an ordered manifest and distribute those pieces across the nodes in the network. This later allows those pieces to be download and reassembled using the manifest file, if the node is part of the network that has those pieces.
 
## Design

- The functionality is broken apart into isolated threads and communication among them is mostly done through the filesystem.
- Simple and small. Could be e-mailed or attached to a forum post along with a manifest.

## TODO

- Encryption - both in the network transport and the file storage
- Error Correction - FEC/ECC/par2-like/something
- Networking - change to http rather than the sketchy tcp socket stuff
- Lots of TODO's in the code. Many of them probably not relevant anymore


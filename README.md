# bct-server
Bluetooth Contact Tracing for Covid19 - server

# Documentation

https://docs.google.com/document/d/1c390iMNWVMDueZkqfhb7kF0XVQKXfHl5PHIAUu7GgTY/edit

# Design Decisions

* Each contact is a hex string
* Simple filesystem storage.  Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
* All contacts are also stored in memory
* On startup the filesystem is traversed to load data (there are various optimizations that can be done to reduce load time, such as checkpointing the local list of contacts
* Python/Flask server


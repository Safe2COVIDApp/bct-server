# bct-server
Bluetooth Contact Tracing for Covid19 - server

# Documentation

https://docs.google.com/document/d/1c390iMNWVMDueZkqfhb7kF0XVQKXfHl5PHIAUu7GgTY/edit

# Design Decisions

* Each contact is a hex string
* Simple filesystem storage.  Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
* All contacts are also stored in memory
* On startup the filesystem is traversed to load data (there are various optimizations that can be done to reduce load time, such as checkpointing the local list of contacts
* Python/Twisted server

# Prerequisits

The geocode support requires the python module Rtree which in turn requires libspatialindex

To install:
* brew install spatialindex (osx)
* apt install libspatialindex-dev (ubuntu)

# Running web Server

1. install requirements (pip install -r requirements.txt)
2. copy sample_config.py to config.py
3. edit config.py
4. ``python server.p [--config_file CONFIG-FILE]`` (if CONFIG-FILE is an http url, then it is fetched over the net)

# testing client
* ``pytest tests``

# trying client

* ``curl -i -X POST -H "Content-Type: application/json" -d '{  "memo":  {}, "contacts": [     { "id": "2345635"}]}' http://localhost:8080/red``
* ``curl -i -X POST -H "Content-Type: application/json" -d '{ "since":"202004100523",  "contact_prefixes":[  "234"]}' http://localhost:8080/green``
* ``curl -i  http://localhost:8080/sync?since=197001010000``


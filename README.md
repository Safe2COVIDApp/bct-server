[![CircleCI](https://circleci.com/gh/Safe2COVIDApp/bct-server.svg?style=svg)](https://circleci.com/gh/Safe2COVIDApp/bct-server)

	
# bct-server
Safe2 protocol server

# Documentation

https://bit.ly/safe2protocol

# Design Decisions

* Each contact is a hex string
* Simple filesystem storage.  
  * Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
  * Geograhic locations are stored in a 4 level directory structure ( TODO-DAN exapand )
* All contacts are also stored in memory
* On startup the filesystem is traversed to load data (there are various optimizations that can be done to reduce load time, such as checkpointing the local list of contacts
* Python/Twisted server

# Prerequisites

The geocode support requires the python module Rtree which in turn requires libspatialindex

To install:
* brew install spatialindex (osx)
* apt install libspatialindex-dev (ubuntu)

# Running on Docker
* docker run --rm -p 5000:5000 danaronson/safe2server:1.1

Either run the Dockerfile from this repo, or run from the docker repository once its installed there.

# Running web Server

1. install requirements (pip install -r requirements.txt)
2. copy sample_config.ini to config.ini
3. edit config.ini
4. ``python server.p [--config_file CONFIG-FILE]`` (if CONFIG-FILE is an http url, then it is fetched over the net)

# testing client
On Ubuntu
```
pip install pytest
pytest tests
```

Or on OSX
```
pip3 install pytest
PYTHON_BIN=python3 pytest tests
```
# trying client

* ``curl -i -X POST -H "Content-Type: application/json" -d '{  "memo":  {}, "contacts": [     { "id": "2345635"}]}' http://localhost:8080/status/send``
* ``curl -i -X POST -H "Content-Type: application/json" -d '{ "since":"2020-04-10T0523Z",  "contact_prefixes":[  "234"]}' http://localhost:8080/status/scan``
* ``curl -i  http://localhost:8080/sync?since=1970-01-01T0000Z``


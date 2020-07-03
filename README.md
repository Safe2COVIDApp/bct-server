[![CircleCI](https://circleci.com/gh/Safe2COVIDApp/bct-server.svg?style=svg)](https://circleci.com/gh/Safe2COVIDApp/bct-server)

	
# bct-server
Safe2 protocol server

# Documentation

https://bit.ly/safe2protocol

# Design Decisions

* Each contact is a hex string
* Simple filesystem storage.  
  * Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
  * Geographic locations are stored in a 4 level directory structure ( TODO-DAN expand )
* All contacts are also stored in memory
* On startup the filesystem is traversed to load data (there are various optimizations that can be done to reduce load time, such as check-pointing the local list of contacts
* Python/Twisted server

# Prerequisites

None - we used to use rtree which required libspatial, but no longer.

# Running on Docker
* docker run --rm -p 5000:5000 danaronson/safe2server:latest

Either run the Dockerfile from this repo, or run from the docker repository once its installed there.

# Running web Server

1. install requirements (pip install -r requirements.txt)
2. copy sample_config.ini to config.ini
3. edit config.ini
4. ``python server.py [--config_file CONFIG-FILE] [--log_level LOG-LEVEL]`` (if CONFIG-FILE is an http url, then it is fetched over the net, LOG_LEVEL overrides the logging level in the config file))

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

Pytest temporarily creates a server to test against, 
but you can test against a separate server instance (for example for performance testing) try:
```
python server.py --config_file sample_global_config.ini --log_level warn
pytest --server=http://localhost:8080 --log-cli-level warn tests/test_pseudoclient.py
```
log_level can be any of 'debug', 'info', 'warn', 'error', 'critical' and overrides whatever the config file says

# trying client

* ``curl -i -X POST -H "Content-Type: application/json" -d '{  "memo":  {}, "contacts": [     { "id": "2345635"}]}' http://localhost:8080/status/send``
* ``curl -i -X POST -H "Content-Type: application/json" -d '{ "since":"2020-04-10T21:47:00Z",  "contact_prefixes":[  "234"]}' http://localhost:8080/status/scan``
* ``curl -i  http://localhost:8080/sync?since=1970-01-01T0000Z``


# Heroku deployment

### Add Procfile

### Test with Curl

* `curl -i https://bct-server-staging.herokuapp.com/sync`
* `curl -i -X POST -H "Content-Type: application/json" -d '{ "since":"2020-04-10T21:47:00Z",  "contact_prefixes":["234"]}' https://bct-server-staging.herokuapp.com/status/scan`
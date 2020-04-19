import logging
from subprocess import Popen, PIPE, STDOUT
from tempfile import TemporaryDirectory
import socket
import pytest
import time
import requests
import sys
import shutil
from signal import SIGUSR1
import json
import os
import rtree
import hashlib
logger = logging.getLogger(__name__)

# default to python, but allow override 
python = os.environ.get('PYTHON_BIN', 'python')

class Data():
    def __init__(self):
        self.valid_ids = ['123456']
        self.locations_in =  [{ "lat": 37.773972, "long": -122.431297 }]
        self.locations_out = [{ "lat": 99.9999, "long": -99.999 }]
        self.locations_box = { "minLat": 37, 'maxLat': 39, 'minLong': -123, 'maxLong': -122}
        self.nonces = [ "123456789_0", "123456789_1", "123456789_2" ]
        return
    
class Server():
    def __init__(self, url, proc, directory):
        self.url = url
        self.proc = proc
        self.directory = directory
        return

    def sync(self):
        logger.info('before sync call')
        req = requests.get(self.url + 'sync')
        logger.info('after sync call')
        return req

    def _status(self, endpoint_name, contacts, locations, **kwargs):
        logger.info('before %s call' % endpoint_name)
        data = {}
        if contacts:
            data['contacts'] = contacts
        if locations:
            data['locations'] = locations
        data.update(kwargs)
        req = requests.post(self.url + endpoint_name,  json= data)
        logger.info('after %s call' % endpoint_name)
        return req

    def send_status(self, contacts = None, locations = None, **kwargs):
        return self._status('send_status', contacts, locations, **kwargs)

    def scan_status(self, contacts = None, locations = None, **kwargs):
        return self._status('scan_status', contacts, locations, **kwargs)

    def reset(self):
        logger.info('sending signal to server')
        for file_name in os.listdir(self.directory):
            file_path = os.path.join(self.directory, file_name)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        #os.kill(60328, SIGUSR1)
        self.proc.send_signal(SIGUSR1)
        logger.info('sent signal to server')
        return

    def get_data_from_id(self, contact_id):
        first_level = contact_id[0:2].upper()
        second_level = contact_id[2:4].upper()
        third_level = contact_id[4:6].upper()
        dir_name = "%s/%s/%s/%s" % (self.directory, first_level, second_level, third_level)
        logger.info('in gdfi')
        matches = []
        try:
            for file_name in os.listdir(dir_name):
                if file_name.endswith('.data'):
                    (code, date, ignore, extension) = file_name.split('.')
                    if code == contact_id:
                        matches.append(json.load(open(dir_name + '/' + file_name)))
        except:
            pass
        return matches

    def get_data_to_match_hash(self, match_term):
        # TODO-DAN I don't think this was right and it looks like its really in directory/rtree but that was a lucky guess - so wanted to check.
        idx = rtree.index.Index('%s/rtree' % (self.directory)) # WAS /Users/dan/tmp/rtree')
        matches = []
        for obj in idx.intersection(idx.bounds, objects = True):
            if match_term == obj.object['updatetoken']:
                matches.append(obj.object)
        return matches


    # The hash_nonce and verify_nonce functions are a pair that may be changed as the function changes.
    # verify(nonce, hashupdates(nonce)) == true;
    # TODO handling of nonce's still is waiting some decisions e.g. do we forward deleted messages ?
    # This code is duplicated between server and conftest.py - TODO-DAN is there a place they both access we should put this ?
    def hash_nonce(self, nonce):
        return hashlib.sha1(nonce).hexdigest()

    def verify_nonce(self, nonce, updatetoken):
        return hash_nonce(nonce) == updatetoken



    
        
@pytest.fixture(scope = "session")
def data():
    return Data()

@pytest.fixture(scope = "session")
def server():
    # setup server
    #yield Server('http://localhost:%s/' % 8080, None, '/Users/dan/tmp')
    #return
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('localhost', 0))
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
        #sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) | 1)
        port = sock.getsockname()[1]
    with TemporaryDirectory() as tmp_dir_name:
        logger.info('created temporary directory %s' % tmp_dir_name)
        config_file_name = tmp_dir_name + '/config.ini'
        open(config_file_name, 'w').write('[DEFAULT]\nDIRECTORY = %s\nLOG_LEVEL = INFO\nPORT = %d\nTesting = True\n' % (tmp_dir_name, port))
        with Popen([python, 'server.py', '--config_file', config_file_name], stderr = PIPE) as proc:
            # let's give the server some time to start
            logger.info('waiting for server to startup')
            #s = socket.create_connection(('localhost', port), timeout = 5.0)
            #s.close()
            time.sleep(2.0)
            logger.info('about to yield')
            #yield 'http://localhost:%s/' % port
            yield Server('http://localhost:%s/' % port, proc, tmp_dir_name)
            logger.info('back from yield')
            logger.info('before terminate, return code is %s' % proc.returncode)
            proc.terminate()
            for line in proc.stderr.readlines():
                logger.info('server output: %s' % line)
            logger.info('terminated')
    return

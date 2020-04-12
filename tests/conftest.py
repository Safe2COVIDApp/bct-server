
import logging
from subprocess import Popen, PIPE
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
logger = logging.getLogger(__name__)


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

    def red(self, contact):
        logger.info('before red call')
        req = requests.post(self.url + 'red',  json={  "memo":  {}, "contacts": [ contact ]})
        logger.info('after red call')
        return req

    def reset(self):
        logger.info('sending signal to server')
        for file_name in os.listdir(self.directory):
            file_path = os.path.join(self.directory, file_name)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        self.proc.send_signal(SIGUSR1)
        logger.info('sent signal to server')
        return

    def get_data_from_id(self, contact_id):
        first_level = contact_id[0:2].upper()
        second_level = contact_id[2:4].upper()
        third_level = contact_id[4:6].upper()
        dir_name = "%s/%s/%s/%s" % (self.directory, first_level, second_level, third_level)
        logger.info('in gdfi')
        try:
            for file_entry in os.scandir(dir_name):
                file_name = file_entry.name
                if file_name.endswith('.data'):
                    (code, date, extension) = file_name.split('.')
                    if code == contact_id:
                        return json.load(open(file_entry.path))
        except:
            logger.exception('foo')
        return None
        

@pytest.fixture(scope = "session")
def server():
    # setup server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('', 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                        sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) | 1)
        port = sock.getsockname()[1]
    with TemporaryDirectory() as tmp_dir_name:
        logger.info('created temporary directory %s' % tmp_dir_name)
        config_file_name = tmp_dir_name + '/config.ini'
        open(config_file_name, 'w').write('[DEFAULT]\nDIRECTORY = %s\nLOG_LEVEL = INFO\nPORT = %d\nTesting = True\n' % (tmp_dir_name, port))
        with Popen(['python', 'server.py', '--config_file', config_file_name], stderr = PIPE) as proc:
            # let's give the server some time to start
            logger.info('waiting for server to startup')
            time.sleep(0.5)
            logger.info('about to yield')
            #yield 'http://localhost:%s/' % port
            yield Server('http://localhost:%s/' % port, proc, tmp_dir_name)
            logger.info('back from yield')
            proc.terminate()
            for line in proc.stderr.readlines():
                logger.info('server output: %s' % line)
            logger.info('terminated')
    return

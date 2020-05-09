import logging
from subprocess import Popen, PIPE
from tempfile import TemporaryDirectory
import socket
import time
import requests
import shutil
from signal import SIGUSR1
import json
import os
from lib import update_token, replacement_token
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# default to python, but allow override 
python = os.environ.get('PYTHON_BIN', 'python')


class Server:
    def __init__(self, url, proc, directory):
        self.url = url
        self.proc = proc
        self.directory = directory
        return

    def __str__(self):
        return "<Server url: %s, directory: %s>" % (self.url, self.directory)

    def sync(self):
        logger.info('before sync call')
        req = requests.get(self.url + '/sync')
        logger.info('after sync call')
        return req

    def _status(self, endpoint_name, seed, contacts, locations, **kwargs):
        # contacts and locations should already have update_tokens if want that functionality
        #logger.info('before %s call' % endpoint_name)
        data = {}
        if seed and kwargs.get('replaces'):
            data['update_tokens'] = [
                update_token(replacement_token(seed, i))
                for i in range(kwargs.get('length'))]
        if contacts:
            data['contacts_id'] = contacts
        if locations:
            data['locations'] = locations
        headers = {}
        current_time = kwargs.get('current_time')
        if current_time:
            headers['X-Testing-Time'] = str(current_time)
        data.update(kwargs)
        # pdb.set_trace()
        logger.info("Sending %s: %s" % (endpoint_name, str(data)))
        req = requests.post(self.url + endpoint_name, json=data, headers=headers)
        #logger.info('after %s call' % endpoint_name)
        return req

    def send_status(self, seed=None, contacts=None, locations=None, **kwargs):
        return self._status('/status/send', seed, contacts, locations, **kwargs)

    def send_status_json(self, seed=None, contacts=None, locations=None, **kwargs):
        resp = self.send_status(seed=seed, contacts=contacts, locations=locations, **kwargs)
        assert resp.status_code == 200
        return resp.json()

    def scan_status(self, seed=None, contacts=None, locations=None, **kwargs):
        return self._status('/status/scan', seed, contacts, locations, **kwargs)

    def scan_status_json(self, seed=None, contacts=None, locations=None, **kwargs):
        resp = self.scan_status(seed=seed, contacts=contacts, locations=locations, **kwargs)
        assert resp.status_code == 200
        return resp.json()

    def status_update(self, seed=None, contacts=None, locations=None, **kwargs):  # Must have replaces
        return self._status('/status/update', seed, None, None, **kwargs)

    def status_update_json(self, seed=None, contacts=None, locations=None, **kwargs):  # Must have replaces
        resp = self.status_update(seed=seed, contacts=contacts, locations=locations, **kwargs)
        assert resp.status_code == 200
        return resp.json()

    def admin_status(self):
        logger.info('before admin_status call')
        resp = requests.get(self.url + '/admin/status')
        assert resp.status_code == 200
        logger.info('after admin_status call')
        return resp

    def admin_config(self):
        logger.info('before admin_config call')
        resp = requests.get(self.url + '/admin/config')
        assert resp.status_code == 200
        logger.info('after admin_config call')
        return resp

    def init(self, json_data):
        logger.info("before call to init")
        resp = requests.post(self.url + '/init', json=json_data)
        assert resp.status_code == 200
        return resp.json()

    def reset(self, delete_files = True):
        logger.info('sending signal to server')
        if delete_files:
            for file_name in os.listdir(self.directory):
                file_path = os.path.join(self.directory, file_name)
                if os.path.isfile(file_path) and (file_name not in ['log.txt']):
                    logger.info('file path is %s' % file_path)
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
        # os.kill(76617, SIGUSR1)
        self.proc.send_signal(SIGUSR1)
        logger.info('sent signal to server')
        return

    def get_data_from_id(self, contact_id, dict_type='contact_dict'):
        first_level = contact_id[0:2].upper()
        second_level = contact_id[2:4].upper()
        third_level = contact_id[4:6].upper()
        dir_name = "%s/%s/%s/%s/%s" % (self.directory, dict_type, first_level, second_level, third_level)
        logger.info('in get_data_from_id')
        matches = []
        try:
            for file_name in os.listdir(dir_name):
                if file_name.endswith('.data'):
                    (code, date_and_extension) = file_name.split(':')
                    if code == contact_id:
                        matches.append(json.load(open(dir_name + '/' + file_name)))
        except FileNotFoundError:
            pass
        return matches

    def get_data_to_match_hash(self, match_term):
        matches = []
        for root, sub_dirs, files in os.walk('%s/spatial_dict' % self.directory):
            for file_name in files:
                if file_name.endswith('.data'):
                    #    if match_term == obj.object['update_token']:
                    matches.append(json.load(open(root + '/' + file_name)))
        return matches


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('localhost', 0))
        return sock.getsockname()[1]


# this can be run as a primary server or a secondary one syncing from a primary one
#
# TODO-DAN note that currently uncommenting one line ignores the server created here and talks to a specific other server,
# TODO-DAN I'm sure that much of this could be bypassed in that case, but I am not sure how as it passes proc and tmp_dir_name to Server()
def run_server(server_urls=None, port=None):
    # setup server
    # yield Server('http://localhost:%s' % 8080, None, '/Users/dan/tmp')
    # return
    if not port:
        port = get_free_port()
    with TemporaryDirectory() as tmp_dir_name:
        logger.info('created temporary directory %s' % tmp_dir_name)
        config_file_path = tmp_dir_name + '/config.ini'
        log_file_path = tmp_dir_name + '/log.txt'
        config_data = '[DEFAULT]\nDIRECTORY = %s\nLOG_LEVEL = INFO\nPORT = %d\nTesting = True\n"BOUNDING_BOX_MINIMUM_DP = 2\nBOUNDING_BOX_MAXIMUM_SIZE = 0.001\nLOCATION_RESOLUTION = 4\nLOG_FILE_PATH = %s\n' % (
            tmp_dir_name, port, log_file_path)
        if server_urls:
            config_data += 'SERVERS = %s\nNEIGHBOR_SYNC_PERIOD = 1\n' % server_urls
        # config_data += '[APPS]\nTESTING_VERSION = 2.0\n'
        open(config_file_path, 'w').write(config_data)
        with Popen([python, 'server.py', '--config_file', config_file_path]) as proc:
            logger.info('waiting for server to startup')
            # let's give the server some time to start
            # Note 2.0 was too short
            time.sleep(3.0)
            logger.info('about to yield')
            url = 'http://localhost:%s' % port
            #url = 'http://localhost:%s' % "8080" # Just for debugging test
            yield Server(url, proc, tmp_dir_name)
            logger.info('back from yield')
            logger.info('before terminate, return code is %s' % proc.returncode)
            proc.terminate()
            for line in open(log_file_path).readlines():
                line.replace('\n', '')
                logger.info('%s output: %s' % (url, line))
            #logger.info('terminated')
    return


@contextmanager
def run_server_in_context(server_urls=None, port=None):
    yield from run_server(server_urls=server_urls, port=port)


def sort_list_of_dictionaries(input_list):
    return set(tuple(sorted(d.items())) for d in input_list)

# the module contains the client and server process to manage ids

import logging
import os
import json
import time
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    return int((dt - epoch).total_seconds())

os.umask(0o007)

# we avoid the GIL by using a process to actually keep track of the ids
from multiprocessing.connection import Listener, Client

import secrets

logger = logging.getLogger(__name__)

auth_key = secrets.token_bytes(16)

address = None

directory_root = None

ids = {}

# load_ids reads the file system for all to keep a cache of ids in memory
def load_ids(directory):
    for root, sub_dirs, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith('.data'):
                (code, date, extension) = file_name.split('.')
                ids[code] = int(date)
    return ids
                

# Contacts are stored in a 4 level directory structure.  Such that for contact ABCDEFGHxxx, it is stored is AB/CD/EF/ABCDEFGHxxx.  Each contact is a file which contains JSON data.
def store_id(hex_string, json_data, now):
    first_level = hex_string[0:2].upper()
    second_level = hex_string[2:4].upper()
    third_level = hex_string[4:6].upper()
    dir_name = "%s/%s/%s/%s" % (directory_root, first_level, second_level, third_level)
    file_name = "%s/%s.%s.data" % (dir_name, hex_string, now)
    os.makedirs(dir_name, 0o770, exist_ok = True)
    with open(file_name, 'w') as file:
        json.dump(json_data, file)
    ids[hex_string] = now
    return

def store_ids(data):
    now = int(time.time())
    for contact in data['contacts']:
        contact_id = contact['id']
        if contact_id not in ids:
            store_id(contact_id, contact, now)
        else:
            logger.info('contact id: %s already in system' % contact_id)
    return {"status": "ok"}


def get_ids(data):
    since = data.get('since')
    ret = {}
    if since:
        ret['since'] = since
        since = int(unix_time(datetime.datetime.strptime(since, "%Y%m%d%H%M")))
    else:
        ret['since'] = "197001010000"
    # for now we ignore since
    matched_ids = []
    for prefix in data['prefixes']:
        # this is completely the wrong datastructure, there are no buckets yet, but we'll add them in another feature
        prefix_length = len(prefix)
        for contact_id in ids:
            if contact_id[0:prefix_length] == prefix:
                contact_date = ids[contact_id]
                logger.debug('matched %s, date: %s' % (contact_id, ids[contact_id]))
                if (not since) or (since <= contact_date):
                    matched_ids.append(contact_id)
    ret['now'] = time.strftime("%Y%m%d%H%M", time.gmtime())
    ret['ids'] = matched_ids
    return ret
    

GREEN = 'green'
RED = 'red'
command_functions = {RED: store_ids,
                     GREEN : get_ids}



# run_server is the main server loop
def run_server():
    with Listener(address, authkey = auth_key) as listener:
        while True:
            logger.info('about to do accept')
            with listener.accept() as conn:
                try: 
                    data = conn.recv()
                    command = data[0]
                    logger.info('connection accepted from %s, processing command: %s' % (listener.last_accepted, command))
                    ret = command_functions[command](*data[1:])
                except Exception as e:
                    logger.exception('Error in server')
                    ret = {"error": "Error wile processing"}
                conn.send([ret])

def setup_server(app):
    global address, directory_root
    address = ('localhost', app.config['PORT'])
    directory_root = app.config['DIRECTORY']
    load_ids(directory_root)
    pid = os.fork()
    if 0 == pid:
        logging.basicConfig(level=app.config['LOG_LEVEL'])
        run_server()
        exit(0)

def client():
    return Client(address, authkey = auth_key)

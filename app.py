from flask import Flask, request
import logging
import tempfile
import os
import json
import secrets

# we avoid the GIL by using a process to actually keep track of the ids
from multiprocessing.connection import Listener, Client

unix_socket = tempfile.mkstemp()[1]
unix_socket = '/tmp/f'
logger = logging.getLogger(__name__)
app = Flask(__name__)
app.config.from_object('config')
app.logger.setLevel(app.config['LOG_LEVEL'])

auth_key = secrets.token_bytes(16)

import pdb
#pdb.set_trace()

address = ('localhost', app.config['PORT'])


try:
    pid = os.fork()
except OSError:
    exit("Could not create a child process")
 
if pid == 0:
    print('foobar')
    print(unix_socket)
    with Listener(address, authkey = auth_key) as listener:
        while True:
            with listener.accept() as conn:
                print('connection accepted from', listener.last_accepted)
                print(json.loads(conn.recv_bytes()))
    print('barfoo')
    exit()

@app.before_request
def create_client():
    print(request.args)
    request.client = Client(address, authkey = auth_key)
    
@app.route("/red", methods = ['POST'])
def post_red():
    request.client.send_bytes(json.dumps(request.json).encode())
    return "Hello world"

if __name__ == '__main__':
    logging.basicConfig()
    app.run()
    

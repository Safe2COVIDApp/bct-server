import argparse

from twisted.web import resource, server as twserver
from twisted.internet import reactor, task
from twisted.internet.threads import deferToThread
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers
from twisted.python import log
import logging
import json
from contacts import Contacts
import configparser
import urllib.request
import uuid
import signal
import atexit
import sys
from lib import set_current_time_for_testing
import pdb

parser = argparse.ArgumentParser(description='Run bct server.')
parser.add_argument('--config_file', default='config.ini',
                    help='config file name, if an http url then the config file contents are fetched over http')
args = parser.parse_args()


log.startLogging(sys.stderr)


# self_string is used for syncing from neighbors, to ignore a sync from ourself
self_string = uuid.uuid4().hex 

# read config file, potentially looking for recursive config files
def get_config():
    conf = configparser.ConfigParser()
    if 'http' == urllib.parse.urlparse(args.config_file).scheme[0:4].lower():
        contents = urllib.request.urlopen(args.config_file).read().decode()
        conf.read_string(contents)
    else:
        conf.read(args.config_file)
    return conf['DEFAULT']


config = get_config()

contacts = Contacts(config)

# noinspection PyUnusedLocal
def receive_signal(signal_number, frame):
    logger.warning('Received signal: %s' % signal_number)
    if ('True' == config.get('Testing')) and (signal.SIGUSR1 == signal_number):
        # testing is set
        logger.info('Testing is set')
        contacts.reset()
    return

signal.signal(signal.SIGUSR1, receive_signal)

atexit.register(contacts.close)

servers_file_path = '%s/.servers' % config['directory']
logging.basicConfig(level = config['log_level'].upper())
logger = logging.getLogger(__name__)
logger.info('starting server')

try:
    servers = json.load(open(servers_file_path))
    logger.info('read last read date from server neighbors from %s' % servers_file_path)
except:  # TODO-DAN code checker doesn't like such a broad exception catch
    servers = {}
if config.get('servers'):
    for server in config.get('servers').split(','):
        if server not in servers:
            servers[server] = '1970-01-01T00:00Z'
            
allowable_methods = ['/status/scan:POST', '/status/send:POST', '/status/update:POST', '/sync:GET', '/admin/config:GET', '/admin/status:GET', '/init:POST']


def defered_function(function):
    def _defered_function():
        logging.info('in thread, running %s' % function)
        result = function()
        logging.info('ran, result is %s' % result)
        return result
    return _defered_function

def resolve_all_functions(ret, request):
    """
    resolve_all_functions goes through the dictionary ret looking for any values that are functions, if there are, then
    it runs the function in a deferred thread with request, the key for that value and the dictionary as args.  
    After the thread runs successfully the result of the function replaces the function value in the dictionary and we
    iterate until all have been resolved
    """
    for key, value in ret.items():
        if 'function' == type(value).__name__:
            function_to_run_in_thread = defered_function(value)
            logger.info('found a function for key %s, running as a deferred' % key)
            defered = deferToThread(function_to_run_in_thread)
            defered.addCallback(defered_result_available, key, ret, request)
            defered.addErrback(defered_result_error, request)
            return twserver.NOT_DONE_YET
    return ret

def defered_result_error(failure, request):
    logger.error("Logging an uncaught exception",
                 exc_info=(failure.type, failure.value, failure.tb))
    request.setResponseCode(400)
    request.write(json.dumps({'error':'internal error'}).encode())
    request.finish()
    return

def defered_result_available(result, key, ret, request):
    logger.info('got result for key %s of %s' % (key, result))
    ret[key] = result
    ret = resolve_all_functions(ret, request)
    if twserver.NOT_DONE_YET != ret:
        # ok, finally done, let's return it
        logger.info('writing HTTP result of %s' % ret)
        request.write(json.dumps(ret).encode())
        request.finish()
    return
    
    


class Simple(resource.Resource):
    isLeaf = True

    def render(self, request):
        logger.info('in render, request: %s, postpath is %s' % (request, request.postpath))
        x_self_string_headers = request.requestHeaders.getRawHeaders('X-Self-String')
        if x_self_string_headers and (self_string in x_self_string_headers):
            logger.info('called by self, returning 302')
            request.setResponseCode(302)
            return 'ok'.encode()
            
        # we have support for setting time for testing purposes
        x_time_for_testing = request.requestHeaders.getRawHeaders('X-Testing-Time')
        if ('True' == config.get('Testing')) and x_time_for_testing:
            x_time_for_testing = float(x_time_for_testing[0])
            logger.info('In testing and current time is being overridden with %f' % x_time_for_testing)
            set_current_time_for_testing(x_time_for_testing)

        content_type_headers = request.requestHeaders.getRawHeaders('content-type')
        if content_type_headers and ('application/json' in content_type_headers):
            data = json.load(request.content)
        else:
            data = request.content.read()
        logger.info('request content: %s' % data)
        # TODO-DAN code checker says this is shadowing outer-level "args" are you intending to overwrite that variable, and if not maybe rename here ?
        args = {k.decode():[item for item in v] for k,v in request.args.items()}

        path = request.path.decode()
        logger.info('path is %s' % path)
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
        if ('%s:%s' % (path, request.method.decode())) in allowable_methods:
            ret =  contacts.execute_route(path, data, args)
            if 'error' in ret:
                request.setResponseCode(ret.get('status', 400))
                ret = ret['error']
                logger.info('error return is %s' % ret)
            else:
                # if any values functuons in ret, then run then asynchronously and return None here
                # if they aren't then return ret
                
                ret = resolve_all_functions(ret, request)
                logger.info('legal return is %s' % ret)
        else:
            request.setResponseCode(402)
            ret = {"error":"no such request"}
            logger.info('return is %s' % ret)
        if twserver.NOT_DONE_YET != ret:
            return json.dumps(ret).encode()
        else:
            return ret
            

def sync_body(body, server):
    data = json.loads(body)
    logger.info('Response body in sync: %s, calling send status' % data)
    contacts.send_status(json.loads(body), None) 
    servers[server] = data['until']
    json.dump(servers, open(servers_file_path, 'w'))
    return


def sync_error(message):
    logger.error(message)
    return

def sync_response(response, server):
    if 302 == response.code:
        logger.info('got 302 from sync, must be requesting from ourself.  Removing from server list')
        servers.pop(server)
        return
    else:
        d = readBody(response)
        d.addCallback(sync_body, server)
        return d



def get_data_from_neighbors():
    logger.info("getting data from neighbors")
    for server, last_request in servers.items():
        url = '%s/sync?since=%s' % (server, last_request)
        logger.info('getting data from %s' % url)
        agent = Agent(reactor)

        request = agent.request(
            b'GET',
            url.encode(),
            Headers({'User-Agent': ['Twisted Web Client Example'],
                     'X-Self-String': [self_string]}),
            None)
        request.addCallback(sync_response, server)
        request.addErrback(sync_error)
    return

if 0 != len(servers):
    l = task.LoopingCall(get_data_from_neighbors)
    l.start(int(config.get('neighbor_sync_period', 600.0)))

site = twserver.Site(Simple())
reactor.listenTCP(int(config.get('port', 8080)), site)
reactor.run()

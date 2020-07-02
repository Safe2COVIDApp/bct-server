import argparse
import os

from twisted.logger import globalLogPublisher, Logger, globalLogBeginner
from twisted.logger import LogLevelFilterPredicate, LogLevel
from twisted.logger import textFileLogObserver, FilteringLogObserver
from twisted.web import resource, server as twserver
from twisted.internet import reactor, task
from twisted.internet.threads import deferToThread
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers
import json
from contacts import Contacts
import configparser
import urllib.request
import uuid
import signal
import atexit
import sys
from lib import set_current_time_for_testing

parser = argparse.ArgumentParser(description='Run bct server.')
parser.add_argument('--config_file', default='config.ini',
                    help='config file name, if an http url then the config file contents are fetched over http')
parser.add_argument('--log_level', 
                    help='logging level', choices=['debug', 'info', 'warn', 'error', 'critical'])

parsed_args = parser.parse_args()

# self_string is used for syncing from neighbors, to ignore a sync from ourself
self_string = uuid.uuid4().hex


# read config file, potentially looking for recursive config files
def get_config():
    conf = configparser.ConfigParser()
    if 'http' == urllib.parse.urlparse(parsed_args.config_file).scheme[0:4].lower():
        contents = urllib.request.urlopen(parsed_args.config_file).read().decode()
        conf.read_string(contents)
    else:
        conf.read(parsed_args.config_file)
    return conf


config_top = get_config()
config = config_top['DEFAULT']

mlog_file_path = config.get('log_file_path')
log_observer = None


def reset_log_file():
    global log_observer
    if log_observer:
        print('removing log observer')
        globalLogPublisher.removeObserver(log_observer)
    log_level = parsed_args.log_level or config['log_level']
    info_predicate = LogLevelFilterPredicate(LogLevel.levelWithName(log_level.lower()))
    if mlog_file_path:
        mlog_file = open(mlog_file_path, 'a+')
    else:
        mlog_file = sys.stderr

    mlog_observer = FilteringLogObserver(textFileLogObserver(mlog_file), predicates=[info_predicate])
    globalLogPublisher.addObserver(mlog_observer)

    # logger.info('resetting log output file')
    return


reset_log_file()
logger = Logger()
globalLogBeginner.beginLoggingTo([])

contacts = Contacts(config_top)


# noinspection PyUnusedLocal
def receive_signal(signal_number, frame):
    logger.info('Received signal: {signal_number}', signal_number=signal_number)
    if ('True' == config.get('Testing')) and (signal.SIGUSR1 == signal_number):
        # testing is set
        logger.info('Testing is set')
        # noinspection PyBroadException,PyPep8
        try:
            contacts.reset()
        except:  
            logger.failure('error resetting, exiting')
            reactor.stop()
    return


signal.signal(signal.SIGUSR1, receive_signal)

atexit.register(contacts.close)

servers_file_path = '%s/.servers' % config['directory']


logger.info('loading server')

try:
    servers = json.load(open(servers_file_path))
    logger.info('read last read date from server neighbors from {servers_file_path}', servers_file_path=servers_file_path)
except json.JSONDecodeError:
    logger.error("Bad JSON in server file at {file_path} recovering automatically", file_path=servers_file_path)
    servers = {}
except FileNotFoundError as err:
    servers = {}
if config.get('servers'):
    for server in config.get('servers').split(','):
        if server not in servers:
            servers[server] = '1970-01-01T00:00Z'

allowable_methods = ['/status/scan:POST', '/status/send:POST', '/status/update:POST', '/sync:GET', '/admin/config:GET',
                     '/admin/status:GET', '/status/result:POST', '/status/data_points:POST', '/init:POST']


def deferred_function(function):
    def _deferred_function():
        logger.info('in thread, running {function}', function=function)
        result = function()
        logger.info('ran, result is {result}', result=result)
        return result

    return _deferred_function


def resolve_all_functions(ret, request):
    """
    resolve_all_functions goes through the dictionary ret looking for any values that are functions, if there are, then
    it runs the function in a deferred thread with request, the key for that value and the dictionary as args
    After the thread runs successfully the result of the function replaces the function value in the dictionary and we
    iterate until all have been resolved
    """
    for key, value in ret.items():
        if 'function' == type(value).__name__:
            function_to_run_in_thread = deferred_function(value)
            logger.info('found a function for key {key}, running as a deferred', key=key)
            deferred = deferToThread(function_to_run_in_thread)
            deferred.addCallback(deferred_result_available, key, ret, request)
            deferred.addErrback(deferred_result_error, request)
            return twserver.NOT_DONE_YET
    return ret


def deferred_result_error(failure, request):
    logger.failure("Logging an uncaught exception", failure=failure)
    request.setResponseCode(400)
    request.write(json.dumps({'error': 'internal error'}).encode())
    request.finish()
    return


def deferred_result_available(result, key, ret, request):
    logger.info('got result for key {key} of {result}', key=key, result=result)
    ret[key] = result
    ret = resolve_all_functions(ret, request)
    if twserver.NOT_DONE_YET != ret:
        # ok, finally done, let's return it
        logger.info('writing HTTP result of {ret}', ret=ret)
        request.write(json.dumps(ret).encode())
        request.finish()
    return


class Simple(resource.Resource):
    isLeaf = True

    def render(self, request):
        logger.info('in render, request: {request}, post_path is {post_path}', request=request, post_path=request.postpath)
        x_self_string_headers = request.requestHeaders.getRawHeaders('X-Self-String')
        if x_self_string_headers and (self_string in x_self_string_headers):
            logger.info('called by self, returning 302')
            request.setResponseCode(302)
            return 'ok'.encode()

        # we have support for setting time for testing purposes
        x_time_for_testing = request.requestHeaders.getRawHeaders('X-Testing-Time')
        if ('True' == config.get('Testing')) and x_time_for_testing:
            x_time_for_testing = float(x_time_for_testing[0])
            logger.info('In testing and current time is being overridden with {time}', time=x_time_for_testing)
            set_current_time_for_testing(x_time_for_testing)

        content_type_headers = request.requestHeaders.getRawHeaders('content-type')
        if content_type_headers and ('application/json' in content_type_headers):
            try:
                data = json.load(request.content)
            except json.JSONDecodeError:
                logger.error('Passed bad JSON in request: {content}', content=request.content)
                request.setResponseCode(500)
                ret = {"error": "Bad JSON in request"}
                return ret
        else:
            data = request.content.read()
        logger.info('request content: {data}', data=data)

        args = {k.decode(): [item for item in v] for k, v in request.args.items()}

        path = request.path.decode()
        method = request.method.decode()
        path_method = '%s:%s' % (path, method)
        logger.info('{method} {path}', method=method, path=path)
        if method == "OPTIONS":
            request.setResponseCode(204)
            request.responseHeaders.addRawHeader(b"Allow", b"OPTIONS, GET, POST")
            request.responseHeaders.addRawHeader(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS")
            request.responseHeaders.addRawHeader(b"Connection", b"Keep-Alive")
            request.responseHeaders.addRawHeader(b"Access-Control-Allow-Headers", b"Content-Type")
            # The cross-origin headers were needed for some health provider portal work that isn't happening yet so commented out.
            # before this gets commented back in, the origins should come from config file
            # request.responseHeaders.addRawHeader(b"Access-Control-Allow-Origin", b"*")
            # request.responseHeaders.addRawHeader(b"Vary", b"Origin")
            return b""
        else:
            request.responseHeaders.addRawHeader(b"content-type", b"application/json")
            # Commented out until we need it for access to server from locally generated or third party server.
            # before this gets commented back in, the origins should come from config file
            # request.responseHeaders.addRawHeader(b"access-control-allow-origin", b"*")
            if path_method in allowable_methods:
                ret = contacts.execute_route(path, data, args)
                if 'error' in ret:
                    request.setResponseCode(ret.get('status', 400))
                    ret = ret['error']
                    logger.error('error return is {ret}', ret=ret)
                else:
                    # if any values functions in ret, then run then asynchronously and return None here
                    # if they aren't then return ret

                    ret = resolve_all_functions(ret, request)
                    logger.info('legal return is {ret}', ret=ret)
            else:
                request.setResponseCode(402)
                ret = {"error": "no such request"}
                logger.error('return is {ret}', ret=ret)
            if twserver.NOT_DONE_YET != ret:
                return json.dumps(ret).encode()
            else:
                return ret


def sync_body(body, remote_server):
    server_name = remote_server  # TODO-119 TODO-67 this will be replaced with a certified name once certificates implemented
    data = json.loads(body)  # TODO-DAN need to handle error (json.JSONDecodeError) here
    logger.info('Response body in sync: {data}, calling send status', data=data)
    json_data = json.loads(body)  # TODO-DAN why do we convert the sync result here as well as 2 lines above and sometimes use json_data and sometimes data below?
    for o in json_data.get('contact_ids', []) + json_data.get('locations', []):
        if not o.get('path'):
            o['path'] = []
        o['path'].append(server_name)
    contacts.send_or_sync(json_data, {})
    servers[remote_server] = data['until']
    json.dump(servers, open(servers_file_path, 'w'))
    return


def sync_error(failure):
    logger.error("Error in connecting to server '{value}'", value=failure.value)
    return


def sync_response(response, remote_server):
    if 302 == response.code:
        logger.info('got 302 from sync, must be requesting from ourself.  Removing from server list')
        servers.pop(remote_server)
        return
    else:
        d = readBody(response)
        d.addCallback(sync_body, remote_server)
        return d


def get_data_from_neighbors():
    logger.info("getting data from neighbors")
    for remote_server, last_request in servers.items():
        url = '%s/sync?since=%s' % (remote_server, last_request)
        logger.info('getting data from {url}', url=url)
        agent = Agent(reactor)

        request = agent.request(
            b'GET',
            url.encode(),
            Headers({'User-Agent': ['Twisted Web Client Example'],
                     'X-Self-String': [self_string]}),
            None)
        request.addCallback(sync_response, remote_server)
        request.addErrback(sync_error)
    return


def delete_expired_data_success(result):
    logger.info('finished deleting from expired data')
    return


def delete_expired_data_failure(failure):
    logger.failure("Logging an uncaught exception", failure=failure)
    return


def delete_expired_data():
    logger.info("Expiring data")
    contacts.move_expired_data_to_deletion_list()
    function_to_run_in_thread = deferred_function(contacts.delete_from_deletion_list)
    deferred = deferToThread(function_to_run_in_thread)
    deferred.addCallback(delete_expired_data_success)
    deferred.addErrback(delete_expired_data_failure)
    return


if 0 != len(servers):
    l1 = task.LoopingCall(get_data_from_neighbors)
    l1.start(float(config.get('neighbor_sync_period', 600.0)))

l2 = task.LoopingCall(delete_expired_data)
l2.start(24*60*60)

site = twserver.Site(Simple())

ON_HEROKU = os.environ.get('ON_HEROKU')

if ON_HEROKU:
    # get the heroku port
    port = int(os.environ.get('PORT', 8080))  # as per OP comments default is 17995
else:
    port = int(config.get('port', 8080))

reactor.listenTCP(port, site)

# gack, we can't reset this... we will try at another time
# l = task.LoopingCall(reset_log_file)
# l.start(10, now = False)

# This is intentionally at warn level to allow when debugging to wait for it to be ready
logger.warn('Server alive and listening on port %s' % port)
reactor.run()

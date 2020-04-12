from twisted.web import server, resource
from twisted.internet import reactor
import logging
import json
from contacts import Contacts
import configparser


# read config file, potentially looking for recursive config files
def get_config():
    conf = configparser.configParser()
    conf.read("config.ini")
    config = conf['DEFAULT']
    url = config.get('URL')
    if url:
        contents = urllib.request.urlopen(config.URL).read()
        conf = configparser.configParser()
        conf.read_string(contents)
        for key, value in conf['DEFAULT']:
            config[key] = value
    return config



logging.basicConfig(level = config.LOG_LEVEL)
logger = logging.getLogger(__name__)
allowable_methods = ['red:POST', 'green:POST', 'sync:GET']



contacts = Contacts(config.DIRECTORY)

class Simple(resource.Resource):
    isLeaf = True

    def render(self, request):
        logger.info('in render, request: %s, postpath is %s' % (request, request.postpath))
        content_type_headers = request.requestHeaders.getRawHeaders('content-type')
        if content_type_headers and ('application/json' in content_type_headers):
            data = json.load(request.content)
        else:
            data = request.content.read()
        logger.info('request content: %s' % data)
        # 
        args = {k.decode():[item for item in v] for k,v in request.args.items()}

        path = request.path.decode()[1:]
        logger.info('path is %s' % path)
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
        if ('%s:%s' % (path, request.method.decode())) in allowable_methods:
            ret =  getattr(contacts, path)(data, args)
            logger.info('legal return is %s' % ret)
        else:
            request.setResponseCode(402)
            ret = {"error":"no such request"}
            logger.info('return is %s' % ret)
        return json.dumps(ret).encode('utf-8')
            


site = server.Site(Simple())
reactor.listenTCP(8080, site)
reactor.run()

from twisted.web import server, resource
from twisted.internet import reactor
import logging
import json
from contacts import Contacts
import config

logging.basicConfig(level = config.LOG_LEVEL)
logger = logging.getLogger(__name__)
allowable_methods = ['red', 'green']

contacts = Contacts(config.DIRECTORY)
class Simple(resource.Resource):
    isLeaf = True

    def render_POST(self, request):
        logger.info('in render_POST, request: %s, postpath is %s' % (request, request.postpath))
        json_data = json.load(request.content)
        logger.info('request content: %s' % json_data)

        path = request.uri.decode()[1:]
        logger.info('path is %s' % path)
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
        if path in allowable_methods:
            ret =  getattr(contacts, path)(json_data)
            logger.info('legal return is %s' % ret)
        else:
            request.setResponseCode(402)
            ret = {"error":"no such request"}
            logger.info('return is %s' % ret)
        return json.dumps(ret).encode('utf-8')
            


site = server.Site(Simple())
reactor.listenTCP(8080, site)
reactor.run()

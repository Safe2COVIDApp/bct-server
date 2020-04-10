from flask import Flask, request, jsonify
import logging
import os
import json
import service



logger = logging.getLogger(__name__)
app = Flask(__name__)
app.config.from_object('config')
app.logger.setLevel(app.config['LOG_LEVEL'])

service.setup_server(app)

def do_service_request(data):
    client = service.client()
    client.send(data)
    logger.info('client data sent')
    
    ret = client.recv()[0]
    if 'error' in ret:
        return "Error", 400
    return jsonify(ret)

    
@app.route("/red", methods = ['POST'])
def post_red():
    data = request.json
    logger.info('in POST red: %s' % data)
    return do_service_request([service.RED, data])

@app.route("/green", methods = ['POST'])
def post_green():
    data = request.json
    logger.info('in POST green: %s' % data)
    return do_service_request([service.GREEN, data])


if __name__ == '__main__':
    logging.basicConfig(level=app.config['LOG_LEVEL'])
    app.run()
    

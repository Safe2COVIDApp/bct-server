from flask import Flask, request
import logging
import os
import json
import service



logger = logging.getLogger(__name__)
app = Flask(__name__)
app.config.from_object('config')
app.logger.setLevel(app.config['LOG_LEVEL'])

service.setup_server(app)

@app.before_request
def create_client():
    request.client = service.client()
    
@app.route("/red", methods = ['POST'])
def post_red():
    data = request.json
    logger.info('in POST red: %s' % data)
    request.client.send([service.STORE_COMMAND, data])
    return 'OK'


if __name__ == '__main__':
    logging.basicConfig(level=app.config['LOG_LEVEL'])
    app.run()
    

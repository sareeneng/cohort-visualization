from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer
from web import flask_app
import logging

if __name__ == '__main__':
    logging.info('Starting externally accessible server')
    LISTEN = ('0.0.0.0', 8050)
    http_server = WSGIServer(LISTEN, flask_app)
    http_server.serve_forever()

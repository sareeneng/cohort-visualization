from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer  # noqa: E402
from web import flask_app  # noqa: E402
import logging  # noqa: E402

if __name__ == '__main__':
    logging.info('Starting externally accessible server')
    
    # https://stackoverflow.com/questions/49038678/cant-disable-flask-werkzeug-logging
    LISTEN = ('0.0.0.0', 8050)
    http_server = WSGIServer(LISTEN, flask_app, log=None)
    http_server.serve_forever()

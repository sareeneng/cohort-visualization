from flask import Flask
from flask_bootstrap import Bootstrap
from flask_wtf.csrf import CSRFProtect
import logging
import os

class Config(object):
	SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(24)

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
logger.setLevel(logging.DEBUG)

handler_info = logging.FileHandler(filename='info.log', mode='a')
handler_info.setFormatter(formatter)
handler_info.setLevel(logging.INFO)

handler_debug = logging.FileHandler(filename='debug.log', mode='w')
handler_debug.setFormatter(formatter)
handler_debug.setLevel(logging.DEBUG)

logger.addHandler(handler_info)
logger.addHandler(handler_debug)

flask_app = Flask(__name__)
flask_app.config.from_object(Config)
bootstrap = Bootstrap(flask_app)
csrf = CSRFProtect(flask_app)
csrf._exempt_views.add('dash.dash.dispatch')

from web import routes
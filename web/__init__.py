from flask import Flask
from flask_bootstrap import Bootstrap
from flask_wtf.csrf import CSRFProtect
import logging
import numpy as np
import os
from flask.json import JSONEncoder


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(24)

class CustomJSONEncoder(JSONEncoder):
    def default(self, o):  
        if isinstance(o, np.int64):
            return int(o)
        if isinstance(o, np.float64):
            return float(o)

        # Any other serializer if needed
        return super(CustomJSONEncoder, self).default(o)

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
flask_app.json_encoder = CustomJSONEncoder
flask_app.config.from_object(Config)
bootstrap = Bootstrap(flask_app)
csrf = CSRFProtect(flask_app)

from web import routes
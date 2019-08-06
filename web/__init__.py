from flask import Flask
from flask_bootstrap import Bootstrap
from flask_wtf.csrf import CSRFProtect
import os

class Config(object):
	SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(24)

flask_app = Flask(__name__)
flask_app.config.from_object(Config)
bootstrap = Bootstrap(flask_app)
csrf = CSRFProtect(flask_app)
csrf._exempt_views.add('dash.dash.dispatch')

from web import routes
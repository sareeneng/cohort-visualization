from web import flask_app
from visualization import app as dash_app_visualization
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple
import logging

application = DispatcherMiddleware(flask_app, {
	'/dash_cohort_visualization': dash_app_visualization.server
})

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

logging.info('Start local server')
run_simple('127.0.0.1', 5000, application)
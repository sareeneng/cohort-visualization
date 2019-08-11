from web import flask_app
from visualization import app as dash_app_visualization
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple
import logging

application = DispatcherMiddleware(flask_app, {
	'/dash_cohort_visualization': dash_app_visualization.server
})

logging.info('Start local server')
run_simple('127.0.0.1', 5000, application)
from web import flask_app
from flask import render_template

@flask_app.route('/')
@flask_app.route('/visualization')
def visualization():
	return render_template('visualization.html', header="Cohort Visualization")
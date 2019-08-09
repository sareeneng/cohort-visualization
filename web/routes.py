from db_structure import DB
from web import flask_app
from flask import jsonify, render_template, request
import logging
import os

@flask_app.route('/')
@flask_app.route('/visualization')
def visualization():
	return render_template('visualization.html', header="Cohort Visualization")


@flask_app.route('/config', methods=['GET', 'POST'])
def config():
	if request.method == 'POST':
		data = request.get_json()
		logging.debug(data)
		if data.get('form') == 'choose_dataset':
			chosen_dataset = data.get('dataset_name')
			db = DB(chosen_dataset)
			return_data = {}
			return_data['table_columns'] = {x: [] for x in db.tables.keys()}
			for table_name, table in db.tables.items():
				df_col_headers = table.df.columns
				col_renames = []
				for df_col_header in df_col_headers:
					idx = df_col_header.rfind('_[')
					df_col_header = df_col_header[:idx]
					col_renames.append((df_col_header, table.df_col_links[df_col_header].display_name))
				return_data['table_columns'][table_name] = col_renames
			logging.debug(jsonify(return_data))
			return jsonify(return_data)
		elif data.get('form') == 'choose_table':
			pass
	datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir()])
	return render_template('config.html', header='Configuration', datasets=datasets)

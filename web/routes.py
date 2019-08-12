from db_structure import DB, DataManager
from web import flask_app
from flask import jsonify, render_template, request
import logging
import os
import pandas as pd
import time

@flask_app.route('/')
@flask_app.route('/visualization')
def visualization():
	datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir()], key=lambda x: x.upper())
	distribution_choices = ['Count', 'Percents', 'Sum', 'Mean']

	return render_template('visualization.html', header="Cohort Visualization", datasets=datasets, distribution_choices=distribution_choices)

@flask_app.route('/get_graph_data')
def get_graph_data():
	start = time.time()
	return_data = {}
	chosen_dataset = request.args.get('chosen_dataset')
	chosen_ind_idxs = request.args.getlist('chosen_ind_idxs[]', None)
	if len(chosen_ind_idxs) == 0:
		return jsonify({})
	
	chosen_outcome_idx = request.args.get('chosen_outcome_idx', None)
	if chosen_outcome_idx == '':
		chosen_outcome_idx = None

	aggregate_fxn = request.args.get('aggregate_fxn')
	
	db = DB(os.path.join('datasets', chosen_dataset))
	dm = DataManager(db, load_all_data=True)

	if chosen_outcome_idx is None:
		all_chosen_idxs = chosen_ind_idxs
	else:
		all_chosen_idxs = [chosen_outcome_idx] + chosen_ind_idxs

	paths = db.find_paths_multi_columns(all_chosen_idxs)
	df = dm.get_biggest_joined_df_option_from_paths(paths, filter_col_idxs=all_chosen_idxs)
	df = dm.aggregate_df(df, groupby_col_idxs=chosen_ind_idxs, aggregate_col_idx=chosen_outcome_idx, aggregate_fxn=aggregate_fxn)
	df = df.fillna(0)  # for charting purposes.

	labels = list(df['groupby_labels'])
	outcome_possibilities = [x for x in df.columns if x != 'groupby_labels']
	datasets = []
	for outcome_possibility in outcome_possibilities:
		datasets.append({
				'label': outcome_possibility,
				'data': list(df[outcome_possibility])
		})

	groupby_col_names = [db.column_display_names[x] for x in chosen_ind_idxs]
	groupby_axis_label = ''
	for x in groupby_col_names:
		groupby_axis_label += x + '_'
	groupby_axis_label = groupby_axis_label[:-1]
	if chosen_outcome_idx is None:
		title = f'{aggregate_fxn} broken down by {groupby_axis_label}'
	else:
		aggregate_axis_label = db.column_display_names[chosen_outcome_idx]
		title = f'{aggregate_fxn} of {aggregate_axis_label} broken down by {groupby_axis_label}'

	return_data = {
		'labels': labels,
		'datasets': datasets,
		'title': title,
		'xaxis_label': groupby_axis_label,
		'yaxis_label': aggregate_fxn

	}
	logging.debug(return_data)
	
	end = time.time()
	logging.info(f'Took {end - start:.2f} seconds to get data')
	return jsonify(return_data)

@flask_app.route('/get_accessible_variables')
def get_accessible_variables():
	return_data = {}
	chosen_dataset = request.args.get('chosen_dataset')
	db = DB(os.path.join('datasets', chosen_dataset))
	column_display_names = db.get_all_column_display_names()
	
	chosen_ind_idxs = request.args.getlist('chosen_ind_idxs[]', None)
	chosen_outcome_idx = request.args.get('chosen_outcome_idx', None)
	accessible_list = []
	if len(chosen_ind_idxs) == 0 and chosen_outcome_idx in [None, '']:
		# User hasn't chosen anything yet, so both the independent variables and outcome variables will have the same options. This will happen when dataset has changed
		for idx, name in column_display_names.items():
			accessible_list.append((idx, name, True))
	else:
		if chosen_outcome_idx in [None, '']:
			all_chosen_idxs = chosen_ind_idxs
		else:
			all_chosen_idxs = [chosen_outcome_idx] + chosen_ind_idxs

		accessible_col_idxs = db.find_column_idxs_still_accessible_idxs(all_chosen_idxs)
		for idx, name in column_display_names.items():
			accessible_list.append((idx, name, idx in accessible_col_idxs))
	
	sorted_accessible_list = sorted(accessible_list, key=lambda x: list(i.upper() for i in x[1]))

	return_data['accessible_columns_list'] = sorted_accessible_list

	return jsonify(return_data)

@flask_app.route('/config', methods=['GET', 'POST'])
def config():
	datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir()], key=lambda x: x.upper())
	return render_template('config.html', header='Configuration', datasets=datasets)

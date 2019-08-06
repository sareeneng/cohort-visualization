import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_reusable_components as drc
from dash.dependencies import Input, Output
from db_structure import DB
import utilities as u

import logging
import os
import pandas as pd
import plotly.graph_objs as go
import time
from web import flask_app

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=flask_app, url_base_pathname='/dash/visualization/')

db = DB(os.path.join('datasets', 'TOPICC'))
all_columns = db.struct.column_factory.columns
# exclude_columns = db.exclude_columns_from_data_viz
exclude_columns = ['PudID']

app.layout = html.Div(
	children=[
		html.Div(
			className="row",
			children=[
				drc.Card(
					className="four columns",
					id="variable_container",
					children=[
						drc.NamedDropdown(
							name="Independent variables",
							id="ind_variables",
							options = [],
							value = [],
							multi=True
						),
						drc.NamedDropdown(
							name="Outcome",
							id="outcome_variable"
						),
						drc.NamedRadioItems(
							name="Distribution Type",
							id="distribution_type",
							options=[{'label': x, 'value': x} for x in ['Count', '% within category', 'sum', 'mean']],
							value='Count',
						)
					]
				)
			]
		),
		html.Div(
			className="row nine columns",
			children=[
				dcc.Graph(
					id="outcome_graph"
				)
			]
		)
	],
)

def get_col_objs(*args):
	args = [[] if x is None else x for x in args]
	args = u.flatten(args)
	return [all_columns[x] for x in args]
	

@app.callback(
	[
		Output('outcome_variable', 'options'),
		Output('ind_variables', 'options')
	],
	[
		Input('outcome_variable', 'value'),
		Input('ind_variables', 'value')
	]
)
def update_variable_options(outcome_var_chosen, ind_vars_chosen):
	if len(ind_vars_chosen) == 0 and outcome_var_chosen is None:
		# User hasn't chosen anything yet, so both the independent variables and outcome variables will have the same options
		full_list = sorted([{'label': v.display_name, 'value': k} for k, v in all_columns.items() if v.display_name not in exclude_columns], key=lambda x: x['label'])
		return full_list, full_list
	
	if outcome_var_chosen is None:
		all_col_idxs = ind_vars_chosen
	else:
		all_col_idxs = [outcome_var_chosen] + ind_vars_chosen
	
	accessible_col_idxs = db.struct.get_still_accessible_columns(include_column_idxs=all_col_idxs)

	accessible_list = sorted([{'label': all_columns[x].display_name, 'value': x} for x in accessible_col_idxs if all_columns[x].display_name not in exclude_columns], key=lambda x: x['label']) 

	return accessible_list, accessible_list


@app.callback(
	Output('outcome_graph', 'figure'),
	[
		Input('outcome_variable', 'value'),
		Input('ind_variables', 'value'),
		Input('distribution_type', 'value')
	]
)
def update_graph(outcome_var_chosen_idx, ind_vars_chosen_idxs, distribution_type):
	if len(ind_vars_chosen_idxs) == 0:
		return {}
		
	if outcome_var_chosen_idx is None:
		all_col_idxs = ind_vars_chosen_idxs
	else:
		all_col_idxs = [outcome_var_chosen_idx] + ind_vars_chosen_idxs

	paths = db.struct.find_paths_multi_columns(all_col_idxs)
	df = db.get_biggest_joined_df_option_from_paths(paths)

	# Now need to get the column header for each column
	all_col_headers = []
	ind_col_headers = []
	outcome_col_header = None
	for idx in all_col_idxs:
		for table, column in db.struct.column_links[idx].items():
			df_col_header = f'{column}_[{table}]'
			if df_col_header in df.columns:
				all_col_headers.append(df_col_header)
				if idx == outcome_var_chosen_idx:
					outcome_col_header = df_col_header
				else:
					ind_col_headers.append(df_col_header)

	df = df.loc[:, all_col_headers]

	if outcome_var_chosen_idx is None:
		# just get the counts then
		distribution = df.groupby(ind_col_headers).size().reset_index(name="Count")
	else:
		g = df.groupby(ind_col_headers)[outcome_col_header]
		
		if distribution_type == 'Count':
			distribution = g.value_counts().unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_col_header].unique()) if not pd.isnull(x)]
		elif distribution_type == '% within category':
			distribution = (g.value_counts(normalize=True)*100).round(1).unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_col_header].unique()) if not pd.isnull(x)]
		elif distribution_type == 'sum':
			distribution = g.sum().reset_index()
			outcome_possibilities = None
		elif distribution_type == 'mean':
			distribution = g.mean().reset_index()
			outcome_possibilities = None
		logging.info(distribution)
	
	def get_breakdown_label(row, ind_variables):
		return_str = ''
		for x in ind_variables:
			return_str += str(row[x]) + '_'
		return_str = return_str[:-1]  # remove trailing underscore
		return return_str
	
	distribution['Breakdown_axis_labels'] = distribution.apply(lambda x: get_breakdown_label(x, ind_col_headers), axis=1)

	ind_display_names = [all_columns[x].display_name for x in ind_vars_chosen_idxs]
	outcome_display_name = None if outcome_var_chosen_idx is None else all_columns[outcome_var_chosen_idx].display_name

	if outcome_col_header is None:
		traces = [go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution["Count"])]
	else:
		traces = []
		if outcome_possibilities is not None:
			for outcome_possibility in outcome_possibilities:
				traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_possibility], name=str(outcome_possibility)))
		else:
			traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_col_header], name=str(outcome_display_name)))

	graph_data = {
		'data': traces,
		'layout': go.Layout(
			title=f'Outcome {outcome_display_name} broken down by {ind_display_names}',
			xaxis={
				'title': f'{outcome_display_name}',
				'titlefont': {'color': 'black', 'size': 14},
				'tickfont': {'size': 9, 'color': 'black'}
			},
			yaxis={
				'title': f'{distribution_type} of {outcome_display_name}',
				'titlefont': {'color': 'black', 'size': 14},
				'tickfont': {'size': 9, 'color': 'black'}
			},
			autosize=False
		)
	}

	return graph_data


if __name__ == '__main__':
	logging.info('First')
	app.run_server(debug=False, host='0.0.0.0', port=8050)

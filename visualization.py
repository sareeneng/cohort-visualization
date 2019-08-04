import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_reusable_components as drc
from dash.dependencies import Input, Output
from db_structure import DB
import utilities as u

import logging
import pandas as pd
import plotly.graph_objs as go
import time

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

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

db = DB('TOPICC')
all_columns = db.get_all_columns()
repetitive_columns = db.get_common_column_names()
non_repetitive_columns = [x for x in all_columns if x not in repetitive_columns]
exclude_columns = db.exclude_columns_from_data_viz

column_label_to_obj = {f'{x.name}': x for x in non_repetitive_columns}
# for x in non_repetitive_columns:
#	column_label_to_obj[x.name] = x

app.layout = html.Div(
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
		),
		dcc.Graph(
			className="seven columns",
			id="outcome_graph"
		)
	]
)

def get_col_obj(*args):
	args = [[] if x is None else x for x in args]
	args = u.flatten(args)	
	return [column_label_to_obj[x] for x in args]
	

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
		full_list = sorted([{'label': x, 'value': x} for x in column_label_to_obj.keys() if x not in exclude_columns], key=lambda y: y['label'])
		return full_list, full_list
	
	current_col_obj_list = get_col_obj(outcome_var_chosen, ind_vars_chosen)
	accessible_col_objs = db.get_still_accessible_columns(include_columns=current_col_obj_list)

	accessible_list = sorted([{'label': x.name, 'value': x.name} for x in accessible_col_objs if x.name not in exclude_columns], key=lambda y: y['label'])

	return accessible_list, accessible_list


@app.callback(
	Output('outcome_graph', 'figure'),
	[
		Input('outcome_variable', 'value'),
		Input('ind_variables', 'value'),
		Input('distribution_type', 'value')
	]
)
def update_graph(outcome_var_chosen, ind_vars_chosen, distribution_type):
	if len(ind_vars_chosen) == 0:
		return {}
		
	current_col_obj_list = get_col_obj(outcome_var_chosen, ind_vars_chosen)
	paths = db.find_paths_multi_columns(current_col_obj_list)
	df = db.get_biggest_joined_df_option_from_paths(paths)

	outcome_var_with_table = None
	ind_vars_with_table = []
	for col_obj in current_col_obj_list:
		for table in col_obj.tables:
			col_header = f'{col_obj.name}_[{table.name}]'
			if col_header in df.columns:
				if col_obj.name == outcome_var_chosen:
					outcome_var_with_table = col_header
				else:
					ind_vars_with_table.append(col_header)

	if outcome_var_chosen is None:
		all_vars_with_tables = ind_vars_with_table
	else:
		all_vars_with_tables = [outcome_var_with_table] + ind_vars_with_table

	df = df.loc[:, all_vars_with_tables]

	if outcome_var_chosen is None:
		# just get the counts then
		distribution = df.groupby(ind_vars_with_table).size().reset_index(name="Count")
	else:
		g = df.groupby(ind_vars_with_table)[outcome_var_with_table]
		
		if distribution_type == 'Count':
			distribution = g.value_counts().unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_var_with_table].unique()) if not pd.isnull(x)]
		elif distribution_type == '% within category':
			distribution = (g.value_counts(normalize=True)*100).round(1).unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_var_with_table].unique()) if not pd.isnull(x)]
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
	
	distribution['Breakdown_axis_labels'] = distribution.apply(lambda x: get_breakdown_label(x, ind_vars_with_table), axis=1)

	if outcome_var_chosen is None:
		traces = [go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution["Count"])]
	else:
		traces = []
		if outcome_possibilities is not None:
			for outcome_possibility in outcome_possibilities:
				traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_possibility], name=str(outcome_possibility)))
		else:
			traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_var_with_table], name=str(outcome_var_with_table)))

	graph_data = {
		'data': traces,
		'layout': go.Layout(
			title=f'Outcome {outcome_var_chosen} broken down by {ind_vars_chosen}',
			xaxis={
				'title': f'{ind_vars_chosen}',
				'titlefont': {'color': 'black', 'size': 14},
				'tickfont': {'size': 9, 'color': 'black'}
			},
			yaxis={
				'title': f'{distribution_type} of {outcome_var_chosen}',
				'titlefont': {'color': 'black', 'size': 14},
				'tickfont': {'size': 9, 'color': 'black'}
			}
		)
	}

	return graph_data


if __name__ == '__main__':
	app.run_server(debug=False, host='0.0.0.0', port=8050)

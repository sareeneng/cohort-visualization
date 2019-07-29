import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_reusable_components as drc
from dash.dependencies import Input, Output
from db_structure import DB

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
					name="Outcome",
					id="outcome_variable",
					options=sorted([{'label': x, 'value': x} for x in column_label_to_obj.keys() if x not in exclude_columns], key=lambda y: y['label'] ),
				),
				drc.NamedDropdown(
					name="Breakdown by",
					id="breakdown_variables",
					options=[],
					value=[],
					multi=True
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

@app.callback(
	[
		Output('breakdown_variables', 'options'),
		Output('outcome_graph', 'figure'),
	],
	[
		Input('outcome_variable', 'value'),
		Input('breakdown_variables', 'value'),
		Input('distribution_type', 'value')
	]
)
def update_graph(outcome_variable, breakdown_variables, distribution_type):
	if outcome_variable is None:
		return_list = [sorted([{'label': x, 'value': x} for x in column_label_to_obj.keys() if x not in exclude_columns], key=lambda y: y['label'])]
		return return_list, {}
		
	outcome_col_obj = column_label_to_obj[outcome_variable]
	if breakdown_variables is not None:
		breakdown_col_objs = [column_label_to_obj[x] for x in breakdown_variables]
	else:
		breakdown_col_objs = []
	current_col_obj_list = [outcome_col_obj] + breakdown_col_objs

	accessible_col_objs = db.get_still_accessible_columns(include_columns=current_col_obj_list)

	return_list = sorted([{'label': x.name, 'value': x.name} for x in accessible_col_objs if x.name not in exclude_columns], key=lambda y: y['label'])
	
	if len(breakdown_variables) > 0:
		paths = db.find_paths_multi_columns(current_col_obj_list, fix_first=True)
		df = db.get_biggest_joined_df_option_from_paths(paths)

		breakdown_variables_with_table = []
		for breakdown_col_obj in breakdown_col_objs:
			for table in breakdown_col_obj.tables:
				breakdown_column_header = f'{breakdown_col_obj.name}_[{table.name}]'
				if breakdown_column_header in df.columns:
					breakdown_variables_with_table.append(breakdown_column_header)
					break
		for table in outcome_col_obj.tables:
			outcome_column_header = f'{outcome_col_obj.name}_[{table.name}]'
			if outcome_column_header in df.columns:
				outcome_variable_with_table = outcome_column_header
				break

		g = df.groupby(breakdown_variables_with_table)[outcome_variable_with_table]
		
		if distribution_type == 'Count':
			distribution = g.value_counts().unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_variable_with_table].unique()) if not pd.isnull(x)]
		elif distribution_type == '% within category':
			distribution = (g.value_counts(normalize=True)*100).round(1).unstack().reset_index()
			outcome_possibilities = [x for x in list(df[outcome_variable_with_table].unique()) if not pd.isnull(x)]
		elif distribution_type == 'sum':
			distribution = g.sum().reset_index()
			outcome_possibilities = None
		elif distribution_type == 'mean':
			distribution = g.mean().reset_index()
			outcome_possibilities = None
		logging.info(distribution)
		
		def get_breakdown_label(row, breakdown_variables):
			return_str = ''
			for x in breakdown_variables:
				return_str += str(row[x]) + '_'
			return_str = return_str[:-1]  # remove trailing underscore
			return return_str
		
		distribution['Breakdown_axis_labels'] = distribution.apply(lambda x: get_breakdown_label(x, breakdown_variables_with_table), axis=1)

		traces = []
		
		if outcome_possibilities is not None:
			for outcome_possibility in outcome_possibilities:
				traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_possibility], name=str(outcome_possibility)))
		else:
			traces.append(go.Bar(x=distribution['Breakdown_axis_labels'], y=distribution[outcome_variable_with_table], name=str(outcome_variable)))

		graph_data = {
			'data': traces,
			'layout': go.Layout(
				title=f'Outcome {outcome_variable} broken down by {breakdown_variables}',
				xaxis={
					'title': f'{breakdown_variables}',
					'titlefont': {'color': 'black', 'size': 14},
					'tickfont': {'size': 9, 'color': 'black'}
				},
				yaxis={
					'title': f'{distribution_type} of {outcome_variable}',
					'titlefont': {'color': 'black', 'size': 14},
					'tickfont': {'size': 9, 'color': 'black'}
				}
			)
		}
	else:
		graph_data = {}

	return return_list, graph_data

if __name__ == '__main__':
	app.run_server(debug=False, host='0.0.0.0', port=8050)

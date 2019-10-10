from db_structure import DB, DataManager
from web import flask_app, db
from web.forms import LoginForm, ChangePWForm
from web.models import User, UserGroups, Group
from flask import flash, jsonify, redirect, render_template, request, url_for, session
from flask_login import current_user, login_user, logout_user, fresh_login_required
from functools import wraps
import json
import logging
import os
import pandas as pd
import time

PAGE_ACCESS = {
    'visualization': ['Basic', 'Admin'],
    'config': ['Admin']
}

# https://stackoverflow.com/questions/15871391/implementing-flask-login-with-multiple-user-classes
def login_required(roles=["ANY"]):
    def wrapper(fn):
        @wraps(fn)
        def decorated_view(*args, **kwargs):
            if not current_user.is_authenticated:
                return flask_app.login_manager.unauthorized()
            uroles = current_user.get_roles()
            if (len(set(roles).intersection(uroles)) == 0) and ("ANY" not in roles):
                return flask_app.login_manager.unauthorized()      
            return fn(*args, **kwargs)
        return decorated_view
    return wrapper

def navbar_access():
    uroles = current_user.get_roles()
    user_access = {}
    for page, auth_roles in PAGE_ACCESS.items():
        if (len(set(uroles).intersection(auth_roles)) > 0) or ("ANY" in auth_roles):
            user_access[page] = True
        else:
            user_access[page] = False
    return user_access

@flask_app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('visualization'))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user)

        if user.check_password(user.username):
            flash('You must change your password from the one assigned to you')
            return redirect(url_for('change_pw'))
        
        next_page = request.args.get('next', 'visualization').replace('/', '')
        if next_page == '':
            next_page = 'visualization'
        return redirect(url_for(next_page))
    return render_template('login.html', header='Sign In', form=form)

@flask_app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

@flask_app.route('/change_pw', methods=['GET', 'POST'])
def change_pw():
	form = ChangePWForm()
	if current_user.is_authenticated:
		if form.validate_on_submit():
			if current_user.check_password(form.old_password.data):
				current_user.set_password(form.new_password.data)
				db.session.commit()
				flash('Password successfully changed')
				return redirect(url_for('change_pw'))
			else:
				flash('Old password is incorrect')
				return redirect(url_for('change_pw'))
	else:
		return redirect(url_for('login'))
	return render_template('change_pw.html', header='Change Password', navbar_access=navbar_access(), form=form)

@flask_app.route('/')
@flask_app.route('/index')
@flask_app.route('/visualization')
@login_required(roles=PAGE_ACCESS['visualization'])
def visualization():
    datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir() and f.name[0:6] != 'sample'], key=lambda x: x.upper())
    distribution_choices = {
        'TEXT': ['Count', 'Percents'],
        'NUMERIC': ['Mean', 'Median', 'Sum']
    }
    return render_template('visualization.html', header="Cohort Visualization", datasets=datasets, distribution_choices=distribution_choices, navbar_access=navbar_access())

@flask_app.route('/get_column_info')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_column_info():
    chosen_dataset = request.args.get('chosen_dataset')
    col_idx = request.args.get('idx')

    db = DB(os.path.join('datasets', chosen_dataset))
    dm = DataManager(db)

    col_info = dm.analyze_col_idx(col_idx)

    return jsonify(col_info)

@flask_app.route('/get_graph_data')
@login_required(roles=PAGE_ACCESS['visualization'])
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
    logging.debug(f'DF size before filtering: {len(df)}')

    filters = json.loads(request.args.get('filters', None))
    logging.debug(f'Original filters: {filters}')
    filters = dm.rewrite_filters(filters)
    logging.debug(f'Rewritten filters: {filters}')
    if filters is not None:
        df = dm.filter_df(df, filters)
    logging.debug(f'DF size after filtering: {len(df)}')

    df = dm.aggregate_df(df, groupby_col_idxs=chosen_ind_idxs, filters=filters, aggregate_col_idx=chosen_outcome_idx, aggregate_fxn=aggregate_fxn)

    logging.debug(df)
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
@login_required(roles=PAGE_ACCESS['visualization'])
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

@flask_app.route('/get_table_columns')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_table_columns():
    return_data = {}
    chosen_dataset = request.args.get('chosen_dataset')
    db = DB(os.path.join('datasets', chosen_dataset))
    return_data['table_columns'] = db.get_all_table_columns()
    return_data['column_display_names'] = db.column_display_names
    return_data['column_links'] = db.column_links
    return_data['exclude_columns'] = db.exclude_columns

    return jsonify(return_data)

def create_temp_structure(dataset, config_dict):
    db = DB(os.path.join('datasets', dataset))
    db.finalize(temporary=True)
    # Code to run fake config
    return db

@flask_app.route('/submit_config_dict', methods=['POST'])
@login_required(roles=PAGE_ACCESS['config'])
def submit_config_dict():
    data = request.get_json()
    logging.debug(data)
    dataset = data.get('chosen_dataset')
    config_dict = data.get('config_dict')
    logging.debug(config_dict)
    
    db = DB(os.path.join('datasets', dataset), config_dict=config_dict)
    db.finalize()

    return_data = {
        'column_links': db.column_links,
        'custom_column_names': db.custom_column_names,
        'column_display_names': db.column_display_names,
        'exclude_columns': db.exclude_columns
    }
    logging.debug(return_data)
    return jsonify(return_data)

@flask_app.route('/get_metadata_config')
@login_required(roles=PAGE_ACCESS['config'])
def get_metadata_config():
    chosen_dataset = request.args.get('chosen_dataset')
    db = DB(os.path.join('datasets', chosen_dataset))
    config_dict = db.get_config_dict()
    
    return_data = {
        'common_column_names': db.common_column_names,
        'table_columns': db.get_all_table_columns(),
        'config_dict': config_dict,
        'column_metadata': db.column_metadata
    }

    logging.debug(return_data)

    return jsonify(return_data)

@flask_app.route('/config')
@login_required(roles=PAGE_ACCESS['config'])
def config():
    datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir()], key=lambda x: x.upper())
    return render_template('config.html', header='Configuration', datasets=datasets, navbar_access=navbar_access())

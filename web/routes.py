import db_structure
from web import flask_app, db
from web.forms import LoginForm, ChangePWForm, AddUserForm, PermissionChangeForm
from web.models import ColumnMetadata, DatasetMetadata, TableMetadata, Group, User, UserGroups
from flask import flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_user, logout_user, fresh_login_required
from sqlalchemy.exc import IntegrityError
from functools import wraps
from collections import defaultdict
import json
import logging
import constants as c

PAGE_ACCESS = {
    'visualization': ['Basic', 'Admin'],
    'config': ['Admin'],
    'manage_users': ['Admin']
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
    datasets = sorted([x[0] for x in db.session.query(DatasetMetadata.dataset_name).all()], key=lambda x: x.upper())

    distribution_choices = {
        'DISCRETE': ['Count', 'Percents'],
        'CONTINUOUS': ['Mean', 'Median', 'Sum']
    }
    return render_template('visualization.html', header="Cohort Visualization", datasets=datasets, distribution_choices=distribution_choices, navbar_access=navbar_access())


@flask_app.route('/get_column_info')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_column_info():
    column_id = request.args.get('column_id')
    found_row = db.session.query(ColumnMetadata).filter(ColumnMetadata.id == column_id).first()
    db_extractor = db_structure.DBExtractor(found_row.dataset_name)
    col_info = db_extractor.analyze_column(table=found_row.table_name, column=found_row.column_source_name, limit_rows=10000)

    return jsonify(col_info)


@flask_app.route('/get_graph_data')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_graph_data():
    return_data = {}
    chosen_dataset = request.args.get('chosen_dataset')
    chosen_ind_column_ids = request.args.getlist('chosen_ind_column_ids[]', None)
    chosen_ind_column_ids = [int(x) for x in chosen_ind_column_ids]
    if len(chosen_ind_column_ids) == 0:
        return jsonify({})
    
    chosen_outcome_column_id = request.args.get('chosen_outcome_column_id', None)
    if chosen_outcome_column_id == '':
        chosen_outcome_column_id = None
    else:
        chosen_outcome_column_id = int(chosen_outcome_column_id)

    aggregate_fxn = request.args.get('aggregate_fxn')

    column_metadata = db.session.query(ColumnMetadata).filter(ColumnMetadata.id.in_(chosen_ind_column_ids + [chosen_outcome_column_id])).all()

    db_extractor = db_structure.DBExtractor(dataset_name=chosen_dataset)

    tables = list(set(x.table_name for x in column_metadata))
    table_columns_of_interest = [(x.table_name, x.column_source_name) for x in column_metadata]
    groupby_columns = [f'{x.table_name}_{x.column_source_name}' for x in column_metadata if x.id in chosen_ind_column_ids]

    aggregate_column = None
    aggregate_column_display_name = None
    for x in column_metadata:
        if x.id == chosen_outcome_column_id:
            aggregate_column = f'{x.table_name}_{x.column_source_name}'
            aggregate_column_display_name = x.column_custom_name

    dfs = db_extractor.get_all_dfs_with_tables(tables, table_columns_of_interest=table_columns_of_interest, limit_rows=10000)
    df = sorted(dfs, key=lambda x: len(x))[-1]

    # Gets filters with {column_id: filter data}
    filters_with_id_keys = json.loads(request.args.get('filters', None))
    # Need to rewrite to {table_columnsource: filter_data}
    filters_with_name_keys = {}
    for column_id_str, filter in filters_with_id_keys.items():
        column_id = int(column_id_str)
        for x in column_metadata:
            if x.id == column_id:
                filters_with_name_keys[f'{x.table_name}_{x.column_source_name}'] = filter
                continue
    
    aggregated_df = db_extractor.aggregate_df(df, groupby_columns, filters_with_name_keys, aggregate_column, aggregate_fxn)

    labels = list(aggregated_df['groupby_labels'])
    outcome_possibilities = [x for x in aggregated_df.columns if x != 'groupby_labels']
    datasets = []
    for outcome_possibility in outcome_possibilities:
        datasets.append({
            'label': outcome_possibility,
            'data': list(aggregated_df[outcome_possibility])
        })

    groupby_col_names = [x.column_custom_name for x in column_metadata if x.id in chosen_ind_column_ids]
    groupby_axis_label = ''
    for x in groupby_col_names:
        groupby_axis_label += x + '_'
    groupby_axis_label = groupby_axis_label[:-1]
    if aggregate_column_display_name is None:
        title = f'{aggregate_fxn} broken down by {groupby_axis_label}'
    else:
        title = f'{aggregate_fxn} of {aggregate_column_display_name} broken down by {groupby_axis_label}'

    return_data = {
        'labels': labels,
        'datasets': datasets,
        'title': title,
        'xaxis_label': groupby_axis_label,
        'yaxis_label': aggregate_fxn
    }

    return jsonify(return_data)


@flask_app.route('/get_accessible_tables')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_accessible_tables():
    return_data = {}
    chosen_dataset = request.args.get('chosen_dataset')
    chosen_ind_column_ids = request.args.getlist('chosen_ind_column_ids[]', None)
    chosen_outcome_column_id = request.args.get('chosen_outcome_column_id', None)
    all_tables = [x[0] for x in db.session.query(TableMetadata.table_name).filter(TableMetadata.dataset_name == chosen_dataset).all()]
    
    if len(chosen_ind_column_ids) == 0 and chosen_outcome_column_id in [None, '']:
        # User hasn't chosen anything yet, so both the independent variables and outcome variables will have the same options. This will happen when dataset has changed
        for table in all_tables:
            return_data[table] = True
    else:
        if chosen_outcome_column_id in [None, '']:
            all_chosen_column_ids = chosen_ind_column_ids
        else:
            all_chosen_column_ids = [chosen_outcome_column_id] + chosen_ind_column_ids

        include_tables = list(set([x[0] for x in db.session.query(ColumnMetadata.table_name).filter(ColumnMetadata.id.in_(all_chosen_column_ids))]))
        
        db_extractor = db_structure.DBExtractor(dataset_name=chosen_dataset)
        accessible_tables = db_extractor.get_still_accessible_tables(include_tables=include_tables)
        for table in all_tables:
            if table in include_tables or table in accessible_tables:
                return_data[table] = True
            else:
                return_data[table] = False

    return jsonify(return_data)


@flask_app.route('/get_table_columns')
@login_required(roles=PAGE_ACCESS['visualization'])
def get_table_columns():
    return_data = defaultdict(list)
    chosen_dataset = request.args.get('chosen_dataset')
    column_metadata = db.session.query(ColumnMetadata).filter(ColumnMetadata.dataset_name == chosen_dataset, ColumnMetadata.visible == True, ColumnMetadata.data_type != c.COLUMN_TYPE_DATETIME).all()  # noqa: E712

    for x in column_metadata:
        return_data[x.table_name].append({'column_id': x.id, 'column_custom_name': x.column_custom_name})

    for k, v in return_data.items():
        return_data[k] = sorted(return_data[k], key=lambda x: x['column_custom_name'].upper())

    return jsonify(return_data)


@flask_app.route('/config')
@login_required(roles=PAGE_ACCESS['config'])
def config():
    datasets = sorted([x[0] for x in db.session.query(DatasetMetadata.dataset_name).all()], key=lambda x: x.upper())
    return render_template('config.html', header='Configuration', datasets=datasets, navbar_access=navbar_access())


@flask_app.route('/column_customization', methods=['PUT', 'GET'])
@login_required(roles=PAGE_ACCESS['config'])
def column_customization():
    if request.method == 'GET':
        return_data = defaultdict(list)
        chosen_dataset = request.args.get('chosen_dataset')
        column_data = db.session.query(ColumnMetadata).filter(ColumnMetadata.dataset_name == chosen_dataset).all()

        for x in column_data:
            return_data[x.table_name].append({
                'column_id': x.id,
                'column_source_name': x.column_source_name,
                'column_custom_name': x.column_custom_name,
                'visible': x.visible
            })
        
        return return_data
    elif request.method == 'PUT':
        # inefficient, but not worth trying to do bulk updates
        data = request.get_json()
        logging.info(f'Update customization {data}')
        success = True
        for column_id, new_column_name in data['custom_column_names'].items():
            found_column = db.session.query(ColumnMetadata).filter(ColumnMetadata.id == column_id).first()
            if found_column is None:
                logging.warning(f'Could not find column with id {column_id}')
                success = False
            else:
                found_column.column_custom_name = new_column_name
        db.session.commit()

        for column_id in data['exclude_column_ids']:
            found_column = db.session.query(ColumnMetadata).filter(ColumnMetadata.id == column_id).first()
            if found_column is None:
                logging.warning(f'Could not find column with id {column_id}')
                success = False
            else:
                found_column.visible = False
        db.session.commit()

        for column_id in data['include_column_ids']:
            found_column = db.session.query(ColumnMetadata).filter(ColumnMetadata.id == column_id).first()
            if found_column is None:
                logging.warning(f'Could not find column with id {column_id}')
                success = False
            else:
                found_column.visible = True
        db.session.commit()
        return jsonify(success)


@flask_app.route('/manage_users', methods=['GET', 'POST'])
@login_required(roles=PAGE_ACCESS['manage_users'])
@fresh_login_required
def manage_users():
	add_user_form = AddUserForm()
	permission_change_form = PermissionChangeForm()
	if request.method == 'POST':
		if request.form.get('submit') == 'Add User':
			logging.info(f'Adding user {add_user_form.username.data} - {add_user_form.last_name.data}, {add_user_form.first_name.data}')

			new_user_obj = User(
				username=add_user_form.username.data,
				first_name=add_user_form.first_name.data,
				last_name=add_user_form.last_name.data
			)
			new_user_obj.set_password(add_user_form.username.data)
			db.session.add(new_user_obj)
			try:
				db.session.flush()
				new_user_id = new_user_obj.id
				basic_group_id = db.session.query(Group).filter(Group.group_name == 'Basic').first().id
				new_user_group_rel = UserGroups(user_id=new_user_id, group_id=basic_group_id)
				db.session.add(new_user_group_rel)
				db.session.commit()
				flash(f'Added {add_user_form.username.data}')
			except IntegrityError:
				logging.error(f'Username {add_user_form.username.data} is already in the db')
				flash(f'Username {add_user_form.username.data} is already in the db')
		
		elif request.form.get('submit') == 'Submit User Changes':
			if current_user.check_password(permission_change_form.password.data):
				data = json.loads(request.form['data'])
				logging.debug(f'Submitting change: {data}')
				for new_data in data['updated_permissions']:
					username = new_data.pop('user')
					new_name = new_data.pop('name')
					new_last_name, new_first_name = new_name.replace(' ', '').split(',')
					if new_last_name == 'None':
						new_last_name = None
					if new_first_name == 'None':
						new_first_name = None
					
					user_obj = db.session.query(User).filter(User.username == username).first()
					user_id = user_obj.id
					current_roles = set(user_obj.get_roles())
					user_obj.first_name = new_first_name
					user_obj.last_name = new_last_name
					db.session.commit()
					
					# Remaining keys are all roles
					new_roles = set([k for k, v in new_data.items() if v in ['true', True]])
					new_non_roles = set([k for k, v in new_data.items() if v in ['false', False]])

					add_roles = new_roles - current_roles
					remove_roles = current_roles.intersection(new_non_roles)

					for add_role in add_roles:
						logging.info(f'Adding role {add_role} to {username}')
						group_id = db.session.query(Group).filter(add_role == Group.group_name).first().id
						new_user_group_rel = UserGroups(user_id=user_id, group_id=group_id)
						db.session.add(new_user_group_rel)
					db.session.commit()

					for remove_role in remove_roles:
						logging.info(f'Removing role {remove_role} from {username}')
						group_id = db.session.query(Group).filter(remove_role == Group.group_name).first().id
						db.session.query(UserGroups).filter(UserGroups.user_id == user_id, UserGroups.group_id == group_id).delete()
					db.session.commit()

			else:
				logging.info(f'Invalid password')
				flash('Invalid password')

		return redirect(url_for('manage_users'))

	user_groups = db.session.query(UserGroups, User, Group).join(User, Group).all()
	all_usernames = [x.username for x in db.session.query(User).all()]
	all_groupnames = [x.group_name for x in db.session.query(Group).all()]

	permission_dict = defaultdict(dict)
	for user in all_usernames:
		for group in all_groupnames:
			permission_dict[user][group] = False

	for user_group_access in user_groups:
		permission_dict[user_group_access.User.username][user_group_access.Group.group_name] = True

	permission_list = []
	for user, permissions in permission_dict.items():
		add_dict = {}
		user_obj = db.session.query(User).filter(User.username == user).first()
		add_dict['user'] = user
		add_dict['name'] = user_obj.full_name
		for k, v in permissions.items():
			add_dict[k] = v
		permission_list.append(add_dict)
	return render_template('manage_users.html', header="Manage Users", navbar_access=navbar_access(), permissions=permission_list, add_user_form=add_user_form, permission_change_form=permission_change_form)

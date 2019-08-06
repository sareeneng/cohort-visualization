import json
import os
import pandas as pd
import itertools
from collections import defaultdict
import utilities as u

PARENT_TO = 'is parent to'
CHILD_OF = 'is child of'
SIBLING_OF = 'is sibling of'
TABLE_TYPE_MANY = 'many'
TABLE_TYPE_ONE = 'one'


class DB():
	'''
	A directed graph connects tables together
	If the tables have a 1:1 relationship, then they are siblings (bidirectional arrow in directed graph)
	If the tables have a many:1 relationship, then the many table is the parent of the 1 table (many table is left table in a join) (unidirectional arrow in directed graph)
	There is no support for many:many relationships.
	'''

	def __init__(self, data_dir_db=None, **kwargs):
		self.tables = {}
		self.columns = []
		self.exclude_columns_from_data_viz = []

		if data_dir_db is not None:
			self.load_from_directory(data_dir_db, **kwargs)

	def get_table_names(self):
		return sorted(list(self.tables.keys()))
			
	def load_from_directory(self, data_dir_db, data_dir_base='datasets', data_file_ext='csv', delimiter=',', **kwargs):
		directory_path = os.path.join(data_dir_base, data_dir_db)
		data_files = [x for x in os.listdir(directory_path) if x.endswith(data_file_ext)]

		# check for config file. If exists, then load from config. Otherwise prompt the user
		config_files = [x for x in os.listdir(directory_path) if x.endswith('config')]
		config_valid = False
		config = None

		if len(config_files) > 1:
			print('ERROR: Only one .config file should be present in the directory. Recommend deleting them and re-configuring')
		elif len(config_files) == 1:
			config_path = os.path.join(directory_path, config_files[0])
			with open(config_path, 'r') as f:
				config = json.load(f)

		for file_name in data_files:
			path = os.path.join(directory_path, file_name)
			table_name = file_name.split('.')[0]  # name the table after the file
			df = pd.read_csv(path, delimiter=delimiter)
			self.tables[table_name] = Table(name=table_name, df=df)

		if config is not None:
			try:
				global_fks = config['global_fks']
				custom_fks = config['custom_fks']
				exclude_columns = config['exclude_columns']
				config_valid = True
			except KeyError:
				print('Incomplete config file - will need to reconfigure')

		common_column_names = self.get_common_column_names()
		if config_valid is False:
			print(common_column_names)
			global_fks_valid = False
			while not global_fks_valid:
				global_fks = input('Which column labels are used to join tables together? Use space to separate them. Use * to indicate all. Case-sensitive. ')
				if global_fks == '':
					global_fks = []
					global_fks_valid = True				
				elif global_fks == '*':
					global_fks = common_column_names
					global_fks_valid = True
				else:
					global_fks_valid = True
					global_fks = global_fks.split(' ')
					for x in global_fks:
						if x not in common_column_names:
							print(f'{x} is not a valid column')
							global_fks_valid = False
			
			exclude_columns_choice = input('Do you want to exclude any of these columns from being used as data visualization inputs/outputs [Y/N]? ')
			exclude_columns_valid = True
			if len(exclude_columns_choice) > 0:
				if exclude_columns_choice[0].upper() == 'Y':
					exclude_columns_valid = False

			if exclude_columns_valid:
				exclude_columns = []
			
			while not exclude_columns_valid:
				exclude_columns_input = input('Which columns do you want to exclude? Use space to separate them. Use * to indicate all. Case-sensitive. ')
				if exclude_columns_input == '':
					exclude_columns = []
					exclude_columns_valid = True
				elif exclude_columns_input == '*':
					exclude_columns = common_column_names
					exclude_columns_valid = True
				else:
					exclude_columns_valid = True
					exclude_columns = exclude_columns_input.split(' ')
					for x in exclude_columns:
						if x not in common_column_names:
							print(f'{x} is not a valid column')
							exclude_columns_valid = False

			# Now get custom FKs
			custom_fks = []
			more_fks_input = input('Do you want to create other foreign key links? [Y/N]? ')
			more_fks = False
			if len(more_fks_input) > 0:
				if more_fks_input[0].upper() == 'Y':
					more_fks = True

			if more_fks is False:
				custom_fks = []	

			while more_fks:
				table_names = self.get_table_names()

				print(table_names)
				table_1_valid = False
				while not table_1_valid:
					table_1_name = input("Enter first table (case-sensitive): ")
					if table_1_name in table_names:
						table_1_valid = True
					else:
						print('Table is not valid')

				table_2_valid = False
				while not table_2_valid:
					table_2_name = input("Enter second table (case-sensitive): ")
					if table_2_name in table_names:
						table_2_valid = True
					else:
						print('Table is not valid')
				
				column_1_valid = False
				table_1_column_names = sorted(self.tables[table_1_name].df.columns)
				print(table_1_column_names)
				while not column_1_valid:
					column_1_name = input("Enter the column from the first table that serves as a link (case-sensitive): ")
					if column_1_name in table_1_column_names:
						column_1_valid = True
					else:
						print('Column name is not valid')

				column_2_valid = False
				table_2_column_names = sorted(self.tables[table_2_name].df.columns)
				print(table_2_column_names)
				while not column_2_valid:
					column_2_name = input("Enter the column from the second table that serves as a link (case-sensitive): ")
					if column_2_name in table_2_column_names:
						column_2_valid = True
					else:
						print('Column name is not valid')
				
				add_dict = {'table_1': table_1_name, 'table_2': table_2_name, 'column_1': column_1_name, 'column_2': column_2_name}
				if add_dict not in custom_fks:
					custom_fks.append(add_dict)
				else:
					print('This link has already been set up')

				more_fks_input = input('Do you want to create other foreign key links? [Y/N]? ')
				
				if len(more_fks_input) > 0:
					if more_fks_input[0].upper() != 'Y':
						more_fks = False
				else:
					more_fks = False


			# dump config
			config = {'global_fks': global_fks, 'custom_fks': custom_fks, 'exclude_columns': exclude_columns}
			config_path = os.path.join(directory_path, f'{data_dir_db}.config')
			with open(config_path, 'w') as f:
				json.dump(config, f, indent=4)

		for global_fk in global_fks:
			new_column = Column(shared=True, repetitive=True)
			for table in self.tables.values():
				if global_fk in table.df.columns:
					new_column.add_table(table, df_col_header=f'{global_fk}_[{table.name}]')
					table.link_df_col_name_to_col(col_name=global_fk, col_obj=new_column)
			self.columns.append(new_column)
			self.assign_global_fk(global_fk)
		
		for custom_fk in custom_fks:
			table_1 = self.tables[custom_fk['table_1']]
			table_2 = self.tables[custom_fk['table_2']]

			col_obj = self.find_column_by_table_col_name(table_1, custom_fk['column_1'])
			if col_obj is None:
				col_obj = self.find_column_by_table_col_name(table_2, custom_fk['column_2'])
				if col_obj is None:
					# If neither columns already exist, then create new shared column, and just use column_1 for its name
					repetitive = custom_fk['column_1'] in common_column_names or custom_fk['column_2'] in common_column_names
					col_obj = Column(shared=True, repetitive=repetitive)
					self.columns.append(col_obj)
					
			table_1.link_df_col_name_to_col(custom_fk['column_1'], col_obj)
			table_2.link_df_col_name_to_col(custom_fk['column_2'], col_obj)
			
			col_obj.add_table(table_1, f"{custom_fk['column_1']}_[{table_1.name}]")
			col_obj.add_table(table_2, f"{custom_fk['column_1']}_[{table_2.name}]")

			self.assign_fk(table_1=table_1, table_2=table_2, column_1_name=custom_fk['column_1'], column_2_name=custom_fk['column_2'])			

		self.exclude_columns_from_data_viz = exclude_columns

		for table in self.tables.values():
			for col_name in table.df.columns:
				if col_name not in table.df_col_links:
					repetitive = col_name in common_column_names
					new_column = Column(shared=False, repetitive=repetitive, table=table, df_col_header=f'{col_name}_[{table.name}]')
					table.link_df_col_name_to_col(col_name=col_name, col_obj=new_column)
					self.columns.append(new_column)
			table.add_suffix()

	def get_common_column_names(self):
		# find all columns that are found in multiple tables, these are obvious candidates for joining
		column_counts = defaultdict(int)
		for table in self.tables.values():
			for column in table.df.columns:
				column_counts[column] += 1

		common_columns = [k for k, v in column_counts.items() if v > 1]
		return common_columns

	def assign_global_fks(self, global_fks):
		# option to supply a list of global fks instead of calling the function over and over
		for global_fk in global_fks:
			self.assign_global_fk(global_fk)

	def assign_global_fk(self, global_fk):
		for table_1, table_2 in itertools.combinations(self.tables.values(), 2):
			self.assign_fk(table_1, table_2, column_1_name=global_fk, column_2_name=global_fk)

	def assign_fk(self, table_1, table_2, column_1_name, column_2_name):
		table_1_type = table_1.get_table_type_for_column(column_name=column_1_name)
		table_2_type = table_2.get_table_type_for_column(column_name=column_2_name)
		# four options: many:many, many:one, one:many, or one:one. If column is not found then get_table_type_for_columns will return None

		if table_1_type is not None and table_2_type is not None:
			if table_1.has_relation(table_2):
				print(f'Relation already exists between {table_1.name} and {table_2.name}. Cannot assign two foreign keys between two tables.')  # serves as a safety check
			else:
				if table_1_type == TABLE_TYPE_MANY:
					if table_2_type == TABLE_TYPE_MANY:
						pass  # do not establish a relationship between these two tables
					elif table_2_type == TABLE_TYPE_ONE:
						self.add_parent_child_link(parent_table=table_1, child_table=table_2, parent_column=column_1_name, child_column=column_2_name)
				elif table_1_type == TABLE_TYPE_ONE:
					if table_2_type == TABLE_TYPE_MANY:
						self.add_parent_child_link(parent_table=table_2, child_table=table_1, parent_column=column_2_name, child_column=column_1_name)
					elif table_2_type == TABLE_TYPE_ONE:
						self.add_sibling_link(sibling_1_table=table_1, sibling_2_table=table_2, sibling_1_column=column_1_name, sibling_2_column=column_2_name)

	def add_parent_child_link(self, parent_table, child_table, parent_column, child_column):
		parent_table.children[child_table] = TableRelation(origin_table=parent_table, other_table=child_table, origin_relation_type=PARENT_TO, origin_column=parent_column, other_column=child_column)
		
		child_table.parents[parent_table] = TableRelation(origin_table=child_table, other_table=parent_table, origin_relation_type=CHILD_OF, origin_column=child_column, other_column=parent_column)

	def add_sibling_link(self, sibling_1_table, sibling_2_table, sibling_1_column, sibling_2_column):
		sibling_1_table.siblings[sibling_2_table] = TableRelation(origin_table=sibling_1_table, other_table=sibling_2_table, origin_relation_type=SIBLING_OF, origin_column=sibling_1_column, other_column=sibling_2_column)

		sibling_2_table.siblings[sibling_1_table] = TableRelation(origin_table=sibling_2_table, other_table=sibling_1_table, origin_relation_type=SIBLING_OF, origin_column=sibling_2_column, other_column=sibling_1_column)

	def find_paths_multi_tables(self, list_of_tables, fix_first=False):
		'''
		Given a list of tables in any order, find a path that traverses all of them.

		If fix_first is True, then the first element will remain constant (useful when wanting to break down a specific outcome by various other variables)
		'''
		# first get all combos, these are candidate incomplete paths (missing intermediary tables)
		if len(list_of_tables) == 1:
			return [list_of_tables]

		permutations = itertools.permutations(list_of_tables)
		if fix_first:
			permutations = [x for x in permutations if x[0] == list_of_tables[0]]
		
		valid_incomplete_paths = []
		for permutation in permutations:
			is_valid=True
			for pair in u.pairwise(permutation):
				if len(self.find_paths_between_tables(start_table=pair[0], destination_table=pair[1])) == 0:
					is_valid=False
			if is_valid:
				valid_incomplete_paths.append(permutation)
		
		unflattened_valid_complete_paths = []
		for valid_incomplete_path in valid_incomplete_paths:
			path_possibilities_pairwise = []
			# combo [B,A,F]
			# BA --> [[B,A]]
			# AF --> [[A,D,C,F],[A,C,F],[A,B,E,F]]
			for pair in u.pairwise(valid_incomplete_path):	
				path_possibilities_pairwise.append(self.find_paths_between_tables(start_table=pair[0], destination_table=pair[1]))
			#print(path_possibilities_pairwise)
			combos = itertools.product(*path_possibilities_pairwise)
			for combo in combos:
				unflattened_valid_complete_paths.append(list(combo))
			
		flattened_valid_complete_paths = []
		for l in unflattened_valid_complete_paths:
			flattened_valid_complete_paths.append(list(u.flatten(l)))
		
		flattened_valid_complete_paths = u.remove_adjacent_repeats(flattened_valid_complete_paths)
		
		return flattened_valid_complete_paths

	def find_paths_multi_columns(self, list_of_columns, fix_first=False):
		'''
		Given a list of columns that need to be traversed along a single path, call find_paths_multi_tables to find paths between then	
		Fix_first is useful when you want to breakdown some outcome by different variables. For example, if you want to get Income broken down by State and Profession, then start_column = Income column, and list_of_columns = [State column, Profession column]
		'''

		column_permutations = itertools.permutations(list_of_columns)
		if fix_first:
			column_permutations = [x for x in column_permutations if x[0] == list_of_columns[0]]

		options = []
		for column_permutation in column_permutations:
			table_list = [x.tables for x in column_permutation]
			for x in itertools.product(*table_list):  # get all possible combinations of tables preserving column order within each column permutation
				options.append(list(x))
		
		# Have a list of lists of possible table combinations. Some of these have duplicated tables, remove adjacent repeats within each list and then duplicated lists in full container
		dedup_options = u.remove_adjacent_repeats(options)
		de_dedup_options = u.remove_duplicated_lists(dedup_options)
		
		# Now I'm left with a list of potential table_paths, but many of them are likely not valid paths, and they are incomplete (do not include intermediary)
		valid_column_paths = []
		for table_list in de_dedup_options:
			paths = self.find_paths_multi_tables(table_list, fix_first=fix_first)

			for path in paths:
				valid_column_paths.append(path)

		dedup_valid_column_paths = u.remove_duplicated_lists(valid_column_paths)
		return dedup_valid_column_paths

	def find_paths_between_tables(self, start_table, destination_table, current_path=[]):
		if start_table == destination_table:
			return [start_table]

		all_paths = []
		current_path = current_path.copy() + [start_table]
		if (destination_table in start_table.children) or (destination_table in start_table.siblings):  # immediately return valid path rather than going through children/siblings
			all_paths.append(current_path.copy() + [destination_table])
			return all_paths
			
		elif len(start_table.children) == 0 and len(start_table.siblings) == 0:  # destination table wasn't found and this path has nowhere else to go
			return []

		elif destination_table in start_table.parents:
			# if found in parents, then if parent is not a sibling of any of this tables siblings, then there's no path to get up to that parent
			# if it is a sibling of a sibling, then return that sibling + destination table appended to current_path
			found_sibling_of_sibling = False
			for sibling_name, sibling_relationship in start_table.siblings.items():
				if destination_table == sibling_relationship.other_table:
					found_sibling_of_sibling = True
					all_paths.append(current_path.copy() + [self.tables[sibling_name]] + [destination_table])
			if found_sibling_of_sibling:
				return all_paths
			else:
				return []

		for child_table in start_table.children.keys():
			for path in self.find_paths_between_tables(start_table=child_table, destination_table=destination_table, current_path=current_path):
				all_paths.append(path)
		
		for sibling_table in start_table.siblings.keys():
			if sibling_table not in current_path:  # prevents just looping across siblings forever
				for path in self.find_paths_between_tables(start_table=sibling_table, destination_table=destination_table, current_path=current_path):
					all_paths.append(path)

		return all_paths

	def get_joined_df_options_from_paths(self, paths):
		# given a list of paths, get all dfs that could arise
		df_choices = []
		for path in paths:
			df = path[0].df  # paths returns a list of tables. Initialize with the first table obj in the path
			previous_table = path[0]
			added_tables = [previous_table]
			if len(path) > 1:
				for next_table in path[1:]:
					columns_for_joining = previous_table.get_fks(next_table)
					# ensure that next_table was not already added in case the path backtracks, otherwise skip over
					if next_table not in added_tables:
						df = pd.merge(df, next_table.df, left_on=f'{columns_for_joining[0]}_[{previous_table.name}]', right_on=f'{columns_for_joining[1]}_[{next_table.name}]')
						added_tables.append(next_table)
					previous_table = next_table
			df_choices.append(df)
		return df_choices

	def get_joined_df_options(self, table_1, table_2):
		# return all dfs that arise from each possible path between two tables
		possible_paths = self.find_paths_between_tables(start_table=table_1, destination_table=table_2)

		return self.get_joined_df_options_from_paths(possible_paths)

	def get_biggest_joined_df_option_from_paths(self, paths):
		df_choices = self.get_joined_df_options_from_paths(paths)

		idx = 0
		longest_length = 0
		counter = 0
		if len(df_choices) > 0:
			for df in df_choices:
				if len(df) > longest_length:
					longest_length = len(df)
					idx = counter
				counter += 1
		else:
			return None
		
		return df_choices[idx]

	def get_biggest_joined_df_option(self, table_1, table_2):
		# return only the longest df that arises from looking at each possible path

		possible_paths = self.find_paths_between_tables(start_table=table_1, destination_table=table_2)

		return self.get_biggest_joined_df_option_from_paths(possible_paths)

	def get_still_accessible_tables(self, include_tables, fix_first=False):
		# Given a list of include_tables that must be in a valid path (not necessarily in order), iterate through the rest of the tables to figure out if there are paths between include_tables and each of those
		possible_tables = [x for x in self.tables.values() if x not in include_tables]

		accessible_tables = []
		for possible_table in possible_tables:
			check_tables = include_tables + [possible_table]
			if len(self.find_paths_multi_tables(check_tables, fix_first=fix_first)) > 0:
				accessible_tables.append(possible_table)
		return accessible_tables

	def get_still_accessible_columns(self, include_columns):
		# Given a list of include_columns that must be in a valid path (not necessarily in order, but the function will preserve order), iterate through the rest of the columns to figure out if there are paths between include_columns and each of those
		# One way to do this is to get all possible combinations of the tables that each include_column is in, figure out which tables are accessible, and then add those tables columns in
		
		accessible_columns = set()
		include_table_options = [x.tables for x in include_columns]
		table_products = itertools.product(*include_table_options)

		dedup_table_products = [u.remove_duplicates(x) for x in table_products]
		de_dedup_table_products = u.remove_duplicated_lists(dedup_table_products)
		
		accessible_tables = set()
		all_paths = []
		for table_combo in de_dedup_table_products:
			potential_paths = self.find_paths_multi_tables(table_combo)
			for path in potential_paths:
				all_paths.append(path)
				for traversed_table in path:
					accessible_tables.add(traversed_table)
		
		# traversed tables represent all the tables I could touch with the columns already included. This means that all their columns are accessible already
		for table in accessible_tables:
			for column in table.get_columns():
				accessible_columns.add(column)
		
		remaining_tables = [x for x in self.tables.values() if x not in accessible_tables]

		# potential paths represents all complete paths that can traverse between the columns in include_columns. To check if the other tables are accessible, see if there's a path between the last member in each path and the remaining table
		for remaining_table in remaining_tables:
			found_valid_path = False
			for path in all_paths:
				if not found_valid_path:
					if len(self.find_paths_between_tables(path[-1], remaining_table)) > 0:
						found_valid_path = True
						accessible_tables.add(remaining_table)
						for column in remaining_table.get_columns():
							accessible_columns.add(column)

		return list(accessible_columns)

	def find_column_by_table_col_name(self, table, column_name):
		for column in self.columns:
			for table in column.tables:
				if column.table_links.get(table, None) == f'{column_name}_[{table.name}]':
					return column
		return None

class Table():
	def __init__(self, name, df):
		self.name = name
		self.df = df
		self.children = {}
		self.siblings = {}
		self.parents = {}
		self.df_col_links = {}

	def __hash__(self):
		return hash(self.name)

	def __eq__(self, other):
		return self.name == other.name

	def link_df_col_name_to_col(self, col_name, col_obj):
		self.df_col_links[col_name] = col_obj

	def add_suffix(self):
		self.df = self.df.add_suffix(f'_[{self.name}]')

	def has_relation(self, other_table):
		return other_table.name in self.get_children_names() + self.get_sibling_names() + self.get_parent_names()

	def get_children_names(self):
		return [x.name for x in self.children.keys()]
	
	def get_sibling_names(self):
		return [x.name for x in self.siblings.keys()]

	def get_parent_names(self):
		return [x.name for x in self.parents.keys()]

	def get_table_type_for_column(self, column_name):
		if column_name not in self.df.columns:
			return None
		
		if len(self.df[column_name].dropna()) > len(self.df[column_name].dropna().unique()):
			return TABLE_TYPE_MANY

		if len(self.df[column_name].dropna()) == len(self.df[column_name].dropna().unique()):
			return TABLE_TYPE_ONE

		return None

	def get_fks(self, next_table):
		if next_table in self.children:
			relation_data = self.children[next_table]
		elif next_table in self.siblings:
			relation_data = self.siblings[next_table]
		else:
			return None

		return (relation_data.origin_column, relation_data.other_column)

	def get_column_by_name(self, column_name):
		return self.df_col_links.get(column_name, None)

	def get_columns(self):
		return list(self.df_col_links.values())

	def __repr__(self):
		return self.name

	
class TableRelation():
	def __init__(self, origin_table, other_table, origin_relation_type, origin_column, other_column):
		self.origin_table = origin_table
		self.other_table = other_table
		self.origin_relation_type = origin_relation_type  # origin_table is ___ other_table
		self.origin_column = origin_column
		self.other_column = other_column


class Column():
	def __init__(self, shared=False, repetitive=False, table=None, df_col_header=None):
		self.shared = shared
		self.repetitive = repetitive
		self.tables = set()
		self.table_links = {}
		if table is not None:
			self.add_table(table, df_col_header)

	def __hash__(self):
		sorted_tables = sorted([x for x in self.table_links.keys()], key = lambda y: y.name)
		sorted_table_names = tuple([x.name for x in sorted_tables])
		df_col_headers = tuple([self.table_links[x] for x in sorted_tables])
		return hash((sorted_table_names, df_col_headers))

	def __eq__(self, other):
		return self.table_links == other.table_links

	@property
	def display_name(self):
		first_table = sorted([x for x in self.tables], key = lambda y: y.name)[0]
		if self.shared:
			return self.prune_table_from_string(self.table_links[first_table])

		if self.repetitive:
			return f'{self.table_links[first_table]}'

		return self.prune_table_from_string(self.table_links[first_table])

	def prune_table_from_string(self, to_prune):
		idx = to_prune.rfind('_[')
		return to_prune[:idx]

	def add_table(self, new_table, df_col_header):
		self.tables.add(new_table)
		self.table_links[new_table] = df_col_header

	def get_table_names(self):
		table_list = list(self.tables)
		return [x.name for x in table_list]			 

	def __repr__(self):
		return_str = ''
		sorted_tables = sorted([x for x in self.tables], key = lambda y: y.name)
		for table in sorted_tables:
			table_link = self.table_links[table]
			return_str += table.name + '->' + self.prune_table_from_string(table_link) + ' & '
		if len(return_str) > 2:
			return_str = return_str[:-3]
		return return_str
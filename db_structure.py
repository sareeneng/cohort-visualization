import json
import os
import pandas as pd
import itertools
from functools import wraps
from collections import defaultdict
import utilities as u
import logging

TABLE_TYPE_MANY = 'MANY'
TABLE_TYPE_ONE = 'ONE'

'''
A directed graph connects tables together
If the tables have a 1:1 relationship, then they are siblings (bidirectional arrow in directed graph)
If the tables have a many:1 relationship, then the many table is the parent of the 1 table (many table is left table in a join) (unidirectional arrow in directed graph)
There is no support for many:many relationships.
'''

class DB():
	def __init__(self, directory_path, arch_file=None, data_file_extension='.csv', delimiter=','):
		logging.info(f'Loading {directory_path}')
		self.finalized = False
		self.directory_path = directory_path
		self.dataset_name = os.path.split(directory_path)[1]
		
		if arch_file is None:
			arch_files = u.find_file_types(directory_path, '.arch')
			if len(arch_files) == 1:
				arch_file = arch_files[0]
			elif len(arch_files) > 1:
				e = f'Multiple .arch files found in {directory_path}'
				logging.error(e)
				raise ValueError(e)

		if arch_file is not None:
			arch_path = os.path.join(directory_path, arch_file)
			self.load_arch_file(arch_path)
		else:
			logging.info(f'No .arch file found in {directory_path}')
			
			self.data_file_extension = data_file_extension
			self.delimiter = delimiter
			self.table_names = []
			self.table_metadata = defaultdict(dict)
			self.column_metadata = defaultdict(dict)
			self.common_column_names = []
			
			self.collect_metadata()

			self.global_fks = []
			self.custom_fks = []
			self.exclude_columns = []
			self.custom_column_names = defaultdict(defaultdict)
			self.column_links = defaultdict(dict)
			self.column_display_names = {}
			self.table_relationships = defaultdict(dict)

	def load_arch_file(self, arch_path):
		logging.info(f'Loading arch file: {arch_path}')
		with open(arch_path, 'r') as f:
			arch_data = json.load(f)
			self.directory_path = arch_data['directory_path']
			self.data_file_extension = arch_data['data_file_extension']
			self.delimiter = arch_data['delimiter']
			self.table_names = arch_data['table_names']
			self.table_metadata = arch_data['table_metadata']
			self.column_metadata = arch_data['column_metadata']
			self.common_column_names = arch_data['common_column_names']
			self.global_fks = arch_data['global_fks']
			self.custom_fks = arch_data['custom_fks']
			self.exclude_columns = arch_data['exclude_columns']
			self.custom_column_names = arch_data['custom_column_names']
			self.column_links = arch_data['column_links']
			self.column_display_names = arch_data['column_display_names']
			self.table_relationships = arch_data['table_relationships']
		self.finalized = True

	def dump_data(self):
		arch_path = f'{os.path.join(self.directory_path, self.dataset_name)}.arch'
		logging.info(f'Dumping arch file to: {arch_path}')
		arch_data = {
			'directory_path': self.directory_path,
			'data_file_extension': self.data_file_extension,
			'delimiter': self.delimiter,
			'table_names': self.table_names,
			'table_metadata': self.table_metadata,
			'column_metadata': self.column_metadata,
			'common_column_names': self.common_column_names,
			'global_fks': self.global_fks,
			'custom_fks': self.custom_fks,
			'exclude_columns': self.exclude_columns,
			'custom_column_names': self.custom_column_names,
			'column_links': self.column_links,
			'column_display_names': self.column_display_names,
			'table_relationships': self.table_relationships
		}
		with open(arch_path, 'w') as f:
			json.dump(arch_data, f, indent=4)
	
	def collect_metadata(self, data_file_extension='.csv', delimiter=','):
		# Could not find a .arch file to load data from, so we need to find out information about the directory provided
		logging.info(f'Calculating metadata for files in directory {self.directory_path}')
		self.data_file_extension = data_file_extension
		self.delimiter = delimiter

		file_names = u.find_file_types(self.directory_path, data_file_extension)

		for file_name in file_names:
			idx = file_name.rfind('.')
			table_name = file_name[:idx]
			self.table_names.append(table_name)
			self.table_metadata[table_name]['file'] = os.path.join(self.directory_path, file_name)

			df = pd.read_csv(os.path.join(self.directory_path, file_name), delimiter=self.delimiter)
			self.column_metadata[table_name] = defaultdict(dict)
			for column in df.columns:
				if len(df[column].dropna()) > len(df[column].dropna().unique()):
					table_type = TABLE_TYPE_MANY
				else:
					table_type = TABLE_TYPE_ONE
				self.column_metadata[table_name][column]['type'] = table_type
		self.common_column_names = self.get_common_column_names()

	###############
	# Metadata type fxns - these are completely dependent on the files
	###############
	
	def get_common_column_names(self):
		column_counts = defaultdict(int)
		for column_dicts in self.column_metadata.values():
			for column in column_dicts:
				column_counts[column] += 1
		common_columns = [k for k, v in column_counts.items() if v > 1]
		return common_columns

	def get_table_columns(self, table):
		return list(self.column_metadata[table].keys())

	def get_all_table_columns(self):
		return_dict = {}
		for table in  self.table_names:
			return_dict[table] = self.get_table_columns(table)
		return return_dict

	def table_has_column(self, table, column):
		return self.column_metadata[table].get(column, None) is not None 

	def get_column_type(self, table, column):
		return self.column_metadata[table][column]['type']

	###############
	# Config type fxns - user provides information re: how to link the files together. This does not actually create the connections yet
	###############
	
	@property
	def config_changes_allowed(self):
		# Probably could be put in a decorator but need to figure out how to do that for class methods
		if self.finalized:
			logging.error('Cannot make configuration changes because it has been finalized either by being loaded from a .arch file or from the finalize() fxn being run. Please run unfinalize(), make your configuration changes, and then run finalize() when you are finished in order to generate a new .arch file')
			return False
		return True

	def finalize(self):
		if self.finalized:
			logging.error('DB has already been marked as finalized')
		else:
			self.generate_links()
			self.dump_data()
			self.finalized = True

	def unfinalize(self):
		self.finalized = False

	def add_global_fks(self, global_fks):
		if self.config_changes_allowed:
			for global_fk in global_fks:
				self.add_global_fk(global_fk)

	def add_global_fk(self, global_fk):
		if self.config_changes_allowed:
			# ensure global_fk is in common_column_names
			if global_fk not in self.common_column_names:
				logging.error(f'{global_fk} is not in common column names: {self.common_column_names}')
			else:
				self.global_fks.append(global_fk)
		
	def add_exclude_column(self, exclude_column):
		if self.config_changes_allowed:
			self.exclude_columns.append(exclude_column)

	def add_custom_column_name(self, table, column, new_column_name):
		if self.config_changes_allowed:
			self.custom_column_names[table][column] = new_column_name

	def add_custom_fk(self, table_1, table_2, column_1, column_2):
		if self.config_changes_allowed:
			if table_1 not in self.table_names:
				logging.error(f'Table {table_1} not found')
				raise ValueError
			if table_2 not in self.table_names:
				logging.error(f'Table {table_2} not found')
				raise ValueError
			if self.table_has_column(table_1, column_1) is False:
				logging.error(f'Column {column_1} not found in table {table_1}')
				raise ValueError
			if self.table_has_column(table_2, column_2) is False:
				logging.error(f'Column {column_2} not found in table {table_2}')
				raise ValueError

			# Now ensure that this is not creating a many:many relationship
			table_1_type = self.get_column_type(table_1, column_1)
			table_2_type = self.get_column_type(table_2, column_2)

			if table_1_type == TABLE_TYPE_MANY and table_2_type == TABLE_TYPE_MANY:
				logging.error(f'Cannot link {table_1}->{column_1} and {table_2}->{column_2} because they have a many-to-many relationship')
				raise ValueError

			add_dict = {'table_1': table_1, 'table_2': table_2, 'column_1': column_1, 'column_2': column_2}
			
			self.custom_fks.append(add_dict)
	
	###############
	# Structure of database - generate links given generated metadata and user-defined configuration
	###############

	def generate_links(self):
		self.column_factory = ColumnFactory(custom_column_names=self.custom_column_names)
		
		for table in self.table_names:
			self.table_relationships[table] = {
				'parents': {},
				'children': {},
				'siblings': {}
			}

		logging.debug(f'Linking {self.global_fks} global_fks')
		self.link_global_fks(self.global_fks)

		for custom_fk in self.custom_fks:
			logging.debug(f'Linking custom_fk {custom_fk}')
			self.link_custom_fk(custom_fk)

		# Now do the rest of the columns that have not been linked by global_fks or by custom_fks
		all_table_columns = self.get_all_table_columns()
		for table, column_list in all_table_columns.items():
			for column in column_list:
				idx = self.get_col_idx(table, column)
				if idx is None:
					repetitive = column in self.common_column_names
					new_column = self.column_factory.create_column(shared=False, repetitive=repetitive)
					self.column_links[new_column.id][table] = column

		for col_idx, table_links in self.column_links.items():
			self.create_fk_relationship(col_idx, table_links)
		
		self.assign_display_names()

	def link_global_fks(self, global_fks):
		for global_fk in global_fks:
			self.link_global_fk(global_fk)

	def link_global_fk(self, global_fk):
		new_column = self.column_factory.create_column(shared=True, repetitive=True)
		self.column_links[new_column.id] = {}

		for table in self.table_names:
			if self.table_has_column(table, global_fk):
				self.column_links[new_column.id][table] = global_fk
	
	def link_custom_fk(self, custom_fk):
		table_1 = custom_fk['table_1']
		table_2 = custom_fk['table_2']
		column_1 = custom_fk['column_1']
		column_2 = custom_fk['column_2']
		
		# First ensure that neither of the columns is already linked to a column. If it is, then use that dictionary to add them instead of creating a new column
		logging.debug(f'{table_1}.{column_1} --> {table_2}.{column_2}')
		idx = self.get_col_idx(table_1, column_1)
		if idx is None:
			idx = self.get_col_idx(table_2, column_2)
		
		if idx is None:
			repetitive = column_1 in self.common_column_names or column_2 in self.common_column_names
			col_obj = self.column_factory.create_column(shared=True, repetitive=repetitive)
			idx = col_obj.id
		else:
			logging.info(f'Linking to column {idx}')
		
		self.column_links[idx][table_1] = column_1
		self.column_links[idx][table_2] = column_2

	def create_fk_relationship(self, col_idx, table_links, suppress_warning=False):
		table_combinations = itertools.combinations(table_links, 2)
		for table_combination in table_combinations:
			table_1 = table_combination[0]
			table_2 = table_combination[1]

			column_1 = table_links[table_1]
			column_2 = table_links[table_2]

			column_1_type = self.get_column_type(table_1, column_1)
			column_2_type = self.get_column_type(table_2, column_2)
			# four options: many:many, many:one, one:many, or one:one.

			if self.table_relationship_exists(table_1, table_2):
				logging.info(f'Relation already exists between {table_1} and {table_2}. Cannot assign two foreign keys between two tables.')  # serves as a safety check
			else:
				if column_1_type == TABLE_TYPE_MANY:
					if column_2_type == TABLE_TYPE_MANY:
						logging.debug(f'Cannot make relationship between {table_1}->{column_1} and {table_2}->{column_2} because they have a many-to-many relationship')
					elif column_2_type == TABLE_TYPE_ONE:
						self.add_parent_child_link(col_idx, parent_table=table_1, child_table=table_2)
				elif column_1_type == TABLE_TYPE_ONE:
					if column_2_type == TABLE_TYPE_MANY:
						self.add_parent_child_link(col_idx, parent_table=table_2, child_table=table_1)
					elif column_2_type == TABLE_TYPE_ONE:
						self.add_sibling_link(col_idx, sibling_1_table=table_1, sibling_2_table=table_2)

	def add_parent_child_link(self, col_idx, parent_table, child_table):
		self.table_relationships[parent_table]['children'][child_table] = col_idx
		self.table_relationships[child_table]['parents'][parent_table] = col_idx

	def add_sibling_link(self, col_idx, sibling_1_table, sibling_2_table):
		self.table_relationships[sibling_1_table]['siblings'][sibling_2_table] = col_idx
		self.table_relationships[sibling_2_table]['siblings'][sibling_1_table] = col_idx

	def get_col_idx(self, table, column):
		# Check to see if this table_name->column_name pair is already associated with an existing column
		for idx, table_links in self.column_links.items():
			for table_existing, column_existing in table_links.items():
				if table_existing == table and column_existing == column:
					return idx
		return None

	def get_table_col_idxs(self, table):
		return_list = []
		for idx, data in self.column_links.items():
			if table in data.keys():
				return_list.append(idx)
		return return_list

	def get_tables_by_col_idx(self, idx):
		return sorted(list(self.column_links[idx].keys()))

	def get_df_col_headers_by_idx(self, idx, table=None):
		if table is not None:
			column = self.column_links[idx][table]
			return f'{column}_[{table}]'
		
		col_headers = []
		for table, column in self.column_links[idx].items():
			col_headers.append(f'{column}_[{table}]')
		
		return col_headers
	def get_joining_col_idx(self, table_1, table_2):
		col_idx_1 = self.get_table_col_idxs(table_1)
		col_idx_2 = self.get_table_col_idxs(table_2)
		return set(col_idx_1).intersection(col_idx_2).pop()

	def assign_display_names(self):
		for idx, col_obj in self.column_factory.columns.items():
			custom_name = self.custom_column_names.get(idx, None)
			if custom_name is not None:
				self.column_display_names[idx] = custom_name
			else:			
				column_tables = self.get_tables_by_col_idx(idx)
				first_table = column_tables[0]
				first_table_column_name = self.column_links[idx][first_table]
				if col_obj.shared:
					self.column_display_names[idx] = first_table_column_name
				elif col_obj.repetitive:
					self.column_display_names[idx] = f'{first_table_column_name}_[{first_table}]'
				else:
					self.column_display_names[idx] = first_table_column_name

	def get_all_column_display_names(self):
		return self.column_display_names
	
	###############
	# Pathfinding functions, determines whether relationships exist and how to get from one column/table to another
	###############

	def table_relationship_exists(self, table_1, table_2):
		if table_2 in self.find_table_all_related_tables(table_1):
			return True
		if table_1 in self.find_table_all_related_tables(table_2):
			return True
		return False

	def find_table_all_related_tables(self, table):
		# Return parents, children, and siblings of given table
		tr = self.table_relationships[table]
		return sorted(list(set(list(tr['parents'].keys()) + list(tr['children'].keys()) + list(tr['siblings'].keys()))))

	def find_table_all_connectable_tables(self, table):
		# Return children and siblings, e.g. tables that I can go to next from this table
		tr = self.table_relationships[table]
		return sorted(list(set(list(tr['children'].keys()) + list(tr['siblings'].keys()))))

	def find_table_children(self, table):
		tr = self.table_relationships[table]
		return sorted(list(tr['children'].keys()))

	def find_table_siblings(self, table):
		tr = self.table_relationships[table]
		return sorted(list(tr['siblings'].keys()))

	def find_table_parents(self, table):
		tr = self.table_relationships[table]
		return sorted(list(tr['parents'].keys()))

	def find_table_still_accessible_tables(self, include_tables, fix_first=False):
		# Given a list of include_tables that must be in a valid path (not necessarily in order), iterate through the rest of the tables to figure out if there are paths between include_tables and each of those
		possible_tables = [x for x in self.table_names if x not in include_tables]

		accessible_tables = []
		for possible_table in possible_tables:
			check_tables = include_tables + [possible_table]
			if len(self.find_paths_multi_tables(check_tables, fix_first=fix_first)) > 0:
				accessible_tables.append(possible_table)
		return accessible_tables

	def find_column_idxs_still_accessible_idxs(self, include_column_idxs):
		# Given a list of include_columns that must be in a valid path (not necessarily in order, but the function will preserve order), iterate through the rest of the columns to figure out if there are paths between include_columns and each of those
		# One way to do this is to get all possible combinations of the tables that each include_column is in, figure out which tables are accessible, and then add those tables columns in
		
		accessible_columns = set()
		include_table_options = [self.get_tables_by_col_idx(x) for x in include_column_idxs]
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
			for idx in self.get_table_col_idxs(table):
				accessible_columns.add(idx)
		
		remaining_tables = [x for x in self.table_names if x not in accessible_tables]

		# potential paths represents all complete paths that can traverse between the columns in include_columns. To check if the other tables are accessible, see if there's a path between the last member in each path and the remaining table
		for remaining_table in remaining_tables:
			found_valid_path = False
			for path in all_paths:
				if not found_valid_path:
					if len(self.find_paths_between_tables(path[-1], remaining_table)) > 0:
						found_valid_path = True
						accessible_tables.add(remaining_table)
						for idx in self.get_table_col_idxs(remaining_table):
							accessible_columns.add(idx)

		return list(accessible_columns)

	def find_paths_between_tables(self, start_table, destination_table, current_path=[]):
		if start_table == destination_table:
			return [start_table]

		all_paths = []
		
		current_path = current_path.copy() + [start_table]
		if (destination_table in self.find_table_all_connectable_tables(start_table)):  # immediately return valid path rather than going through children/siblings
			all_paths.append(current_path.copy() + [destination_table])
			return all_paths
			
		elif len(self.find_table_children(start_table)) == 0 and len(self.find_table_siblings(start_table)) == 0:  # destination table wasn't found and this path has nowhere else to go
			return []

		elif destination_table in self.find_table_parents(start_table):
			# if found in parents, then if parent is not a sibling of any of this tables siblings, then there's no path to get up to that parent
			# if it is a sibling of a sibling, then return that sibling + destination table appended to current_path
			found_sibling_of_sibling = False
			start_table_siblings = self.find_table_siblings(start_table)
			for start_table_sibling in start_table_siblings:
				sibling_siblings = self.find_table_siblings(start_table_sibling)
				if destination_table in sibling_siblings:
					found_sibling_of_sibling = True
					all_paths.append(current_path.copy() + [start_table_sibling] + [destination_table])
			if found_sibling_of_sibling:
				return all_paths
			else:
				return []

		for child_table in self.find_table_children(start_table):
			for path in self.find_paths_between_tables(start_table=child_table, destination_table=destination_table, current_path=current_path):
				all_paths.append(path)
		
		for sibling_table in self.find_table_siblings(start_table):
			if sibling_table not in current_path:  # prevents just looping across siblings forever
				for path in self.find_paths_between_tables(start_table=sibling_table, destination_table=destination_table, current_path=current_path):
					all_paths.append(path)

		return all_paths

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

	def find_paths_multi_columns(self, list_of_column_idxs, fix_first=False):
		'''
		Given a list of columns that need to be traversed along a single path, call find_paths_multi_tables to find paths between then	
		Fix_first is useful when you want to breakdown some outcome by different variables. For example, if you want to get Income broken down by State and Profession, then start_column = Income column, and list_of_columns = [State column, Profession column]
		'''

		column_permutations = itertools.permutations(list_of_column_idxs)
		if fix_first:
			column_permutations = [x for x in column_permutations if x[0] == list_of_column_idxs[0]]

		options = []
		for column_permutation in column_permutations:
			table_list = [self.get_tables_by_col_idx(x) for x in column_permutation]
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
	

class DataManager():
	def __init__(self, db, load_all_data=False):
		self.db = db
		self.table_dfs = {}
		if load_all_data:
			self.load_all_tables()

	def load_all_tables(self):
		for table, table_metadata in self.db.table_metadata.items():
			file_path = table_metadata['file']
			logging.debug(f'Loading file {file_path}')
			self.table_dfs[table] = pd.read_csv(file_path, delimiter=self.db.delimiter).add_suffix(f'_[{table}]')

	def load_tables(self, tables):
		for table in tables:
			file_path = self.db.table_metadata[table]['file']
			logging.debug(f'Loading file {file_path}')
			self.table_dfs[table] = pd.read_csv(file_path, delimiter=self.db.delimiter).add_suffix(f'_[{table}]')

	def get_joined_df_options_from_paths(self, paths, filter_col_idxs=None):
		# given a list of paths, get all dfs that could arise
		df_choices = []

		for path in paths:
			df = self.table_dfs[path[0]]  # paths returns a list of tables. Initialize with the first table obj in the path
			previous_table = path[0]
			added_tables = [previous_table]
			if len(path) > 1:
				for next_table in path[1:]:
					col_idx_joining = self.db.get_joining_col_idx(previous_table, next_table)
					# ensure that next_table was not already added in case the path backtracks, otherwise skip over
					if next_table not in added_tables:
						left_column = self.db.column_links[col_idx_joining][previous_table]
						left_column = f'{left_column}_[{previous_table}]'
						right_column = self.db.column_links[col_idx_joining][next_table]
						right_column = f'{right_column}_[{next_table}]'

						df = pd.merge(df, self.table_dfs[next_table], left_on=left_column, right_on=right_column)
						added_tables.append(next_table)
					previous_table = next_table
			
			if filter_col_idxs is not None:
				col_headers = []
				for idx in filter_col_idxs:
					idx_all_headers = self.db.get_df_col_headers_by_idx(idx)
					common_headers = [x for x in idx_all_headers if x in df.columns]
					col_headers += common_headers
				df = df.loc[:, col_headers]			
			df_choices.append(df)
		return df_choices

	def get_biggest_joined_df_option_from_paths(self, paths, filter_col_idxs=None):
		df_choices = self.get_joined_df_options_from_paths(paths, filter_col_idxs)

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

	def aggregate_df(self, df, groupby_col_idxs, aggregate_col_idx=None, aggregate_fxn='Count'):
		groupby_col_headers = []
		aggregate_col_header = None
		for idx in groupby_col_idxs:
			col_headers = self.db.get_df_col_headers_by_idx(idx)
			groupby_col_headers += [x for x in col_headers if x in df.columns]
		if aggregate_col_idx is not None:
			col_headers = self.db.get_df_col_headers_by_idx(aggregate_col_idx)
			aggregate_col_header = next(x for x in col_headers if x in df.columns)

		def get_breakdown_label(row, ind_variables):
			return_str = ''
			for x in ind_variables:
				return_str += str(row[x]) + '_'
			return_str = return_str[:-1]  # remove trailing underscore
			return return_str

		if aggregate_col_header is None:
			# just get the counts then
			df = df.groupby(groupby_col_headers).size().reset_index(name="Count")
		else:
			g = df.groupby(groupby_col_headers)[aggregate_col_header]

			if aggregate_fxn == 'Count':
				df = g.value_counts().unstack().reset_index()
			elif aggregate_fxn == 'Percents':
				df = (g.value_counts(normalize=True)*100).round(1).unstack().reset_index()
			elif aggregate_fxn == 'Sum':
				df = g.sum().reset_index()
			elif aggregate_fxn == 'Mean':
				df = (g.mean()).round(2).reset_index()
		
		df['groupby_labels'] = df.apply(lambda x: get_breakdown_label(x, groupby_col_headers), axis=1)
		df = df.drop(columns=groupby_col_headers)
		
		return df


class ColumnFactory():
	def __init__(self, custom_column_names):
		self.counter = 0
		self.custom_column_names = custom_column_names
		self.columns = {}

	def create_column(self, **kwargs):
		new_column = Column(id=self.counter, **kwargs)
		self.columns[self.counter] = new_column
		self.counter += 1
		return new_column
		
class Column():
	def __init__(self, id, shared=False, repetitive=False):
		self.id = id
		self.shared = shared
		self.repetitive = repetitive
		self.display_name = None

	def __repr__(self):
		return f'Column {self.id}'

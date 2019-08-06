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


class DB():
	'''
	A directed graph connects tables together
	If the tables have a 1:1 relationship, then they are siblings (bidirectional arrow in directed graph)
	If the tables have a many:1 relationship, then the many table is the parent of the 1 table (many table is left table in a join) (unidirectional arrow in directed graph)
	There is no support for many:many relationships.
	'''

	def __init__(self, directory_path, setup_file=None, load_data=True, metadata_file=None, config_file=None, data_file_extension='.csv', delimiter=','):
		self.directory_path = directory_path
		self.dataset_name = os.path.split(directory_path)[1]
		self.setup_complete = False
		self.tables = {}

		if setup_file is None:
			setup_files = u.find_file_types(directory_path, '.setup')
			if len(setup_files) == 1:
				setup_file = setup_files[0]
			elif len(setup_files) > 1:
				e = f'Multiple .setup files found in {directory_path}'
				logging.error(e)
				raise ValueError(e)
		
		self.metadata = Metadata()  # will create metadata if not provided
		self.config = Config(self.metadata)

		if setup_file is None:
			logging.info('No .setup file provided or found')
			self.metadata.init_from_directory(directory_path, data_file_extension, delimiter)
		else:
			self.load_setup_file(setup_path=os.path.join(directory_path, setup_file))

		if load_data:
			self.load_data_files(directory_path, data_file_extension, delimiter)

	def load_setup_file(self, setup_path):
		logging.info(f'Loading setup file: {setup_path}')
		with open(setup_path, 'r') as f:
			data = json.load(f)
			self.metadata.load_data(data['metadata'])
			self.config.load_data(data['config'])
			self.struct = Struct(self.metadata, self.config)
		self.setup_complete = True

	def setup_changes_allowed(self):
		if self.setup_complete:
			logging.error('Cannot make changes to setup. Run unfinalize_setup() if you want to make changes, and then run finalize_setup when you are finished to generate a new .setup file')
			return False
		return True

	def unfinalize_setup(self):
		# Used if I want to edit config
		self.setup_complete = False
	
	def finalize_setup(self):
		# Using metadata and config, generate struct and dump everything to .setup file
		if self.setup_complete:
			logging.error('Setup has already been marked as complete')
		else:
			self.struct = Struct(self.metadata, self.config)
			setup_path = os.path.join(self.directory_path, f'{self.dataset_name}.setup')
			data = {
				'metadata': self.metadata.dump_data(),
				'config': self.config.dump_data()
			}
			with open(setup_path, 'w') as f:
				logging.info(f'Dumping setup data to {setup_path}')
				json.dump(data, f, indent=4)
			self.setup_complete = True
	
	def load_data_files(self, directory_path, data_file_extension, delimiter):
		file_names = u.find_file_types(directory_path, data_file_extension)

		for file_name in file_names:
			logging.debug(f'Loading file {file_name}')
			idx = file_name.rfind('.')
			table_name = file_name[:idx]
			df = pd.read_csv(os.path.join(directory_path, file_name), delimiter=delimiter)
			self.tables[table_name] = Table(name=table_name, df=df, add_suffix=True)
	
	def add_global_fks_to_config(self, global_fks):
		if self.setup_changes_allowed():
			self.config.add_global_fks(global_fks)

	def add_custom_fk_to_config(self, table_1_name, table_2_name, column_1_name, column_2_name):
		if self.setup_changes_allowed():
			self.config.add_custom_fk(table_1_name, table_2_name, column_1_name, column_2_name)

	def add_custom_column_name_to_config(self, table_name, column_name, new_column_name):
		if self.setup_changes_allowed():
			self.config.add_custom_column_name(table_name, column_name, new_column_name)

	def get_joined_df_options_from_paths(self, paths):
		# given a list of paths, get all dfs that could arise
		df_choices = []
		for path in paths:
			df = self.tables[path[0]].df  # paths returns a list of tables. Initialize with the first table obj in the path
			previous_table = path[0]
			added_tables = [previous_table]
			if len(path) > 1:
				for next_table in path[1:]:
					col_idx_joining = self.struct.find_joining_col_idx(previous_table, next_table)
					# ensure that next_table was not already added in case the path backtracks, otherwise skip over
					if next_table not in added_tables:
						left_column = self.struct.column_links[col_idx_joining][previous_table]
						left_column = f'{left_column}_[{previous_table}]'
						right_column = self.struct.column_links[col_idx_joining][next_table]
						right_column = f'{right_column}_[{next_table}]'

						df = pd.merge(df, self.tables[next_table].df, left_on=left_column, right_on=right_column)
						added_tables.append(next_table)
					previous_table = next_table
			df_choices.append(df)
		return df_choices

	def get_joined_df_options(self, table_1, table_2):
		# return all dfs that arise from each possible path between two tables
		possible_paths = self.struct.find_paths_between_tables(start_table_name=table_1, destination_table_name=table_2)

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

		possible_paths = self.struct.find_paths_between_tables(start_table_name=table_1, destination_table_name=table_2)

		return self.get_biggest_joined_df_option_from_paths(possible_paths)

class Table():
	def __init__(self, name, df, add_suffix=False):
		self.name = name
		self.df = df
		
		if add_suffix:
			self.add_suffix()

	def add_suffix(self):
		self.df = self.df.add_suffix(f'_[{self.name}]')

	def __repr__(self):
		return self.name

class Metadata():
	def __init__(self):
		self.table_structure = {}

	def init_from_directory(self, directory_path, data_file_extension='.csv', delimiter=','):
		file_names = u.find_file_types(directory_path, data_file_extension)

		for file_name in file_names:
			idx = file_name.rfind('.')
			table_name = file_name[:idx]
			df = pd.read_csv(os.path.join(directory_path, file_name), delimiter=delimiter)
			column_types = []
			for column in df.columns:
				if len(df[column].dropna()) > len(df[column].dropna().unique()):
					table_type = TABLE_TYPE_MANY
				else:
					table_type = TABLE_TYPE_ONE
				column_types.append([column, table_type])
			self.table_structure[table_name] = column_types
		logging.debug(f'Metadata - table_structure: {self.table_structure}')

		self.common_column_names = self.get_common_column_names()
		self.table_names = self.get_table_names()

	def load_data(self, data):
		self.table_structure = data['table_structure']
		self.common_column_names = data['common_column_names']
		self.table_names = self.get_table_names()

	def dump_data(self):
		return_dict = {
			'table_structure': self.table_structure,
			'common_column_names': self.common_column_names
		}
		return return_dict

	def get_table_names(self):
		return sorted(list(self.table_structure.keys()))

	def get_table_columns(self):
		return_dict = {}
		for table, column_types in self.table_structure.items():
			column_list = []
			for column_type in column_types:
				column_list.append(column_type[0])
			return_dict[table] = column_list
		return return_dict

	def table_has_col(self, table_name, column_name):
		found = next((x for x in self.table_structure[table_name] if x[0] == column_name), False)
		if found is False:
			return False
		return True

	def get_common_column_names(self):
		column_counts = defaultdict(int)
		for column_list in self.table_structure.values():
			for column in column_list:
				column_counts[column[0]] += 1
		common_columns = [k for k, v in column_counts.items() if v > 1]
		return common_columns

	def get_table_type_for_column(self, table_name, column_name):
		column_types = self.table_structure[table_name]
		return next(x[1] for x in column_types if x[0] == column_name)

class Config():
	def __init__(self, metadata):
		self.metadata = metadata
		self.global_fks = []
		self.custom_fks = []
		self.exclude_columns = []
		self.custom_column_names = defaultdict(dict)
		
	def load_data(self, data):
		self.global_fks = data['global_fks']
		self.custom_fks = data['custom_fks']
		self.exclude_columns = data['exclude_columns']
		self.custom_column_names = data['custom_column_names']

	def dump_data(self):
		return_dict = {
			'global_fks': self.global_fks,
			'custom_fks': self.custom_fks,
			'exclude_columns': self.exclude_columns,
			'custom_column_names': self.custom_column_names
		}
		return return_dict

	def add_global_fks(self, global_fks):
		for global_fk in global_fks:
			self.add_global_fk(global_fk)

	def add_global_fk(self, global_fk):
		# ensure global_fk is in common_column_names
		if global_fk not in self.metadata.common_column_names:
			logging.error(f'{global_fk} is not in common column names: {self.metadata.common_column_names}')
		else:
			self.global_fks.append(global_fk)
		
	def add_exclude_column(self, exclude_column):
		self.exclude_columns.append(exclude_column)

	def add_custom_column_name(self, table_name, column_name, new_column_name):
		self.custom_column_names[table_name][column_name] = new_column_name

	def add_custom_fk(self, table_1_name, table_2_name, column_1_name, column_2_name):
		if table_1_name not in self.metadata.table_names:
			logging.error(f'Table {table_1_name} not found')
			raise ValueError
		if table_2_name not in self.metadata.table_names:
			logging.error(f'Table {table_2_name} not found')
			raise ValueError
		if self.metadata.table_has_col(table_1_name, column_1_name) is False:
			logging.error(f'Column {column_1_name} not found in table {table_1_name}')
			raise ValueError
		if self.metadata.table_has_col(table_2_name, column_2_name) is False:
			logging.error(f'Column {column_2_name} not found in table {table_2_name}')
			raise ValueError

		# Now ensure that this is not creating a many:many relationship
		table_1_type = self.metadata.get_table_type_for_column(table_1_name, column_1_name)
		table_2_type = self.metadata.get_table_type_for_column(table_2_name, column_2_name)

		if table_1_type == TABLE_TYPE_MANY and table_2_type == TABLE_TYPE_MANY:
			logging.error(f'Cannot link {table_1_name}->{column_1_name} and {table_2_name}->{column_2_name} because they have a many-to-many relationship')
			raise ValueError

		add_dict = {'table_1_name': table_1_name, 'table_2_name': table_2_name, 'column_1_name': column_1_name, 'column_2_name': column_2_name}
		
		self.custom_fks.append(add_dict)

class Struct():
	def __init__(self, metadata, config):
		self.metadata = metadata
		self.config = config
		self.column_factory = ColumnFactory(metadata=self.metadata, struct=self)
		self.column_links = defaultdict(dict)
		self.custom_column_names = {}
		self.table_relationships = defaultdict(dict)
		for table in self.metadata.table_names:
			self.table_relationships[table] = {
				'parents': {},
				'children': {},
				'siblings': {}
			}
		
		logging.debug(f'Linking {self.config.global_fks} global_fks')
		self.link_global_fks(self.config.global_fks)

		for custom_fk in self.config.custom_fks:
			logging.debug(f'Linking custom_fk {custom_fk}')
			self.link_custom_fk(custom_fk)

		# Now do the rest of the columns that have not been linked by global_fks or by custom_fks
		table_columns = self.metadata.get_table_columns()
		for table_name, column_list in table_columns.items():
			for column_name in column_list:
				idx = self.find_col_idx_by_table(table_name, column_name)
				if idx is None:
					repetitive = column_name in self.metadata.common_column_names
					new_column = self.column_factory.create_column(shared=False, repetitive=repetitive)
					self.column_links[new_column.id][table_name] = column_name

		for table_name, rename_dict in self.config.custom_column_names.items():
			for column_name, new_column_name in rename_dict.items():
				logging.debug(f'Renaming {table_name}->{column_name} to {new_column_name}')
				self.link_custom_column_name(table_name, column_name, new_column_name)

		for col_idx, table_links in self.column_links.items():
			self.create_fk_relationship(col_idx, table_links)
		
		self.column_factory.assign_display_names()

	def link_global_fks(self, global_fks):
		# option to supply a list of global fks instead of calling the function over and over
		for global_fk in global_fks:
			self.link_global_fk(global_fk)

	def link_global_fk(self, global_fk):
		new_column = self.column_factory.create_column(shared=True, repetitive=True)
		self.column_links[new_column.id] = {}

		for table_name in self.metadata.table_names:
			if self.metadata.table_has_col(table_name, global_fk):
				self.column_links[new_column.id][table_name] = global_fk

	def link_custom_fk(self, custom_fk):
		table_1_name = custom_fk['table_1_name']
		table_2_name = custom_fk['table_2_name']
		column_1_name = custom_fk['column_1_name']
		column_2_name = custom_fk['column_2_name']
		
		# First ensure that neither of the columns is already linked to a column. If it is, then use that dictionary to add them instead of creating a new column
		logging.debug(f'{table_1_name}.{column_1_name} --> {table_2_name}.{column_2_name}')
		idx = self.find_col_idx_by_table(table_1_name, column_1_name)
		if idx is None:
			idx = self.find_col_idx_by_table(table_2_name, column_2_name)
		
		if idx is None:
			repetitive = column_1_name in self.metadata.common_column_names or column_2_name in self.metadata.common_column_names
			col_obj = self.column_factory.create_column(shared=True, repetitive=repetitive)
			idx = col_obj.id
		else:
			logging.info(f'Linking to column {idx}')
		
		self.column_links[idx][table_1_name] = column_1_name
		self.column_links[idx][table_2_name] = column_2_name

	def link_custom_column_name(self, table_name, column_name, new_column_name):
		idx = self.find_col_idx_by_table(table_name, column_name)
		if idx is None:
			logging.error(f'Could not find {table_name}->{column_name} to rename')
			return
		
		existing_column_name = self.custom_column_names.get(idx, None)
		if existing_column_name is not None:
			logging.warning(f'Changing custom name from {existing_column_name} to {new_column_name}')
		self.custom_column_names[idx] = new_column_name

	def create_fk_relationship(self, col_idx, table_links, suppress_warning=False):
		table_combinations = itertools.combinations(table_links, 2)
		for table_combination in table_combinations:
			table_1_name = table_combination[0]
			table_2_name = table_combination[1]

			column_1_name = table_links[table_1_name]
			column_2_name = table_links[table_2_name]

			table_1_type = self.metadata.get_table_type_for_column(table_1_name, column_1_name)
			table_2_type = self.metadata.get_table_type_for_column(table_2_name, column_2_name)
			# four options: many:many, many:one, one:many, or one:one.

			if self.table_relationship_exists(table_1_name, table_2_name):
				logging.info(f'Relation already exists between {table_1_name} and {table_2_name}. Cannot assign two foreign keys between two tables.')  # serves as a safety check
			else:
				if table_1_type == TABLE_TYPE_MANY:
					if table_2_type == TABLE_TYPE_MANY:
						if suppress_warning is False:
							logging.warning(f'Cannot make relationship between {table_1_name}->{column_1_name} and {table_2_name}->{column_2_name} because they have a many-to-many relationship')
					elif table_2_type == TABLE_TYPE_ONE:
						self.add_parent_child_link(col_idx, parent_table_name=table_1_name, child_table_name=table_2_name)
				elif table_1_type == TABLE_TYPE_ONE:
					if table_2_type == TABLE_TYPE_MANY:
						self.add_parent_child_link(col_idx, parent_table_name=table_2_name, child_table_name=table_1_name)
					elif table_2_type == TABLE_TYPE_ONE:
						self.add_sibling_link(col_idx, sibling_1_table_name=table_1_name, sibling_2_table_name=table_2_name)

	def table_relationship_exists(self, table_1_name, table_2_name):
		if table_2_name in self.table_all_related_tables(table_1_name):
			return True
		if table_1_name in self.table_all_related_tables(table_2_name):
			return True
		return False

	def table_all_related_tables(self, table_name):
		# Return parents, children, and siblings of given table_name
		tr = self.table_relationships[table_name]
		return sorted(list(set(list(tr['parents'].keys()) + list(tr['children'].keys()) + list(tr['siblings'].keys()))))

	def table_all_connectable_tables(self, table_name):
		# Return children and siblings, e.g. tables that I can go to next from this table
		tr = self.table_relationships[table_name]
		return sorted(list(set(list(tr['children'].keys()) + list(tr['siblings'].keys()))))

	def table_children(self, table_name):
		tr = self.table_relationships[table_name]
		return sorted(list(tr['children'].keys()))

	def table_siblings(self, table_name):
		tr = self.table_relationships[table_name]
		return sorted(list(tr['siblings'].keys()))

	def table_parents(self, table_name):
		tr = self.table_relationships[table_name]
		return sorted(list(tr['parents'].keys()))

	def col_idx_tables(self, idx):
		return sorted(list(self.column_links[idx].keys()))

	def find_joining_col_idx(self, table_1_name, table_2_name):
		col_idx_1 = self.table_col_idxs(table_1_name)
		col_idx_2 = self.table_col_idxs(table_2_name)
		return set(col_idx_1).intersection(col_idx_2).pop()

	def table_col_idxs(self, table_name):
		return_list = []
		for idx, data in self.column_links.items():
			if table_name in data.keys():
				return_list.append(idx)
		return return_list

	def find_col_idx_by_table(self, table_name, column_name):
		# Check to see if this table_name->column_name pair is already associated with an existing column
		for idx, table_links in self.column_links.items():
			for table_name_existing, column_name_existing in table_links.items():
				if table_name_existing == table_name and column_name_existing == column_name:
					return idx
		return None

	def add_parent_child_link(self, col_idx, parent_table_name, child_table_name):
		self.table_relationships[parent_table_name]['children'][child_table_name] = col_idx
		self.table_relationships[child_table_name]['parents'][parent_table_name] = col_idx

	def add_sibling_link(self, col_idx, sibling_1_table_name, sibling_2_table_name):
		self.table_relationships[sibling_1_table_name]['siblings'][sibling_2_table_name] = col_idx
		self.table_relationships[sibling_2_table_name]['siblings'][sibling_1_table_name] = col_idx

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
				if len(self.find_paths_between_tables(start_table_name=pair[0], destination_table_name=pair[1])) == 0:
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
				path_possibilities_pairwise.append(self.find_paths_between_tables(start_table_name=pair[0], destination_table_name=pair[1]))
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
			table_list = [self.col_idx_tables(x) for x in column_permutation]
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

	def find_paths_between_tables(self, start_table_name, destination_table_name, current_path=[]):
		if start_table_name == destination_table_name:
			return [start_table_name]

		all_paths = []
		
		current_path = current_path.copy() + [start_table_name]
		if (destination_table_name in self.table_all_connectable_tables(start_table_name)):  # immediately return valid path rather than going through children/siblings
			all_paths.append(current_path.copy() + [destination_table_name])
			return all_paths
			
		elif len(self.table_children(start_table_name)) == 0 and len(self.table_siblings(start_table_name)) == 0:  # destination table wasn't found and this path has nowhere else to go
			return []

		elif destination_table_name in self.table_parents(start_table_name):
			# if found in parents, then if parent is not a sibling of any of this tables siblings, then there's no path to get up to that parent
			# if it is a sibling of a sibling, then return that sibling + destination table appended to current_path
			found_sibling_of_sibling = False
			start_table_siblings = self.table_siblings(start_table_name)
			for start_table_sibling in start_table_siblings:
				sibling_siblings = self.table_siblings(start_table_sibling)
				if destination_table_name in sibling_siblings:
					found_sibling_of_sibling = True
					all_paths.append(current_path.copy() + [start_table_sibling] + [destination_table_name])
			if found_sibling_of_sibling:
				return all_paths
			else:
				return []

		for child_table_name in self.table_children(start_table_name):
			for path in self.find_paths_between_tables(start_table_name=child_table_name, destination_table_name=destination_table_name, current_path=current_path):
				all_paths.append(path)
		
		for sibling_table_name in self.table_siblings(start_table_name):
			if sibling_table_name not in current_path:  # prevents just looping across siblings forever
				for path in self.find_paths_between_tables(start_table_name=sibling_table_name, destination_table_name=destination_table_name, current_path=current_path):
					all_paths.append(path)

		return all_paths
	def get_still_accessible_tables(self, include_tables, fix_first=False):
		# Given a list of include_tables that must be in a valid path (not necessarily in order), iterate through the rest of the tables to figure out if there are paths between include_tables and each of those
		possible_tables = [x for x in self.metadata.table_names if x not in include_tables]

		accessible_tables = []
		for possible_table in possible_tables:
			check_tables = include_tables + [possible_table]
			if len(self.find_paths_multi_tables(check_tables, fix_first=fix_first)) > 0:
				accessible_tables.append(possible_table)
		return accessible_tables

	def get_still_accessible_columns(self, include_column_idxs):
		# Given a list of include_columns that must be in a valid path (not necessarily in order, but the function will preserve order), iterate through the rest of the columns to figure out if there are paths between include_columns and each of those
		# One way to do this is to get all possible combinations of the tables that each include_column is in, figure out which tables are accessible, and then add those tables columns in
		
		accessible_columns = set()
		include_table_options = [self.col_idx_tables(x) for x in include_column_idxs]
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
			for idx in self.table_col_idxs(table):
				accessible_columns.add(idx)
		
		remaining_tables = [x for x in self.metadata.table_names if x not in accessible_tables]

		# potential paths represents all complete paths that can traverse between the columns in include_columns. To check if the other tables are accessible, see if there's a path between the last member in each path and the remaining table
		for remaining_table in remaining_tables:
			found_valid_path = False
			for path in all_paths:
				if not found_valid_path:
					if len(self.find_paths_between_tables(path[-1], remaining_table)) > 0:
						found_valid_path = True
						accessible_tables.add(remaining_table)
						for idx in self.table_col_idxs(remaining_table):
							accessible_columns.add(idx)

		return list(accessible_columns)

class ColumnFactory():
	def __init__(self, metadata, struct):
		self.counter = 0
		self.metadata = metadata
		self.struct = struct
		self.columns = {}

	def create_column(self, **kwargs):
		new_column = Column(id=self.counter, **kwargs)
		self.columns[self.counter] = new_column
		self.counter += 1
		return new_column

	def assign_display_names(self):
		# Must run only after struct has been generated
		for idx, col_obj in self.columns.items():
			custom_name = self.struct.custom_column_names.get(idx, None)
			if custom_name is not None:
				col_obj.display_name = custom_name
			else:			
				column_tables = self.struct.col_idx_tables(idx)
				first_table = column_tables[0]
				first_table_column_name = self.struct.column_links[idx][first_table]
				if col_obj.shared:
					col_obj.display_name = first_table_column_name
				elif col_obj.repetitive:
					col_obj.display_name = f'{first_table_column_name}_[{first_table}]'
				else:
					col_obj.display_name = first_table_column_name
		
class Column():
	def __init__(self, id, shared=False, repetitive=False):
		self.id = id
		self.shared = shared
		self.repetitive = repetitive
		self.display_name = None

	def __repr__(self):
		return f'Column {self.id}'

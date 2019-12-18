import itertools
import json
import logging
import os
import pandas as pd
import constants as c
import utilities as u

from collections import defaultdict
from decimal import Decimal as D
from sqlalchemy import create_engine
from pandas.api.types import is_numeric_dtype


class DBMaker():
    '''
    This class will take the files in the directory and then create a SQL database
    '''

    def __init__(self, directory_path, data_file_extension='.csv', delimiter=','):
        self.directory_path = directory_path
        self.abs_path = os.path.join(os.getcwd(), directory_path)
        self.data_file_extension = data_file_extension
        self.delimiter = delimiter

    def create_db(self, db_name=None, overwrite=False):
        db_files = u.find_file_types(self.directory_path, '.db')
        metadata_files = u.find_file_types(self.directory_path, '.metadata')

        if len(db_files) + len(metadata_files) > 0:
            if overwrite:
                self.purge_dbs()
            else:
                raise Exception('Cannot proceed, there are existing .db or .metadata files in the directory. If you want to overwrite it, pass in "overwrite=True"')

        if db_name is None:
            db_name = os.path.split(self.abs_path)[1]

        engine = create_engine('sqlite:///' + os.path.join(self.abs_path, f'{db_name}.db'), echo=False)

        metadata = defaultdict(dict)
        data_file_names = u.find_file_types(self.directory_path, self.data_file_extension)
        for data_file_name in data_file_names:
            idx = data_file_name.rfind('.')
            table_name = data_file_name[:idx]
            metadata[table_name]['file'] = data_file_name
            metadata[table_name]['columns'] = []

            logging.info(f'Writing {table_name} to db')
            df = pd.read_csv(os.path.join(self.abs_path, data_file_name))
            for column in df.columns:
                if len(df[column].dropna()) > len(df[column].dropna().unique()):
                    column_type = c.COLUMN_MANY
                else:
                    column_type = c.COLUMN_ONE
                metadata[table_name]['columns'].append({'name': column, 'type': column_type})
            
            df.to_sql(table_name, con=engine)
        
        logging.info(f'Finished writing {db_name}.db')
        
        logging.info(f'Dumping metadata to {db_name}.metadata')
        metadata_path = os.path.join(self.abs_path, f'{db_name}.metadata')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)

    def purge_dbs(self):
        db_files = u.find_file_types(self.directory_path, '.db')
        metadata_files = u.find_file_types(self.directory_path, '.metadata')

        for x in db_files + metadata_files:
            logging.warning(f'Removing {x}')
            os.remove(os.path.join(self.directory_path, x))


class DBLinker():
    def __init__(self, directory_path):
        # Create global FKs, custom FKs. Write to .links file
        self.directory_path = directory_path
        self.abs_path = os.path.join(os.getcwd(), directory_path)
        self.finalized = False
        self.load_links()
        self.load_metadata()
        if self.table_relationships is None:
            self.table_relationships = {}
            for table in self.table_names:
                self.table_relationships[table] = {
                    'parents': {},          # MANY of Many-to-one relationships
                    'children': {},         # ONE of Many-to-one relationships
                    'siblings': {},         # One-to-one relationships
                    'step_siblings': {}     # Many-to-many relationships
                }
        else:
            self.finalized = True

    @property
    def table_names(self):
        return [k for k in self.metadata.keys()]

    def load_links(self):
        links_files = u.find_file_types(self.abs_path, '.links')
        if len(links_files) > 1:
            raise Exception('Cannot proceed, there are multiple .links files in the directory. Delete all but one to continue')
        elif len(links_files) == 0:
            self.table_relationships = None
        else:
            links_file = links_files[0]
            with open(os.path.join(self.abs_path, links_file), 'r') as f:
                self.table_relationships = json.load(f)

    def load_metadata(self):
        metadata_files = u.find_file_types(self.abs_path, '.metadata')
        if len(metadata_files) > 1:
            raise Exception('Cannot proceed, there are multiple .metadata files in the directory. Delete all but one to continue')
        elif len(metadata_files) == 0:
            raise Exception(f'No .metadata files found in {self.abs_path}')
        else:
            metadata_file = metadata_files[0]
            self.db_name = metadata_file.split('.metadata')[0]
            with open(os.path.join(self.abs_path, metadata_file), 'r') as f:
                self.metadata = json.load(f)

    def get_column_type(self, table, column):
        return next(x['type'] for x in self.metadata[table]['columns'] if x['name'] == column)

    def get_table_columns(self, table):
        return [x['name'] for x in self.metadata[table]['columns']]

    def table_relationship_exists(self, table_1, table_2):
        if table_2 in self.find_table_all_related_tables(table_1):
            return True
        if table_1 in self.find_table_all_related_tables(table_2):
            return True
        return False

    def find_table_all_related_tables(self, table):
        # Return parents, children, siblings, and step-siblings of given table
        tr = self.table_relationships[table]

        return sorted(list(set(list(tr['parents'].keys()) + list(tr['children'].keys()) + list(tr['siblings'].keys()) + list(tr['step_siblings'].keys()))))

    def add_global_fk(self, column):
        # Find all tables that have this column name, then run add_fk to all combos
        
        if self.finalized:
            raise Exception('Linking has been finalized. Delete the .metadata file if you want to re-do it')
        tables_found = []
        for table in self.table_names:
            if column in self.get_table_columns(table):
                tables_found.append(table)
        
        for table_combination in itertools.combinations(tables_found, 2):
            self.add_fk(table_combination[0], column, table_combination[1], column)

    def add_fk(self, table_1, column_1, table_2, column_2):
        if self.finalized:
            raise Exception('Linking has been finalized. Delete the .metadata file if you want to re-do it')
        try:
            column_1_type = self.get_column_type(table_1, column_1)
            column_2_type = self.get_column_type(table_2, column_2)
        except KeyError as e:
            logging.error(e)
            raise(e)
        except StopIteration:
            logging.error(f'Unable to find one of {column_1} or {column_2}')
            raise(StopIteration)
        
        if self.table_relationship_exists(table_1, table_2):
            logging.info(f'Relationship already exists between {table_1} and {table_2}. Cannot assign two foreign keys between two tables.')  # serves as a safety check. Will prioritize global_fks then custom_fks by nature of the order in which they are called in generate_links()
        else:
            if column_1_type == c.COLUMN_MANY:
                if column_2_type == c.COLUMN_MANY:
                    self.add_step_sibling_link(step_sibling_1_table=table_1, step_sibling_1_column=column_1, step_sibling_2_table=table_2, step_sibling_2_column=column_2)
                
                elif column_2_type == c.COLUMN_ONE:
                    self.add_parent_child_link(parent_table=table_1, parent_column=column_1, child_table=table_2, child_column=column_2)

            elif column_1_type == c.COLUMN_ONE:
                if column_2_type == c.COLUMN_MANY:
                    self.add_parent_child_link(parent_table=table_2, parent_column=column_2, child_table=table_1, child_column=column_1)
                
                elif column_2_type == c.COLUMN_ONE:
                    self.add_sibling_link(sibling_1_table=table_1, sibling_1_column=column_1, sibling_2_table=table_2, sibling_2_column=column_2)

    def add_parent_child_link(self, parent_table, parent_column, child_table, child_column):
        self.table_relationships[parent_table]['children'][child_table] = [parent_column, child_column]
        self.table_relationships[child_table]['parents'][parent_table] = [child_column, parent_column]

    def add_sibling_link(self, sibling_1_table, sibling_1_column, sibling_2_table, sibling_2_column):
        self.table_relationships[sibling_1_table]['siblings'][sibling_2_table] = [sibling_1_column, sibling_2_column]
        self.table_relationships[sibling_2_table]['siblings'][sibling_1_table] = [sibling_2_column, sibling_1_column]

    def add_step_sibling_link(self, step_sibling_1_table, step_sibling_1_column, step_sibling_2_table, step_sibling_2_column):
        self.table_relationships[step_sibling_1_table]['step_siblings'][step_sibling_2_table] = [step_sibling_1_column, step_sibling_2_column]
        self.table_relationships[step_sibling_2_table]['step_siblings'][step_sibling_1_table] = [step_sibling_2_column, step_sibling_1_column]

    def finalize(self):
        links_path = os.path.join(self.abs_path, f'{self.db_name}.links')
        with open(links_path, 'w') as f:
            json.dump(self.table_relationships, f, indent=4)
        self.finalized = True


class DBCustomizer():
    def __init__(self, directory_path):
        # User-defined custom column names, etc
        self.directory_path = directory_path
        self.abs_path = os.path.join(os.getcwd(), directory_path)
        self.load_metadata()
        self.load_links()

    def load_metadata(self):
        metadata_files = u.find_file_types(self.abs_path, '.metadata')
        if len(metadata_files) > 1:
            raise Exception('Cannot proceed, there are multiple .metadata files in the directory. Delete all but one to continue')
        elif len(metadata_files) == 0:
            raise Exception(f'No .metadata files found in {self.abs_path}')
        else:
            metadata_file = metadata_files[0]
            self.db_name = metadata_file.split('.metadata')[0]
            with open(os.path.join(self.abs_path, metadata_file), 'r') as f:
                self.metadata = json.load(f)

    def load_links(self):
        links_files = u.find_file_types(self.abs_path, '.links')
        if len(links_files) > 1:
            raise Exception('Cannot proceed, there are multiple .links files in the directory. Delete all but one to continue')
        elif len(links_files) == 0:
            raise Exception(f'No .links files found in {self.abs_path}')
        else:
            links_file = links_files[0]
            with open(os.path.join(self.abs_path, links_file), 'r') as f:
                self.table_relationships = json.load(f)


class DBExtractor():
    def __init__(self, directory_path):
        # path-finding, get data out
        self.directory_path = directory_path
        self.abs_path = os.path.join(os.getcwd(), directory_path)
        self.load_engine()
        self.load_metadata()
        self.load_links()

    def load_engine(self):
        db_files = u.find_file_types(self.abs_path, '.db')
        if len(db_files) > 1:
            raise Exception('Cannot proceed, there are multiple .db files in the directory. Delete all but one to continue')
        elif len(db_files) == 0:
            raise Exception(f'No .db files found in {self.abs_path}')
        else:
            db_file = db_files[0]
            self.engine = create_engine('sqlite:///' + os.path.join(self.abs_path, db_file))

    def load_metadata(self):
        metadata_files = u.find_file_types(self.abs_path, '.metadata')
        if len(metadata_files) > 1:
            raise Exception('Cannot proceed, there are multiple .metadata files in the directory. Delete all but one to continue')
        elif len(metadata_files) == 0:
            raise Exception(f'No .metadata files found in {self.abs_path}')
        else:
            metadata_file = metadata_files[0]
            self.db_name = metadata_file.split('.metadata')[0]
            with open(os.path.join(self.abs_path, metadata_file), 'r') as f:
                self.metadata = json.load(f)

    def load_links(self):
        links_files = u.find_file_types(self.abs_path, '.links')
        if len(links_files) > 1:
            raise Exception('Cannot proceed, there are multiple .links files in the directory. Delete all but one to continue')
        elif len(links_files) == 0:
            raise Exception(f'No .links files found in {self.abs_path}')
        else:
            links_file = links_files[0]
            with open(os.path.join(self.abs_path, links_file), 'r') as f:
                self.table_relationships = json.load(f)

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
            is_valid = True
            for pair in u.pairwise(permutation):
                if len(self.find_paths_between_tables(start_table=pair[0], destination_table=pair[1])) == 0:
                    is_valid = False
            if is_valid:
                valid_incomplete_paths.append(permutation)
        
        unflattened_valid_complete_paths = []
        for valid_incomplete_path in valid_incomplete_paths:
            path_possibilities_pairwise = []
            for pair in u.pairwise(valid_incomplete_path):
                path_possibilities_pairwise.append(self.find_paths_between_tables(start_table=pair[0], destination_table=pair[1]))
            # print(path_possibilities_pairwise)
            combos = itertools.product(*path_possibilities_pairwise)
            for combo in combos:
                unflattened_valid_complete_paths.append(list(combo))

        flattened_valid_complete_paths = []
        for l in unflattened_valid_complete_paths:
            flattened_valid_complete_paths.append(list(u.flatten(l)))
        
        flattened_valid_complete_paths = u.remove_adjacent_repeats(flattened_valid_complete_paths)
        
        return flattened_valid_complete_paths

    def get_joining_keys(self, table_1, table_2):
        # order matters here
        if table_2 in self.find_table_children(table_1):
            return self.table_relationships[table_1]['children'][table_2]
        if table_2 in self.find_table_siblings(table_1):
            return self.table_relationships[table_1]['siblings'][table_2]
        return None

    def get_df_from_path(self, path, table_columns_of_interest):
        sql_statement = f'SELECT '
        for column in table_columns_of_interest:
            sql_statement += column + ', '
        sql_statement = sql_statement[:-2]

        sql_statement += f' FROM {path[0]} '
        previous_table = path[0]
        for table in path[1:]:
            keys = self.get_joining_keys(previous_table, table)
            try:
                left_key, right_key = keys[0], keys[1]
            except TypeError:
                logging.error(f'Path {path} is invalid. Unable to join {previous_table} to {table}')
                raise(TypeError)
            sql_statement += f'JOIN {table} ON {previous_table}.{left_key} = {table}.{right_key} '
            previous_table = table
        logging.info(sql_statement)
        df = pd.read_sql(sql_statement, con=self.engine)
        return df

    def aggregate_df(self, df_original, groupby_columns, filters, aggregate_column=None, aggregate_fxn='Count'):
        logging.debug(f'Aggregate by {groupby_columns}')
        if aggregate_column is not None:
            logging.debug(f'Aggregate for {aggregate_column}')
        
        df = df_original.copy(deep=True)
        df = df.dropna()

        # Code to generate filter perumutations and do actual filtering
        filter_filters = []
        for column in groupby_columns:
            filter = filters.get(column, None)
            if filter is None:
                series = df.loc[:, column]
                if is_numeric_dtype(series):
                    min = u.reduce_precision(series.min(), 2)
                    max = u.reduce_precision(series.max(), 2)

                    label = f'({min}, {max})'
                    df[column] = label
                    filter_filters.append([label])
                else:
                    filter_filters.append(sorted(series.unique(), key=lambda x: x.upper()))
            elif filter['type'] == 'list':
                filter_filters.append(filter['filter'])
                df = df[df[column].isin(filter['filter'])]
            elif filter['type'] == 'range':
                bin_cuts = self.get_bin_cuts(filter['filter']['min'], filter['filter']['max'], filter['filter']['bins'])
                bin_labels = [str(x) for x in u.pairwise(bin_cuts)]
                bin_labels = [x.replace(')', ']') for x in bin_labels]
                df[column] = pd.cut(df[column], bin_cuts, include_lowest=True, labels=bin_labels).dropna()
                filter_filters.append(bin_labels)
        
        groupby_label_options = []
        for filter_combo in itertools.product(*filter_filters):
            label = ''
            for i in filter_combo:
                label += str(i) + '_'
            label = label[:-1]
            groupby_label_options.append(label)
        
        if len(df) > 0:
            if aggregate_column is None:
                # just get the counts then
                df = df.groupby(groupby_columns).size()
                if len(groupby_columns) > 1:
                    df = df.unstack(fill_value=0).sort_index(axis=1).stack()
                df = df.reset_index(name='Count')
            else:
                g = df.groupby(groupby_columns, observed=True)

                if aggregate_fxn == 'Count':
                    df = g[aggregate_column].value_counts().unstack(fill_value=0).sort_index(axis=1).reset_index()
                elif aggregate_fxn == 'Percents':
                    df = (g[aggregate_column].value_counts(normalize=True) * 100).round(1).unstack(fill_value=0).sort_index(axis=1).reset_index()
                elif aggregate_fxn == 'Sum':
                    df = g.sum().reset_index()
                    df[aggregate_column] = df[aggregate_column].fillna(0)
                elif aggregate_fxn == 'Mean':
                    df = (g.mean()).round(2).reset_index()
                    df[aggregate_column] = df[aggregate_column].fillna(0)
                elif aggregate_fxn == 'Median':
                    df = (g.median()).round(2).reset_index()
                    df[aggregate_column] = df[aggregate_column].fillna(0)

            def get_breakdown_label(row, ind_variables):
                return_str = ''
                for x in ind_variables:
                    return_str += str(row[x]) + '_'
                return_str = return_str[:-1]  # remove trailing underscore
                return return_str

            df['groupby_labels'] = df.apply(lambda x: get_breakdown_label(x, groupby_columns), axis=1)
        else:
            df['groupby_labels'] = None

        df = df.drop(columns=groupby_columns)
        
        # Some groupbys will have 0 patients, but I still want to display 0
        found_labels = list(df['groupby_labels'].value_counts().index)
        missing_labels = [x for x in groupby_label_options if x not in found_labels]
        if len(missing_labels) > 0:
            for missing_label in missing_labels:
                df = df.append({'groupby_labels': missing_label}, ignore_index=True)
            df = df.fillna(0)

        def find_sort_order(row):
            return groupby_label_options.index(row['groupby_labels'])

        df['sort_order'] = df.apply(lambda x: find_sort_order(x), axis=1)
        df = df.sort_values(by='sort_order')
        df = df.drop(columns=['sort_order'])
        
        return df

    def get_bin_cuts(self, min, max, num_bins):
        min = D(min)
        max = D(max)
        num_bins = D(num_bins)
        
        step_size = (max - min) / num_bins
        current_cut = min
        bin_cuts = []
        while len(bin_cuts) < num_bins:
            current_cut = u.reduce_precision(current_cut, precision=2)
            bin_cuts.append(float(current_cut))
            current_cut += step_size
        bin_cuts.append(float(max))  # added in case there are rounding errors, and planning for choosing inclusive right-non-inclusive intervals

        return bin_cuts

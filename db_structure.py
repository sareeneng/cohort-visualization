import itertools
import logging
import os
import pandas as pd
import sqlite3
import constants as c
import utilities as u

from decimal import Decimal as D
from pandas.api.types import is_numeric_dtype
from sqlalchemy.exc import OperationalError
from web import db, flask_app
from web.models import DatasetMetadata, TableMetadata, ColumnMetadata, TableRelationship


class DBMaker():
    '''
    This class will take the files in the directory and then create tables in the main application db. It will also add metadata
    '''

    def __init__(self, dataset_name, directory_path, data_file_extension='.csv', delimiter=','):
        self.directory_path = directory_path
        self.abs_path = os.path.join(os.getcwd(), directory_path)
        self.dataset_name = dataset_name
        self.data_file_extension = data_file_extension
        self.data_conn = sqlite3.connect(flask_app.config['DATA_DB'])

    def create_db(self, overwrite=False):
        # First check to see if either dataset_name or the folder are already in the db
        x = db.session.query(DatasetMetadata).filter(DatasetMetadata.dataset_name == self.dataset_name).first()
        if x is not None:
            e = f'{self.dataset_name} is already in the db'
            logging.error(e)
            raise Exception(e)

        x = db.session.query(DatasetMetadata).filter(DatasetMetadata.folder == self.directory_path).first()
        if x is not None:
            e = f'{self.directory_path} is already in the db'
            logging.error(e)
            raise Exception(e)

        prefix = self.dataset_name

        dataset_metadata = DatasetMetadata(
            dataset_name=self.dataset_name,
            folder=self.directory_path,
            prefix=prefix
        )
        db.session.add(dataset_metadata)
        
        data_file_names = u.find_file_types(self.directory_path, self.data_file_extension)

        for data_file_name in data_file_names:
            idx = data_file_name.rfind('.')
            table_name = data_file_name[:idx]
            db_location = f'{prefix}_{table_name}'

            logging.info(f'Writing {table_name} to {db_location}')
            
            df = pd.read_csv(os.path.join(self.abs_path, data_file_name))
            df.to_sql(db_location, con=self.data_conn)
            table_metadata = TableMetadata(
                dataset_name=self.dataset_name,
                table_name=table_name,
                db_location=db_location,
                file=data_file_name
            )

            db.session.add(table_metadata)

            for column in df.columns:
                if len(df[column].dropna()) > len(df[column].dropna().unique()):
                    is_many = True
                else:
                    is_many = False

                column_metadata = ColumnMetadata(
                    dataset_name=self.dataset_name,
                    table_name=table_name,
                    column_source_name=column,
                    column_custom_name=column,
                    is_many=is_many
                )
                db.session.add(column_metadata)
        
        db.session.commit()
        logging.info(f'Finished writing {self.dataset_name}')

    def remove_db(self):
        table_metadata = db.session.query(TableMetadata).filter(TableMetadata.dataset_name == self.dataset_name).all()
        for table in table_metadata:
            sql_statement = f'DROP TABLE {table.db_location};'
            try:
                self.data_conn.cursor().execute(sql_statement)
            except OperationalError:
                logging.error(f'Unable to drop {table.db_location}. Does it exist in the db?')
        
        db.session.query(TableMetadata).filter(TableMetadata.dataset_name == self.dataset_name).delete()

        db.session.query(ColumnMetadata).filter(ColumnMetadata.dataset_name == self.dataset_name).delete()
        
        db.session.query(DatasetMetadata).filter(DatasetMetadata.dataset_name == self.dataset_name).delete()

        db.session.query(TableRelationship).filter(TableRelationship.dataset_name == self.dataset_name).delete()
        
        db.session.commit()


class DBLinker():
    def __init__(self, dataset_name):
        # Create global FKs, custom FKs. Write to .links file
        self.dataset_name = dataset_name

    def column_type_is_many(self, table, column):
        found_row = db.session.query(ColumnMetadata).filter(ColumnMetadata.dataset_name == self.dataset_name, ColumnMetadata.table_name == table, ColumnMetadata.column_source_name == column).first()
        try:
            return found_row.is_many
        except AttributeError:
            logging.error(f'Unable to find column type for {table}.{column}')
            return None

    def table_relationship_exists(self, table_1, table_2):
        if db.session.query(TableRelationship).filter(TableRelationship.dataset_name == self.dataset_name, ((TableRelationship.reference_table == table_1) & (TableRelationship.other_table == table_2)) | ((TableRelationship.reference_table == table_2) & (TableRelationship.other_table == table_1))).first() is None:
            return False
        return True

    def add_global_fk(self, column):
        # Find all tables that have this column name, then run add_fk to all combos
        tables_found = [x[0] for x in db.session.query(ColumnMetadata.table_name).filter(ColumnMetadata.dataset_name == self.dataset_name, ColumnMetadata.column_source_name == column).all()]
        
        for table_combination in itertools.combinations(tables_found, 2):
            self.add_fk(table_combination[0], column, table_combination[1], column)

    def add_fk(self, table_1, column_1, table_2, column_2):
        column_1_is_many = self.column_type_is_many(table_1, column_1)
        column_2_is_many = self.column_type_is_many(table_2, column_2)
        
        if self.table_relationship_exists(table_1, table_2):
            logging.info(f'Relationship already exists between {table_1} and {table_2}. Cannot assign two foreign keys between two tables.')  # serves as a safety check.
        else:
            if column_1_is_many:
                if column_2_is_many:
                    self.add_step_sibling_link(step_sibling_1_table=table_1, step_sibling_1_column=column_1, step_sibling_2_table=table_2, step_sibling_2_column=column_2)
                
                elif not column_2_is_many:
                    self.add_parent_child_link(parent_table=table_1, parent_column=column_1, child_table=table_2, child_column=column_2)

            elif not column_1_is_many:
                if column_2_is_many:
                    self.add_parent_child_link(parent_table=table_2, parent_column=column_2, child_table=table_1, child_column=column_1)
                
                elif not column_2_is_many:
                    self.add_sibling_link(sibling_1_table=table_1, sibling_1_column=column_1, sibling_2_table=table_2, sibling_2_column=column_2)
        
        db.session.commit()

    def add_parent_child_link(self, parent_table, parent_column, child_table, child_column, commit=False):
        parent_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=parent_table,
            other_table=child_table,
            reference_key=parent_column,
            other_key=child_column,
            is_parent=True
        )

        child_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=child_table,
            other_table=parent_table,
            reference_key=child_column,
            other_key=parent_column,
            is_child=True
        )
        
        db.session.add(parent_row)
        db.session.add(child_row)
        if commit:
            db.session.commit()

    def add_sibling_link(self, sibling_1_table, sibling_1_column, sibling_2_table, sibling_2_column, commit=False):
        sibling_1_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=sibling_1_table,
            other_table=sibling_2_table,
            reference_key=sibling_1_column,
            other_key=sibling_2_column,
            is_sibling=True
        )
        sibling_2_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=sibling_2_table,
            other_table=sibling_1_table,
            reference_key=sibling_2_column,
            other_key=sibling_1_column,
            is_sibling=True
        )

        db.session.add(sibling_1_row)
        db.session.add(sibling_2_row)
        if commit:
            db.session.commit()

    def add_step_sibling_link(self, step_sibling_1_table, step_sibling_1_column, step_sibling_2_table, step_sibling_2_column, commit=False):
        step_sibling_1_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=step_sibling_1_table,
            other_table=step_sibling_2_table,
            reference_key=step_sibling_1_column,
            other_key=step_sibling_2_column,
            is_step_sibling=True
        )
        step_sibling_2_row = TableRelationship(
            dataset_name=self.dataset_name,
            reference_table=step_sibling_2_table,
            other_table=step_sibling_1_table,
            reference_key=step_sibling_2_column,
            other_key=step_sibling_1_column,
            is_step_sibling=True
        )

        db.session.add(step_sibling_1_row)
        db.session.add(step_sibling_2_row)

        if commit:
            db.session.commit()

    def remove_fk(self, table_1, table_2):
        pass

    def remove_global_fk(self, column):
        pass

    def remove_all_relationships(self):
        logging.warning(f'Will remove all links for dataset {self.dataset_name}')
        db.session.query(TableRelationship).filter(TableRelationship.dataset_name == self.dataset_name).delete()
        db.session.commit()


class DBCustomizer():
    def __init__(self, dataset_name):
        # User-defined custom column names, etc
        self.dataset_name = dataset_name

    def rename_column(self, reference_table, original_name, new_name):
        found_row = db.session.query(ColumnMetadata).filter(
            ColumnMetadata.dataset_name == self.dataset_name,
            ColumnMetadata.table_name == reference_table,
            ColumnMetadata.column_source_name == original_name
        ).first()
        try:
            found_row.column_custom_name = new_name
        except AttributeError:
            e = f'Cannot find {reference_table}->{original_name}'
            logging.error(e)
            raise AttributeError(e)

        # Then look for where this column serves as a relationship to other tables
        found_related_table_rows = db.session.query(TableRelationship).filter(
            TableRelationship.reference_table == reference_table,
            TableRelationship.reference_key == original_name
        ).all()
        
        for related_table_row in found_related_table_rows:
            related_table = related_table_row.other_table
            related_column = related_table_row.other_key
            found_related_row = db.session.query(ColumnMetadata).filter(
                ColumnMetadata.dataset_name == self.dataset_name,
                ColumnMetadata.table_name == related_table,
                ColumnMetadata.column_source_name == related_column
            ).first()
            try:
                found_related_row.column_custom_name = new_name
            except AttributeError:
                e = f'Cannot find {related_table}->{related_column}'
                logging.error(e)
                raise AttributeError(e)

        db.session.commit()

    def get_custom_column_name(self, reference_table, original_name):
        try:
            return db.session.query(ColumnMetadata).filter(
                ColumnMetadata.dataset_name == self.dataset_name,
                ColumnMetadata.table_name == reference_table,
                ColumnMetadata.column_source_name == original_name
            ).first()
        except AttributeError:
            e = f'Cannot find {reference_table}->{original_name}'
            logging.error(e)
            raise AttributeError(e)
    
    def change_column_visibility(self, visible=True):
        pass


class DBExtractor():
    def __init__(self, dataset_name):
        # path-finding, get data out
        self.dataset_name = dataset_name
        self.prefix = db.session.query(DatasetMetadata.prefix).filter(DatasetMetadata.dataset_name == self.dataset_name).first()[0]
        self.data_conn = sqlite3.connect(flask_app.config['DATA_DB'])
    
    def find_table_all_connectable_tables(self, table):
        # Return children and siblings, e.g. tables that I can go to next from this table

        return sorted([x[0] for x in db.session.query(TableRelationship.other_table).filter(
            TableRelationship.dataset_name == self.dataset_name,
            TableRelationship.reference_table == table,
            (TableRelationship.is_parent | TableRelationship.is_sibling)
        ).all()], key=lambda x: x.upper())

    def find_table_children(self, table):
        return sorted([x[0] for x in db.session.query(TableRelationship.other_table).filter(
            TableRelationship.dataset_name == self.dataset_name,
            TableRelationship.reference_table == table,
            TableRelationship.is_parent
        ).all()], key=lambda x: x.upper())

    def find_table_siblings(self, table):
        return sorted([x[0] for x in db.session.query(TableRelationship.other_table).filter(
            TableRelationship.dataset_name == self.dataset_name,
            TableRelationship.reference_table == table,
            TableRelationship.is_sibling
        ).all()], key=lambda x: x.upper())

    def find_table_parents(self, table):
        return sorted([x[0] for x in db.session.query(TableRelationship.other_table).filter(
            TableRelationship.dataset_name == self.dataset_name,
            TableRelationship.reference_table == table,
            TableRelationship.is_child
        ).all()], key=lambda x: x.upper())

    def find_multi_tables_still_accessible_tables(self, include_tables, fix_first=False):
        # Given a list of include_tables that must be in a valid path (not necessarily in order), iterate through the rest of the tables to figure out if there are paths between include_tables and each of those
        
        # In order for a table to be potentially connectable, it must have is_child, is_sibling or is_parent = True for all of the include_tables

        # First verify that this is a valid path that has been put forward
        if len(self.find_paths_multi_tables(include_tables, fix_first=fix_first)) == 0:
            return []

        connectable_relationships = db.session.query(TableRelationship).filter(
            TableRelationship.other_table.in_(include_tables),
            ((TableRelationship.is_parent) | (TableRelationship.is_sibling) | (TableRelationship.is_child))
        ).all()

        possible_tables = list(set([x.reference_table for x in connectable_relationships]))
        accessible_tables = []
        for table in possible_tables:
            related_include_tables = set()
            for x in connectable_relationships:
                if x.reference_table == table and x.other_table in include_tables:
                    related_include_tables.add(x.other_table)
            if len(related_include_tables) == len(include_tables):
                accessible_tables.append(table)
        
        return accessible_tables

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
        return db.session.query(TableRelationship.reference_key, TableRelationship.other_key).filter(
            TableRelationship.reference_table == table_1,
            TableRelationship.other_table == table_2
        ).first()

    def get_biggest_df_from_paths(self, paths, table_columns_of_interest):
        if len(paths) == 1:
            return self.get_df_from_path(paths[0], table_columns_of_interest)

        dfs = []
        for path in paths:
            dfs.append(self.get_df_from_path(path, table_columns_of_interest))
        biggest_df = dfs[0]

        for df in dfs[1:]:
            if len(df) > len(biggest_df):
                biggest_df = df

        return df

    def get_df_from_path(self, path, table_columns_of_interest):
        # table_columns of interest is a list of (table, column)
        sql_statement = f'SELECT '
        for table, column in table_columns_of_interest:
            # custom_name = self.db_customizer.get_custom_column_name(table, column)
            sql_statement += f'{self.prefix}_{table}.{column} AS {table}_{column}, '  # AS {custom_name}, '
        sql_statement = sql_statement[:-2]

        previous_table = f'{path[0]}'
        sql_statement += f' FROM {self.prefix}_{previous_table} '
        for current_table in path[1:]:
            current_table_db = f'{self.prefix}_{current_table}'
            previous_table_db = f'{self.prefix}_{previous_table}'
            keys = self.get_joining_keys(previous_table, current_table)
            try:
                left_key, right_key = keys[0], keys[1]
            except TypeError:
                logging.error(f'Path {path} is invalid. Unable to join {previous_table} to {current_table}')
                raise(TypeError)
            sql_statement += f'JOIN {current_table_db} ON {previous_table_db}.{left_key} = {current_table_db}.{right_key} '
            previous_table = current_table

        logging.info(sql_statement)
        df = pd.read_sql(sql_statement, con=self.data_conn)
        return df

    def aggregate_df(self, df_original, groupby_columns, filters, aggregate_column=None, aggregate_fxn='Count'):
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

    def analyze_column(self, table, column):
        sql_statement = f'SELECT {column} from {self.prefix}_{table}'
        series = pd.read_sql(sql_statement, con=self.data_conn).loc[:, column].dropna()
        if is_numeric_dtype(series):
            return {
                'type': c.COLUMN_TYPE_NUMERIC,
                'min': series.min(),
                'mean': series.mean(),
                'max': series.max(),
                'median': series.median()
            }
        else:
            return {
                'type': c.COLUMN_TYPE_TEXT,
                'possible_vals': sorted(list(series.unique()), key=lambda x: x.upper())
            }

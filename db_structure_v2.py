import logging
import os
import pandas as pd
import constants as c
import utilities as u

from collections import defaultdict


class DBMaker():
    '''
    This class will take the files in the directory and then create a SQL database
    '''

    def __init__(self, directory_path, data_file_extension='.csv', delimiter=',', overwrite=False):
        logging.info(f'Loading {directory_path}')
        self.directory_path = directory_path
        
        db_files = u.find_file_types(self.directory_path, '.db')
        if len(db_files) > 0:
            raise Exception('Cannot proceed, there is already a .db file in the directory. If you want to overwrite it, pass in "overwrite=True"')
        else:
            for x in db_files:
                logging.warning(f'Removing {x}')
                os.remove(os.path.join(self.directory_path, x))
        
        self.dataset_name = os.path.split(directory_path)[1]
        self.data_file_extension = data_file_extension
        self.delimiter = delimiter

        self.metadata = self.collect_metadata()
        self.dump_to_sql()

    def collect_metadata(self, return_metadata=False):
        # Could not find a .arch file to load data from, so we need to find out information about the directory provided

        logging.info(f'Calculating metadata for files in directory {self.directory_path}')
        
        file_names = u.find_file_types(self.directory_path, self.data_file_extension)
        metadata = defaultdict(dict)

        for file_name in file_names:
            idx = file_name.rfind('.')
            table_name = file_name[:idx]
            metadata[table_name]['file'] = file_name

            df = pd.read_csv(os.path.join(self.directory_path, file_name), delimiter=self.delimiter)
            metadata[table_name]['columns'] = []
            for column in df.columns:
                if len(df[column].dropna()) > len(df[column].dropna().unique()):
                    column_type = c.COLUMN_MANY
                else:
                    column_type = c.COLUMN_ONE
                metadata[table_name]['columns'].append({'name': column, 'type': column_type})

        logging.debug(f'Metadata: {metadata}')

        return metadata

    def dump_to_sql(self):
        pass

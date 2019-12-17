import logging
import os
import pandas as pd
import utilities as u

from sqlalchemy import create_engine


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
        if len(db_files) > 0:
            if overwrite:
                self.purge_dbs()
            else:
                raise Exception('Cannot proceed, there is already a .db file in the directory. If you want to overwrite it, pass in "overwrite=True"')

        if db_name is None:
            db_name = os.path.split(self.abs_path)[1]

        engine = create_engine('sqlite:///' + os.path.join(self.abs_path, f'{db_name}.db'), echo=False)

        data_file_names = u.find_file_types(self.directory_path, self.data_file_extension)
        for data_file_name in data_file_names:
            idx = data_file_name.rfind('.')
            table_name = data_file_name[:idx]
            logging.info(f'Writing {table_name} to db')
            df = pd.read_csv(os.path.join(self.abs_path, data_file_name))
            df.to_sql(table_name, con=engine)
        
        logging.info(f'Done writing to {db_name}.db')

    def purge_dbs(self):
        db_files = u.find_file_types(self.directory_path, '.db')
        for x in db_files:
            logging.warning(f'Removing {x}')
            os.remove(os.path.join(self.directory_path, x))

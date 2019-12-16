import os
import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':
	datasets = sorted([f.name for f in os.scandir('datasets') if f.is_dir() and 'sample' not in f], key=lambda x: x.upper())
	for dataset in datasets:
		data_dir = os.path.join(os.getcwd(), 'datasets', dataset)
		engine = create_engine('sqlite:///' + os.path.join(data_dir, f'{dataset}.db'), echo=False)
		csv_files = [x for x in os.listdir(data_dir) if x[-4:] == '.csv']
		for csv_file in csv_files:
			table_name = csv_file.split('.csv')[0]
			df = pd.read_csv(os.path.join(data_dir, f'{csv_file}'))
			df.to_sql(table_name, con=engine)

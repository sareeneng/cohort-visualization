from collections import defaultdict
import pandas as pd
import os

def get_choice(choices, prompt):
	choice_dict = {}
	counter = 1
	for choice in choices:
		print(f'{counter}: {choice}')
		choice_dict[counter] = choice
		counter += 1
	print('\n')
	user_selected_number = int(input(prompt))
	return choice_dict[user_selected_number]

if __name__ == '__main__':
	data_dir = 'datasets\\TOPICC\\'
	data_file_ext = 'csv'
	
	data_files = [x for x in os.listdir(data_dir) if x.endswith(data_file_ext)]
	dfs = {}
	for f in data_files:
		path = os.path.join(data_dir, f)
		dict_name = f.split('.')[0]  # remove csv extension
		print(f'Loading {path}')
		dfs[dict_name] = pd.read_csv(path)
	print(f'Done loading {len(dfs)} files\n')

	column_counts = defaultdict(int)
	for df in dfs.values():
		for column in df.columns:
			column_counts[column] += 1
	
	var_across_files = (get_choice(choices=[k for k, v in column_counts.items() if v > 1], prompt='Which column uniquely identifies records across all files? '))

	# now figure out which files have the most occurences of unique records in the column var_across_files
	var_across_files_counts = {}
	max_unique_ids = 0
	for df_name, df in dfs.items():
		if var_across_files in df.columns:
			unique_id_count = len(df[var_across_files].unique())
			var_across_files_counts[df_name] = unique_id_count
			max_unique_ids = max(max_unique_ids, unique_id_count)

	join_df_names = [k for k, v in var_across_files_counts.items() if v == max_unique_ids]
	print(f'Joining together {join_df_names} as they all have {max_unique_ids} unique {var_across_files}. These dataframes will be joined together and will serve as a base.')
	for df_name, df in dfs.items():
		print(f'{df_name} - {len(df)}')
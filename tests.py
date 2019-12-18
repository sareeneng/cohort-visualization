import db_structure_v2
import logging
import os
import utilities as u
import unittest

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
logger.setLevel(logging.DEBUG)

handler_info = logging.FileHandler(filename='info.log', mode='a')
handler_info.setFormatter(formatter)
handler_info.setLevel(logging.INFO)

handler_debug = logging.FileHandler(filename='debug.log', mode='w')
handler_debug.setFormatter(formatter)
handler_debug.setLevel(logging.DEBUG)

logger.addHandler(handler_info)
logger.addHandler(handler_debug)


@unittest.skip
class TestPathFinding(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.directory_path = os.path.join('datasets', 'sample2')
        self.db_maker = db_structure_v2.DBMaker(directory_path=self.directory_path)
        self.db_maker.create_db()
        self.db_linker = db_structure_v2.DBLinker(directory_path=self.directory_path)
        self.db_linker.add_global_fk('col1')
        self.db_linker.add_global_fk('col2')
        self.db_linker.add_global_fk('col3')
        self.db_linker.add_global_fk('col4')
        self.db_linker.add_global_fk('col5')
        self.db_linker.add_global_fk('col6')
        self.db_linker.add_global_fk('col7')
        self.db_linker.add_global_fk('col8')
        self.db_linker.finalize()
        self.db = db_structure_v2.DBExtractor(directory_path=self.directory_path)

    @classmethod
    def tearDownClass(self):
        print('Removing files')
        os.remove(os.path.join(self.directory_path, 'sample2.db'))
        os.remove(os.path.join(self.directory_path, 'sample2.links'))
        os.remove(os.path.join(self.directory_path, 'sample2.metadata'))

    def test_two_tables(self):
        x = self.db.find_paths_between_tables('A', 'F')
        self.assertEqual(len(x), 3)
        self.assertIn(['A', 'D', 'C', 'F'], x)
        self.assertIn(['A', 'C', 'F'], x)
        self.assertIn(['A', 'B', 'E', 'F'], x)

        x = self.db.find_paths_between_tables('B', 'F')
        self.assertEqual(len(x), 3)
        self.assertIn(['B', 'E', 'F'], x)
        self.assertIn(['B', 'A', 'D', 'C', 'F'], x)
        self.assertIn(['B', 'A', 'C', 'F'], x)

        # A could have the option to go A-->C or A-->D-->C. However it will always be better to use A-->C direct unless I need to pull in a var from D
        x = self.db.find_paths_between_tables('A', 'C')
        self.assertEqual([['A', 'C']], x)

        x = self.db.find_paths_between_tables('A', 'A')
        self.assertEqual(['A'], x)

        x = self.db.find_paths_between_tables('B', 'E')
        self.assertEqual([['B', 'E']], x)

        x = self.db.find_paths_between_tables('E', 'B')
        self.assertEqual([], x)

    def test_multi(self):
        # test back-tracking with multi_tables path-finding
        x = self.db.find_paths_multi_tables(['A', 'D', 'C', 'F'])
        self.assertEqual(len(x), 2)
        self.assertIn(['A', 'D', 'C', 'F'], x)
        self.assertIn(['A', 'C', 'D', 'C', 'F'], x)

        x = self.db.find_paths_multi_tables(['A', 'B', 'D', 'E'])
        self.assertEqual([], x)

        x = self.db.find_paths_multi_tables(['D', 'C', 'F'])
        self.assertEqual(len(x), 2)
        self.assertIn(['D', 'C', 'F'], x)
        self.assertIn(['C', 'D', 'C', 'F'], x)

        x = self.db.find_paths_multi_tables(['D', 'C', 'F'], fix_first=True)
        self.assertEqual([['D', 'C', 'F']], x)


class TestUtilities(unittest.TestCase):
    def test_duplicate_handling(self):
        test_list = [['A', 'B', 'C'], ['B', 'C', 'C'], ['A', 'B', 'C'], [], ['A', 'B', 'A']]

        x = u.remove_duplicated_lists(test_list)
        self.assertEqual([['A', 'B', 'C'], ['B', 'C', 'C'], [], ['A', 'B', 'A']], x)

        x = u.remove_adjacent_repeats(test_list)
        self.assertEqual([['A', 'B', 'C'], ['B', 'C'], ['A', 'B', 'C'], [], ['A', 'B', 'A']], x)

        x = u.remove_duplicates(['A', 'B', 'C', 'A'])
        self.assertEqual(['A', 'B', 'C'], x)

        x = u.remove_duplicates(['A'])
        self.assertEqual(['A'], x)
        
        x = u.remove_duplicates(['A', 'A'])
        self.assertEqual(['A'], x)


class TestDataExtraction(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.directory_path = os.path.join('datasets', 'TOPICC')
        # Assume that I've already set up the .links and .metadata files
        self.db_extractor = db_structure_v2.DBExtractor(directory_path=self.directory_path)

    def test_categorical(self):
        # Just get counts for each group
        path = ['HOSPITALADMIT', 'CAREPROCESSES', 'DEATH']
        df = self.db_extractor.get_df_from_path(path, table_columns_of_interest=['HOSPITALADMIT.Sex', 'CAREPROCESSES.MechVent', 'DEATH.DeathMode'])
        self.assertEqual(len(df), 275)
        self.assertEqual(list(df.columns), ['Sex', 'MechVent', 'DeathMode'])

        # Test with no real filters
        filters = {
            'MechVent': None,
            'Sex': None,
            'DeathMode': None
        }
        
        df_no_filters = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex', 'DeathMode'], filters=filters)
        self.assertEqual(len(df_no_filters), 16)
        self.assertEqual(df_no_filters['Count'].sum(), 275)

        df_aggreg_counts = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex'], filters=filters, aggregate_column='DeathMode', aggregate_fxn='Count')
        self.assertEqual(len(df_aggreg_counts.columns), 5)
        self.assertEqual(df_aggreg_counts[df_aggreg_counts['groupby_labels'] == 'Yes_Male'].iloc[0]['Failed resuscitation'], 24)

        df_aggreg_percs = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex'], filters=filters, aggregate_column='DeathMode', aggregate_fxn='Percents')
        df_aggreg_percs['PercSums'] = df_aggreg_percs['Brain death'] + df_aggreg_percs['Failed resuscitation'] + df_aggreg_percs['Limitation of care'] + df_aggreg_percs['Withdrawal of care']
        self.assertEqual(len(df_aggreg_percs[(df_aggreg_percs['PercSums'] > 99.5) & (df_aggreg_percs['PercSums'] < 100.5)]), len(df_aggreg_percs))

        # Test with filters
        filters = {
            'MechVent': {'type': 'list', 'filter': ['Yes']},
            'Sex': {'type': 'list', 'filter': ['Male']},
            'DeathMode': None
        }

        df_filters = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex', 'DeathMode'], filters=filters)
        self.assertEqual(len(df_filters), 4)
        self.assertEqual(df_filters['Count'].sum(), 129)

        filters = {
            'MechVent': {'type': 'list', 'filter': ['Yes']},
            'Sex': {'type': 'list', 'filter': ['Male']},
        }

        df_aggreg_counts = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex'], filters=filters, aggregate_column='DeathMode', aggregate_fxn='Count')
        self.assertEqual(len(df_aggreg_counts.columns), 5)
        self.assertEqual(df_aggreg_counts[df_aggreg_counts['groupby_labels'] == 'Yes_Male'].iloc[0]['Failed resuscitation'], 24)

        df_aggreg_percs = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex'], filters=filters, aggregate_column='DeathMode', aggregate_fxn='Percents')
        df_aggreg_percs['PercSums'] = df_aggreg_percs['Brain death'] + df_aggreg_percs['Failed resuscitation'] + df_aggreg_percs['Limitation of care'] + df_aggreg_percs['Withdrawal of care']
        self.assertEqual(len(df_aggreg_percs[(df_aggreg_percs['PercSums'] > 99.5) & (df_aggreg_percs['PercSums'] < 100.5)]), len(df_aggreg_percs))

    def test_numeric(self):
        path = ['HOSPITALADMIT', 'CAREPROCESSES', 'PHYSIOSTATUS']
        df = self.db_extractor.get_df_from_path(path, table_columns_of_interest=['HOSPITALADMIT.Sex', 'CAREPROCESSES.MechVent', 'PHYSIOSTATUS.LowpH'])
        self.assertEqual(len(df), 10078)

        filters = {
            'MechVent': None,
            'Sex': None,
            'LowpH': None
        }
        df_no_filters = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex', 'LowpH'], filters=filters)
        self.assertEqual(len(df_no_filters), 4)
        self.assertEqual(df_no_filters['Count'].sum(), 4815)

        df_aggreg_mean = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex'], filters=filters, aggregate_column='LowpH', aggregate_fxn='Mean')
        self.assertEqual(df_aggreg_mean[df_aggreg_mean['groupby_labels'] == 'Yes_Male'].iloc[0]['LowpH'], 7.29)

        filters = {
            'MechVent': {'type': 'list', 'filter': ['Yes']},
            'Sex': {'type': 'list', 'filter': ['Male']},
            'LowpH': {'type': 'range', 'filter': {'min': 6.8, 'max': 6.9, 'bins': 10}}
        }

        df_filters = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'Sex', 'LowpH'], filters=filters)
        self.assertEqual(len(df_filters), 10)
        self.assertEqual(df_filters['Count'].sum(), 14)

        filters = {
            'MechVent': {'type': 'list', 'filter': ['Yes']},
            'LowpH': {'type': 'range', 'filter': {'min': 6.8, 'max': 6.9, 'bins': 4}},
            'Sex': None
        }

        df_aggreg_mean = self.db_extractor.aggregate_df(df, groupby_columns=['MechVent', 'LowpH'], filters=filters, aggregate_column='Sex')
        self.assertEqual(len(df_aggreg_mean), 4)
        self.assertEqual(df_aggreg_mean[df_aggreg_mean['groupby_labels'] == 'Yes_(6.79, 6.81]'].iloc[0]['Male'], 3)


if __name__ == '__main__':
    unittest.main()

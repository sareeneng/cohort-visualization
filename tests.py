import db_structure
import logging
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


class TestPathFinding(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.db_extractor = db_structure.DBExtractor('sample3')

    def test_two_nodes(self):
        x = self.db_extractor.find_paths_between_tables('payments', 'products')
        self.assertEqual(len(x), 2)
        self.assertIn(['payments', 'orders', 'order_details', 'products'], x)
        self.assertIn(['payments', 'order_details', 'products'], x)

        x = self.db_extractor.find_paths_between_tables('orders', 'order_details')
        self.assertEqual([['orders', 'order_details']], x)

        x = self.db_extractor.find_paths_between_tables('products', 'orders')
        self.assertEqual([], x)

        x = self.db_extractor.find_paths_between_tables('payments', 'offices')
        self.assertEqual(len(x), 4)
        self.assertIn(['payments', 'orders', 'order_details', 'offices'], x)
        self.assertIn(['payments', 'orders', 'customers', 'offices'], x)
        self.assertIn(['payments', 'order_details', 'offices'], x)
        self.assertIn(['payments', 'order_details', 'orders', 'customers', 'offices'], x)
    
    def test_multi(self):
        x = self.db_extractor.find_paths_multi_tables(['payments', 'products', 'offices'])
        self.assertEqual(len(x), )


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


@unittest.skip
class TestDataExtraction(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Assume that I've already set up all the links and such
        self.db_extractor = db_structure.DBExtractor(dataset_name='TOPICC')

    def test_categorical(self):
        # Just get counts for each group
        path = ['HOSPITALADMIT', 'CAREPROCESSES', 'DEATH']
        df = self.db_extractor.get_df_from_path(path, table_columns_of_interest=[('HOSPITALADMIT', 'Sex'), ('CAREPROCESSES', 'MechVent'), ('DEATH', 'DeathMode')])
        self.assertEqual(len(df), 275)
        self.assertEqual(list(df.columns), ['HOSPITALADMIT_Sex', 'CAREPROCESSES_MechVent', 'DEATH_DeathMode'])

        # Test with no real filters
        filters = {
            'CAREPROCESSES_MechVent': None,
            'HOSPITALADMIT_Sex': None,
            'DEATH_DeathMode': None
        }
        
        df_no_filters = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex', 'DEATH_DeathMode'], filters=filters)
        self.assertEqual(len(df_no_filters), 16)
        self.assertEqual(df_no_filters['Count'].sum(), 275)

        df_aggreg_counts = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex'], filters=filters, aggregate_column='DEATH_DeathMode', aggregate_fxn='Count')
        self.assertEqual(len(df_aggreg_counts.columns), 5)
        self.assertEqual(df_aggreg_counts[df_aggreg_counts['groupby_labels'] == 'Yes_Male'].iloc[0]['Failed resuscitation'], 24)

        df_aggreg_percs = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex'], filters=filters, aggregate_column='DEATH_DeathMode', aggregate_fxn='Percents')
        df_aggreg_percs['PercSums'] = df_aggreg_percs['Brain death'] + df_aggreg_percs['Failed resuscitation'] + df_aggreg_percs['Limitation of care'] + df_aggreg_percs['Withdrawal of care']
        self.assertEqual(len(df_aggreg_percs[(df_aggreg_percs['PercSums'] > 99.5) & (df_aggreg_percs['PercSums'] < 100.5)]), len(df_aggreg_percs))

        # Test with filters
        filters = {
            'CAREPROCESSES_MechVent': {'type': 'list', 'filter': ['Yes']},
            'HOSPITALADMIT_Sex': {'type': 'list', 'filter': ['Male']},
            'DEATH_DeathMode': None
        }

        df_filters = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex', 'DEATH_DeathMode'], filters=filters)
        self.assertEqual(len(df_filters), 4)
        self.assertEqual(df_filters['Count'].sum(), 129)

        filters = {
            'CAREPROCESSES_MechVent': {'type': 'list', 'filter': ['Yes']},
            'HOSPITALADMIT_Sex': {'type': 'list', 'filter': ['Male']},
        }

        df_aggreg_counts = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex'], filters=filters, aggregate_column='DEATH_DeathMode', aggregate_fxn='Count')
        self.assertEqual(len(df_aggreg_counts.columns), 5)
        self.assertEqual(df_aggreg_counts[df_aggreg_counts['groupby_labels'] == 'Yes_Male'].iloc[0]['Failed resuscitation'], 24)

        df_aggreg_percs = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex'], filters=filters, aggregate_column='DEATH_DeathMode', aggregate_fxn='Percents')
        df_aggreg_percs['PercSums'] = df_aggreg_percs['Brain death'] + df_aggreg_percs['Failed resuscitation'] + df_aggreg_percs['Limitation of care'] + df_aggreg_percs['Withdrawal of care']
        self.assertEqual(len(df_aggreg_percs[(df_aggreg_percs['PercSums'] > 99.5) & (df_aggreg_percs['PercSums'] < 100.5)]), len(df_aggreg_percs))

    def test_numeric(self):
        path = ['HOSPITALADMIT', 'CAREPROCESSES', 'PHYSIOSTATUS']
        df = self.db_extractor.get_df_from_path(path, table_columns_of_interest=[('HOSPITALADMIT', 'Sex'), ('CAREPROCESSES', 'MechVent'), ('PHYSIOSTATUS', 'LowpH')])
        self.assertEqual(len(df), 10078)

        filters = {
            'CAREPROCESSES_MechVent': None,
            'HOSPITALADMIT_Sex': None,
            'PHYSIOSTATUS_LowpH': None
        }
        df_no_filters = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex', 'PHYSIOSTATUS_LowpH'], filters=filters)
        self.assertEqual(len(df_no_filters), 4)
        self.assertEqual(df_no_filters['Count'].sum(), 4815)

        df_aggreg_mean = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex'], filters=filters, aggregate_column='PHYSIOSTATUS_LowpH', aggregate_fxn='Mean')
        self.assertEqual(df_aggreg_mean[df_aggreg_mean['groupby_labels'] == 'Yes_Male'].iloc[0]['PHYSIOSTATUS_LowpH'], 7.29)

        filters = {
            'CAREPROCESSES_MechVent': {'type': 'list', 'filter': ['Yes']},
            'HOSPITALADMIT_Sex': {'type': 'list', 'filter': ['Male']},
            'PHYSIOSTATUS_LowpH': {'type': 'range', 'filter': {'min': 6.8, 'max': 6.9, 'bins': 10}}
        }

        df_filters = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'HOSPITALADMIT_Sex', 'PHYSIOSTATUS_LowpH'], filters=filters)
        self.assertEqual(len(df_filters), 10)
        self.assertEqual(df_filters['Count'].sum(), 14)

        filters = {
            'CAREPROCESSES_MechVent': {'type': 'list', 'filter': ['Yes']},
            'PHYSIOSTATUS_LowpH': {'type': 'range', 'filter': {'min': 6.8, 'max': 6.9, 'bins': 4}},
            'HOSPITALADMIT_Sex': None
        }

        df_aggreg_mean = self.db_extractor.aggregate_df(df, groupby_columns=['CAREPROCESSES_MechVent', 'PHYSIOSTATUS_LowpH'], filters=filters, aggregate_column='HOSPITALADMIT_Sex')
        self.assertEqual(len(df_aggreg_mean), 4)
        self.assertEqual(df_aggreg_mean[df_aggreg_mean['groupby_labels'] == 'Yes_(6.79, 6.81]'].iloc[0]['Male'], 3)


if __name__ == '__main__':
    unittest.main()

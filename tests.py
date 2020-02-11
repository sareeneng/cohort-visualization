import db_structure
import graph
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
        g = graph.Graph()
        g.add_node('A')
        g.add_node('B')
        g.add_node('C')
        g.add_node('D')
        g.add_node('E')
        g.add_node('F')
        g.add_edge('A', 'C', weight=2)
        g.add_edge('A', 'D', weight=4)
        g.add_edge('A', 'B', weight=1)
        g.add_edge('B', 'A', weight=3)
        g.add_edge('B', 'E', weight=5)
        g.add_edge('C', 'D', weight=2)
        g.add_edge('C', 'F', weight=3)
        g.add_edge('D', 'C', weight=3)
        g.add_edge('E', 'F', weight=3)
        self.g = g

    def test_two_nodes(self):
        x = self.g.find_paths_between_nodes('A', 'F')
        self.assertEqual(len(x), 3)
        self.assertIn(['A', 'D', 'C', 'F'], x)
        self.assertIn(['A', 'C', 'F'], x)
        self.assertIn(['A', 'B', 'E', 'F'], x)
        sum_weights = sorted([i.get_weight('sum') for i in x])
        self.assertEqual([5, 9, 10], sum_weights)
        min_weights = sorted([i.get_weight('min') for i in x])
        self.assertEqual([1, 2, 3], min_weights)
        max_weights = sorted([i.get_weight('max') for i in x])
        self.assertEqual([3, 4, 5], max_weights)

        x = self.g.find_paths_between_nodes('B', 'F')
        self.assertEqual(len(x), 3)
        self.assertIn(['B', 'E', 'F'], x)
        self.assertIn(['B', 'A', 'D', 'C', 'F'], x)
        self.assertIn(['B', 'A', 'C', 'F'], x)
        sum_weights = sorted([i.get_weight('sum') for i in x])
        self.assertEqual([8, 8, 13], sum_weights)
        min_weights = sorted([i.get_weight('min') for i in x])
        self.assertEqual([2, 3, 3], min_weights)
        max_weights = sorted([i.get_weight('max') for i in x])
        self.assertEqual([3, 4, 5], max_weights)

        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='largest', weight_operation='min')
        self.assertEqual([['A', 'D', 'C', 'F']], x)
        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='largest', weight_operation='max')
        self.assertEqual([['A', 'B', 'E', 'F']], x)
        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='largest', weight_operation='sum')
        self.assertEqual([['A', 'D', 'C', 'F']], x)
        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='smallest', weight_operation='min')
        self.assertEqual([['A', 'B', 'E', 'F']], x)
        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='smallest', weight_operation='max')
        self.assertEqual([['A', 'C', 'F']], x)
        x = self.g.find_best_paths_between_nodes('A', 'F', best_is='smallest', weight_operation='sum')
        self.assertEqual([['A', 'C', 'F']], x)

        # A could have the option to go A-->C or A-->D-->C. However it will always be better to use A-->C direct unless I need to pull in a var from D
        x = self.g.find_paths_between_nodes('A', 'C')
        self.assertEqual([['A', 'C']], x)

        x = self.g.find_paths_between_nodes('A', 'A')
        self.assertEqual(['A'], x)

        x = self.g.find_paths_between_nodes('B', 'E')
        self.assertEqual([['B', 'E']], x)

        x = self.g.find_paths_between_nodes('E', 'B')
        self.assertEqual([], x)

    def test_multi(self):
        # ensure no back-tracking with multi_tables path-finding
        x = self.g.find_paths_multi_nodes(['A', 'D', 'C', 'F'], back_tracking_allowed=False)
        self.assertEqual(len(x), 1)
        self.assertIn(['A', 'D', 'C', 'F'], x)

        # now check back-tracking with multi_tables path-finding
        x = self.g.find_paths_multi_nodes(['A', 'D', 'C', 'F'], back_tracking_allowed=True)
        self.assertEqual(len(x), 2)
        self.assertIn(['A', 'D', 'C', 'F'], x)
        self.assertIn(['A', 'C', 'D', 'C', 'F'], x)

        x = self.g.find_paths_multi_nodes(['A', 'B', 'D', 'E'])
        self.assertEqual([], x)

        x = self.g.find_paths_multi_nodes(['D', 'C', 'F'], back_tracking_allowed=True)
        self.assertEqual(len(x), 2)
        self.assertIn(['D', 'C', 'F'], x)
        self.assertIn(['C', 'D', 'C', 'F'], x)

        x = self.g.find_paths_multi_nodes(['D', 'C', 'F'], fix_first=True)
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

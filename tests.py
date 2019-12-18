import db_structure_v2
import os
import utilities as u
import unittest


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


if __name__ == '__main__':
    unittest.main()

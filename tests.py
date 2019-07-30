import db_structure
import utilities as u
import pandas as pd
import unittest

class TestPathFinding(unittest.TestCase):
	@classmethod
	def setUpClass(self):
		self.db = db_structure.DB('sample2')
		self.A = self.db.tables['A']
		self.B = self.db.tables['B']
		self.C = self.db.tables['C']
		self.D = self.db.tables['D']
		self.E = self.db.tables['E']
		self.F = self.db.tables['F']
		
		
	def test_two_tables(self):
		x = self.db.find_paths_between_tables(self.A, self.F)
		self.assertEqual(len(x), 3)
		self.assertIn([self.A, self.D, self.C, self.F], x)
		self.assertIn([self.A, self.C, self.F], x)
		self.assertIn([self.A, self.B, self.E, self.F], x)

		x = self.db.find_paths_between_tables(self.B, self.F)
		self.assertEqual(len(x), 3)
		self.assertIn([self.B, self.E, self.F], x)
		self.assertIn([self.B, self.A, self.D, self.C, self.F], x)
		self.assertIn([self.B, self.A, self.C, self.F], x)

		# A could have the option to go A-->C or A-->D-->C. However it will always be better to use A-->C direct unless I need to pull in a var from D
		x = self.db.find_paths_between_tables(self.A, self.C)
		self.assertEqual([[self.A, self.C]], x)

		x = self.db.find_paths_between_tables(self.A, self.A)
		self.assertEqual([self.A], x)

		x = self.db.find_paths_between_tables(self.B, self.E)
		self.assertEqual([[self.B, self.E]], x)

		x = self.db.find_paths_between_tables(self.E, self.B)
		self.assertEqual([], x)

	def test_multi(self):
		# test back-tracking with multi_tables path-finding
		x = self.db.find_paths_multi_tables([self.A, self.D, self.C, self.F])
		self.assertEqual(len(x), 2)
		self.assertIn([self.A, self.D, self.C, self.F], x)
		self.assertIn([self.A, self.C, self.D, self.C, self.F], x)

		x = self.db.find_paths_multi_tables([self.A, self.B, self.D, self.E])
		self.assertEqual([], x)

		x = self.db.find_paths_multi_tables([self.D, self.C, self.F])
		self.assertEqual(len(x), 2)
		self.assertIn([self.D, self.C, self.F], x)
		self.assertIn([self.C, self.D, self.C, self.F], x)

		x = self.db.find_paths_multi_tables([self.D, self.C, self.F], fix_first=True)
		self.assertEqual([[self.D, self.C, self.F]], x)

		colx = self.db.common_columns['col1']  # in tables A, C
		coly = self.db.common_columns['col4']  # in tables B, E
		colz = self.db.tables['F'].columns['col8']  # only in table F

		x = self.db.find_paths_multi_columns([colx, coly, colz])
		self.assertEqual(len(x), 6)
		self.assertIn([self.A, self.B, self.E, self.F], x)
		self.assertIn([self.A, self.B, self.A, self.D, self.C, self.F], x)
		self.assertIn([self.A, self.B, self.A, self.C, self.F], x)
		self.assertIn([self.B, self.A, self.D, self.C, self.F], x)
		self.assertIn([self.B, self.A, self.C, self.F], x)
		self.assertIn([self.B, self.A, self.B, self.E, self.F], x)

		x = self.db.find_paths_multi_columns([colx, coly, colz], fix_first=True)
		self.assertEqual(len(x), 3)

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
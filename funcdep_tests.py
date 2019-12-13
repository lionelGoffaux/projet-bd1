import os
import sqlite3
import unittest

import funcdep
import utils

TEST_DB = os.path.join(os.getcwd(), 'test.sqlite')


class FuncDepTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        init_db_sql = os.path.join('misc', 'init_test_db.sql')

        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        utils.execute_sql_file(c, init_db_sql)
        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            os.remove(TEST_DB)
        except:  # TODO: spécifier les erreurs possibles
            pass

    def setUp(self) -> None:
        self.db = funcdep.DB('test.sqlite')

    def tearDown(self) -> None:
        self.db.close()

    def test_name(self):
        self.assertEqual('test.sqlite', self.db.name)

    def test_get_tables(self):
        tables = ['TRIPS', 'BUSES', 'DESTINATIONS']

        for t in tables:
            self.assertIn(t, self.db.tables)

    def test_get_fields(self):
        fields = ['Number_Plate', 'Chassis', 'Make', 'Mileage']

        for f in fields:
            self.assertIn(f, self.db.get_fields('BUSES'))

    def test_table_df(self):
        self.db.add_df('BUSES', 'Chassis', 'Mileage')
        self.assertIn('FuncDep', self.db.tables)

    def test_add_df(self):
        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.assertIn(('TRIPS', 'Date Driver Departure_Time', 'Destination'), self.db.list_df())
        self.assertIn(('TRIPS', 'Date Driver Departure_Time', 'Destination'), self.db.list_table_df('TRIPS'))

    def test_add_twice(self):
        self.db.purge_df()

        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        
        with self.assertRaises(funcdep.DFAddTwiceError):
            self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')


    def test_unknow_table(self):
        with self.assertRaises(funcdep.UnknowTableError):
            self.db.add_df('RANDOM', 'Chassis', 'Mileage')

        with self.assertRaises(funcdep.UnknowTableError):
            self.db.get_fields('RONDOM')

        with self.assertRaises(funcdep.UnknowTableError):
            self.db.check_table_df('RANDOM')

        with self.assertRaises(funcdep.UnknowTableError):
            self.db.del_df('RANDOM', 'Chassis', 'Mileage')

        with self.assertRaises(funcdep.UnknowTableError):
            self.db.list_table_df('RANDOM')

    def test_unknow_fields(self):
        with self.assertRaises(funcdep.UnknowFieldsError):
            self.db.add_df('BUSES', 'Chassis', 'Random')

        with self.assertRaises(funcdep.UnknowFieldsError):
            self.db.add_df('BUSES', 'Random', 'Mileage')

        with self.assertRaises(funcdep.UnknowFieldsError):
            self.db.add_df('BUSES', 'Chassis Random', 'Mileage')

    def test_df_not_found(self):
        with self.assertRaises(funcdep.DFNotFoundError):
            self.db.del_df('BUSES', 'Make', 'Mileage')

    def test_df_not_singular(self):
        with self.assertRaises(funcdep.DFNotSingularError):
            self.db.add_df('BUSES', 'Chassis', 'Mileage Make')

    def test_df_table_not_accepted(self):
        with self.assertRaises(funcdep.DFTableError):
            self.db.add_df('FuncDep', 'Chassis', 'Mileage')

        with self.assertRaises(funcdep.DFTableError):
            self.db.get_fields('FuncDep')

    def test_rhs_not_include(self):
        with self.assertRaises(funcdep.RHSIncludeToLHSError):
            self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Date')

    def test_purge_df(self):
        self.db.purge_df()

        self.assertEqual(0, len(self.db.list_df()))

    # TODO: test ckeck df

if __name__ == '__main__':
    unittest.main()

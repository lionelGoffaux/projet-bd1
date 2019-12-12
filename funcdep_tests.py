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

        for t in self.db.tables:
            self.assertIn(t, tables)

    def test_get_fields(self):
        fields = ['Number_Plate', 'Chassis', 'Make', 'Mileage']

        for f in fields:
            self.assertIn(f, self.db.get_fields('BUSES'))

        for f in self.db.get_fields('BUSES'):
            self.assertIn(f, fields)

    def test_table_df(self):
        self.assertNotIn('Funcdep', self.db.tables)
        self.db.add_df('BUSES', 'Chassis', 'Mileage')
        self.assertIn('Funcdep', self.db.tables)

    def test_add_df(self):
        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.assertIn(('TRIPS', 'Date Driver Departure_Time', 'Destination'), self.db.list_df())
        self.assertIn(('TRIPS', 'Date Driver Departure_Time', 'Destination'), self.db.list_table_df('TRIPS'))


if __name__ == '__main__':
    unittest.main()

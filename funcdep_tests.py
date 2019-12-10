import os
import sqlite3
import unittest

import funcdep
import utils

TEST_DB = os.path.join(os.getcwd(), 'test.sqlite')


class MyTestCase(unittest.TestCase):

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
        self.assertEqual(['TRIPS', 'BUSES', 'DESTINATIONS'], self.db.tables)

    def test_get_flieds(self):
        self.assertEqual(['Number_Plate', 'Chassis', 'Make', 'Mileage'], self.db.get_fields('BUSES'))


if __name__ == '__main__':
    unittest.main()

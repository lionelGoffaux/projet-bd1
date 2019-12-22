import os
import sqlite3
import unittest

import funcdep
import utils

TEST_DB = os.path.join(os.getcwd(), 'test.sqlite')


class FuncDepTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.tearDownClass()

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

    def test_unknown_table(self):
        with self.assertRaises(funcdep.UnknownTableError):
            self.db.add_df('RANDOM', 'Chassis', 'Mileage')

        with self.assertRaises(funcdep.UnknownTableError):
            self.db.get_fields('RANDOM')

        with self.assertRaises(funcdep.UnknownTableError):
            self.db.check_table_df('RANDOM')

        with self.assertRaises(funcdep.UnknownTableError):
            self.db.del_df('RANDOM', 'Chassis', 'Mileage')

        with self.assertRaises(funcdep.UnknownTableError):
            self.db.list_table_df('RANDOM')

        with self.assertRaises(funcdep.UnknownTableError):
            self.db.check_table_df('RANDOM')

    def test_unknown_fields(self):
        with self.assertRaises(funcdep.UnknownFieldsError):
            self.db.add_df('BUSES', 'Chassis', 'Random')

        with self.assertRaises(funcdep.UnknownFieldsError):
            self.db.add_df('BUSES', 'Random', 'Mileage')

        with self.assertRaises(funcdep.UnknownFieldsError):
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

    def test_check_df(self):
        self.db.purge_df()
        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.db.add_df('BUSES', 'Chassis', 'Make')

        res = {('TRIPS', 'Date Driver Departure_Time', 'Destination'): [],
               ('BUSES', 'Chassis', 'Make'): [("DDT 123",    "XGUR6775",   "Renault",    212342),
                                               ("DDT 456",    "XGUR6775",   "Mercedes",   212350)]}

        res_trips = {('TRIPS', 'Date Driver Departure_Time', 'Destination'): []}

        res_buses = {('BUSES', 'Chassis', 'Make'): [("DDT 123",    "XGUR6775",   "Renault",    212342),
                                                    ("DDT 456",    "XGUR6775",   "Mercedes",   212350)]}

        self.assertEqual(res, self.db.check_df())
        self.assertEqual(res_trips, self.db.check_table_df('TRIPS'))
        self.assertEqual(res_buses, self.db.check_table_df('BUSES'))

    def test_df_closure(self):
        self.db.purge_df()

        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.db.add_df('TRIPS', 'Date Destination Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Driver')

        expected_result = ('Date', 'Number_Plate', 'Departure_Time','Driver', 'Destination')
        result = self.db.df_closure('Date Number_Plate Departure_Time', self.db.list_df())

        self.assertEqual(5, len(result))

        for r in expected_result:
            self.assertIn(r, result)

    def test_detect_useless_df(self):
        self.db.purge_df()

        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.db.add_df('TRIPS', 'Date Destination Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Driver')

        self.assertTrue(self.db.is_df_useless(('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')))

        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')
        self.assertTrue(self.db.is_df_useless(('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')))

    def test_find_useless_df(self):
        self.db.purge_df()

        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.db.add_df('TRIPS', 'Date Destination Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')

        expected_res = [('TRIPS', 'Date Number_Plate Departure_Time', 'Driver'), ('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')]
        res = self.db.find_useless_df()

        self.assertEqual(expected_res, res)


    def test_clean_useless_df(self):
        self.db.purge_df()

        self.db.add_df('TRIPS', 'Date Driver Departure_Time', 'Destination')
        self.db.add_df('TRIPS', 'Date Destination Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Driver')
        self.db.add_df('TRIPS', 'Date Number_Plate Departure_Time', 'Destination')

        self.db.clean_useless_df()

        self.assertEqual(0, len(self.db.find_useless_df()))

    def test_find_super_key(self):
        self.db.purge_df()
        expected_res=[['Date'], ['Date', 'Departure_Time'], ['Date', 'Departure_Time', 'Destination'], ['Date', 'Departure_Time', 'Destination', 'Driver'], ['Date', 'Departure_Time', 'Destination', 'Driver', 'Number_Plate']]
        self.db.add_df('TRIPS', 'Date', 'Departure_Time')
        self.db.add_df('TRIPS', 'Date', 'Destination')
        self.db.add_df('TRIPS', 'Date', 'Number_Plate')
        self.db.add_df('TRIPS', 'Date', 'Driver')
        attributs = "Date Number_Plate Departure_Time Driver Destination"
        self.assertEqual(expected_res,self.db.find_super_key(attributs,self.db.list_df()))

    def test_find_ckey(self):
        self.db.purge_df()
        expected_res=[['Date']]
        self.db.add_df('TRIPS', 'Date', 'Departure_Time')
        self.db.add_df('TRIPS', 'Date', 'Destination')
        self.db.add_df('TRIPS', 'Date', 'Number_Plate')
        self.db.add_df('TRIPS', 'Date', 'Driver')
        self.db.add_df('TRIPS', 'Driver', 'Destination')
        attributs = "Date Number_Plate Departure_Time Driver Destination"
        self.assertEqual(expected_res,self.db.find_ckey(attributs,self.db.list_df()))
    
    def test_is_bcnf(self):
        self.db.purge_df()
        attributs = "Date Number_Plate Departure_Time Driver Destination"
        self.db.add_df('TRIPS', 'Date', 'Departure_Time')
        self.db.add_df('TRIPS', 'Date', 'Destination')
        self.db.add_df('TRIPS', 'Date', 'Number_Plate')
        self.db.add_df('TRIPS', 'Date', 'Driver')
        self.assertTrue(self.db.is_bcnf(attributs,self.db.list_df()))

    def test_is_3nf(self):
        self.db.purge_df()
        attributs = "Date Number_Plate Departure_Time"
        self.db.add_df('TRIPS', 'Date Number_Plate', 'Departure_Time')
        self.db.add_df('TRIPS', 'Departure_Time', 'Number_Plate')
        self.assertTrue(self.db.is_3nf(attributs,self.db.list_df()))

if __name__ == '__main__':
    unittest.main()

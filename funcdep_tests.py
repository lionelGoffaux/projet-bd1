import unittest
import funcdep


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.db = funcdep.DB('test.db')

    def tearDown(self) -> None:
        self.db.close()

    def test_name_immutable(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()

import unittest
from src.etl import extraction


class TestDataExtraction(unittest.TestCase):

    def test_api_connection(self):
        with self.assertRaises(Exception):
            extraction()


if __name__ == "__main__":
    unittest.main()

import unittest
from src.etl import extraction
from config import API_URL


class TestDataExtraction(unittest.TestCase):

    def test_api_connection(self):
        with self.assertRaises(Exception):
            extraction(API_URL)


if __name__ == "__main__":
    unittest.main()

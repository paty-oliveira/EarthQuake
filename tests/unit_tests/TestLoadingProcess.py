import unittest
from tests.PySparkSetup import PySparkSetup
from src.Pipeline import Storage, Loading


class FakeStorage(Storage):

    def __init__(self):
        self.loaded_data = None

    def save(self, data):
        self.loaded_data = data


class SetupLoadingProcess(PySparkSetup):

    def test_loaded_data_should_have_same_columns_comparing_with_original_dataframe(self):
        repository = FakeStorage()
        loading = Loading(repository)
        loading.load(self.test_data)

        current_result = repository.loaded_data.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_loaded_data_should_have_same_schema_comparing_with_original_dataframe(self):
        repository = FakeStorage()
        loading = Loading(repository)
        loading.load(self.test_data)

        current_result = repository.loaded_data.schema
        expected_result = self.test_data.schema

        self.assertEqual(current_result, expected_result)

    def test_loaded_data_should_have_same_number_of_rows_comparing_with_original_dataframe(self):
        repository = FakeStorage()
        loading = Loading(repository)
        loading.load(self.test_data)

        current_result = repository.loaded_data.count()
        expected_result = self.test_data.count()

        self.assertEqual(current_result, expected_result)


if __name__ == '__main__':
    unittest.main()

import unittest
import os
from unittest.mock import patch
from tests.setup.PySparkSetup import PySparkSetup
from src.Pipeline import ApiInput, Extraction, Transformation, Loading, CsvStorage
from pyspark.sql.types import TimestampType, DateType


class TestSparkDataPipeline(PySparkSetup):
    FAKE_URL = "http://fake_url.com"
    TMP_FOLDER_PATH = "tests/integration_tests/spark/tmp"
    OUTPUT_FILEPATH = "tests/integration_tests/spark/tmp/testing.csv"
    FAKE_INPUT_DATA = [(1623884400, "California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
                       (1623798000, "Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
                       (1623932824, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
                       (1623625200, "Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
                        "yellow"),
                       (1623538800, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10.0], "green"),
                       (1623452400, "Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
                       (1623366000, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
                       (1623279600, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
                       (1623193200, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
                       (1623106800, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10.0], "green")]
    FAKE_EXPECTED_DATA = [("2021-06-17", "California", 0.82, "automatic", -116.8, 33.3333333, 12.04),
                          ("2021-06-16", "Alaska", 1.1, "automatic", -148.942, 64.9081, 10.6),
                          ("2021-06-17", "Chile", 4.9, "reviewed", -70.6202, -21.4265, 52.24),
                          ("2021-06-14", "Hawaii", 2.0099, "automatic", -155.429000854492,
                           19.2180004119873, 33.2999992370605),
                          ("2021-06-13", "Indonesia", 4.8, "reviewed", 126.419, 0.2661, 10.0),
                          ("2021-06-12", "Nevada", 0.5, "automatic", -116.242, 36.7564, 0.8),
                          ("2021-06-11", "Arkansas", 1.9, "reviewed", -91.4295, 35.863, 16.41),
                          ("2021-06-10", "Montana", 1.33, "reviewed", -110.434, 44.4718333, 2.21),
                          ("2021-06-09", "Oklahoma", 1.58, "reviewed", -98.53233333, 36.57083333, 6.31),
                          ("2021-06-08", "Idaho", 2.6, "reviewed", -115.186, 44.2666, 10.0)]

    def create_tmp_folder(self):
        if not os.path.exists(self.TMP_FOLDER_PATH):
            os.makedirs(self.TMP_FOLDER_PATH)

    def delete_test_file(self):
        if os.path.isfile(self.OUTPUT_FILEPATH):
            os.remove(self.OUTPUT_FILEPATH)

    @patch.object(ApiInput, 'get_data')
    def test_should_return_transformed_data_using_all_pipeline_components(self, mock_get_data):
        self.create_tmp_folder()

        fake_api_input = ApiInput(self.FAKE_URL)
        mock_get_data.return_value = self.FAKE_INPUT_DATA

        extraction_process = Extraction(fake_api_input)
        extraction_process.extract()

        raw_data = extraction_process.data
        raw_df = self.spark.createDataFrame(raw_data, ["date", "place", "mag", "status", "coordinates", "alert"])

        transformation_process = Transformation(raw_df)
        transformation_process.drop(["alert"])
        transformation_process.rename({"mag": "magnitude", "place": "city"})
        transformation_process.replace_null_values({"status": "Automatic"})
        transformation_process.lowercase(["status"])
        transformation_process.convert_data_type({"date": TimestampType()})
        transformation_process.convert_data_type({"date": DateType()})
        transformation_process.split_content("coordinates", ["longitude", "latitude", "depth"])
        transformed_df = transformation_process.dataframe

        csv_storage = CsvStorage(self.OUTPUT_FILEPATH)
        loading_process = Loading(csv_storage)
        loading_process.load(transformed_df)

        current_result = self.spark \
            .read \
            .csv(self.OUTPUT_FILEPATH, header=True, inferSchema=True) \
            .collect()
        expected_result = self.spark \
            .createDataFrame(self.FAKE_EXPECTED_DATA,
                             ["date", "city", "magnitude", "status", "longitude", "latitude", "depth"]) \
            .collect()

        self.assertEqual(current_result, expected_result)

        self.delete_test_file()


if __name__ == '__main__':
    unittest.main()

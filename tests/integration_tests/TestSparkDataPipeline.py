import unittest
from unittest.mock import patch
from tests.PySparkSetup import PySparkSetup
from src.Pipeline import ApiInput, Extraction, Transformation, Loading, CsvStorage


class TestSparkDataPipeline(PySparkSetup):
    FAKE_URL = "http://fake_url.com"
    FAKE_INPUT_DATA = [("California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
                       ("Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
                       ("Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
                       ("Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
                        "yellow"),
                       ("Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10.0], "green"),
                       ("Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
                       ("Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
                       ["Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None],
                       ("Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
                       ("Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10.0], "green")]
    FAKE_EXPECTED_DATA = [("California", 0.82, "automatic", -116.8, 33.3333333, 12.04),
                          ("Alaska", 1.1, "automatic", -148.942, 64.9081, 10.6),
                          ("Chile", 4.9, "reviewed", -70.6202, -21.4265, 52.24),
                          ("Hawaii", 2.0099, "automatic", -155.429000854492,
                           19.2180004119873, 33.2999992370605),
                          ("Indonesia", 4.8, "reviewed", 126.419, 0.2661, 10.0),
                          ("Nevada", 0.5, "automatic", -116.242, 36.7564, 0.8),
                          ("Arkansas", 1.9, "reviewed", -91.4295, 35.863, 16.41),
                          ("Montana", 1.33, "reviewed", -110.434, 44.4718333, 2.21),
                          ("Oklahoma", 1.58, "reviewed", -98.53233333, 36.57083333, 6.31),
                          ("Idaho", 2.6, "reviewed", -115.186, 44.2666, 10.0)]
    OUTPUT_FILEPATH = "tests/integration_tests/tmp/testing.csv"

    @patch.object(ApiInput, 'get_data')
    def test_should_return_transformed_data_using_all_pipeline_components(self, mock_get_data):
        fake_api_input = ApiInput(self.FAKE_URL)
        mock_get_data.return_value = self.FAKE_INPUT_DATA

        extraction_process = Extraction(fake_api_input)
        extraction_process.extract()

        raw_data = extraction_process.data
        raw_df = self.spark.createDataFrame(raw_data, ["place", "mag", "status", "coordinates", "alert"])

        transformation_process = Transformation(raw_df)
        transformation_process.drop(["alert"])
        transformation_process.rename({"mag": "magnitude", "place": "city"})
        transformation_process.replace_null_values({"status": "Automatic"})
        transformation_process.lowercase(["status"])
        transformation_process.split_content("coordinates", ["longitude", "latitude", "depth"])

        transformed_df = transformation_process.dataframe

        csv_storage = CsvStorage(self.OUTPUT_FILEPATH)
        loading_process = Loading(csv_storage)
        loading_process.load(transformed_df)

        current_result = self.spark\
            .read\
            .csv(self.OUTPUT_FILEPATH, header=True, inferSchema=True)\
            .collect()
        expected_result = self.spark \
            .createDataFrame(self.FAKE_EXPECTED_DATA,
                             ["city", "magnitude", "status", "longitude", "latitude", "depth"])\
            .collect()

        self.assertEqual(current_result, expected_result)


if __name__ == '__main__':
    unittest.main()

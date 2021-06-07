import unittest
import logging
from pyspark.sql import SparkSession
from src.EtlPipeline import Transformation


class PySparkTest(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)

    @classmethod
    def create_testing_spark_session(cls):
        return SparkSession \
            .builder \
            .master("local") \
            .appName("my-local-testing-pyspark-context") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_spark_session()
        cls.input_df = cls.spark.createDataFrame([
            ("California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
            ("Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            ("Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            ("Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
            ("Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            ("Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
            ("Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            ("Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            ("Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            ("Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["place", "mag", "status", "coordinates", "alert"]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestLowercaseTransformation(PySparkTest):

    def test_should_have_same_df_when_column_param_is_empty(self):
        transformation = Transformation(self.input_df)
        transformation.to_lowercase([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.input_df.collect())

    def test_should_have_same_df_when_column_not_exist_in_dataframe(self):
        transformation = Transformation(self.input_df)
        transformation.to_lowercase(['xpto'])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.input_df.collect())

    def test_should_transform_one_column_in_lowercase(self):
        transformation = Transformation(self.input_df)
        transformation.to_lowercase(['place'])

        transformed_df = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            ("california", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
            ("alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            ("chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            ("hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
            ("indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            ("nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
            ("arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            ("montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            ("oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            ("idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(transformed_df, expected_result)

    def test_should_transform_many_colums_in_lowercase(self):
        transformation = Transformation(self.input_df)
        transformation.to_lowercase(['place', 'status'])

        transformed_df = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            ("california", 0.82, "automatic", [-116.8, 33.3333333, 12.04], None),
            ("alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            ("chile", 4.9, "reviewed", [-70.6202, -21.4265, 52.24], None),
            ("hawaii", 2.0099, "automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
            ("indonesia", 4.8, "reviewed", [126.419, 0.2661, 10], "green"),
            ("nevada", 0.5, "automatic", [-116.242, 36.7564, 0.8], None),
            ("arkansas", 1.9, "reviewed", [-91.4295, 35.863, 16.41], "green"),
            ("montana", 1.33, "reviewed", [-110.434, 44.4718333, 2.21], None),
            ("oklahoma", 1.58, "reviewed", [-98.53233333, 36.57083333, 6.31], None),
            ("idaho", 2.6, "reviewed", [-115.186, 44.2666, 10], "green")
        ], ["place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(transformed_df, expected_result)


class TestDropColumns(PySparkTest):

    def test_should_have_same_df_when_column_list_is_empty(self):
        transformation = Transformation(self.input_df)
        transformation.drop([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.input_df.collect())

    def test_should_return_same_df_when_column_not_exists(self):
        transformation = Transformation(self.input_df)
        transformation.drop(['xpto'])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.input_df.collect())

    def test_should_remove_one_column_from_dataframe(self):
        transformation = Transformation(self.input_df)
        transformation.drop(['alert'])

        current_result = transformation.dataframe.columns
        expected_result = self.spark.createDataFrame([
            ("california", 0.82, "automatic", [-116.8, 33.3333333, 12.04]),
            ("alaska", 1.1, None, [-148.942, 64.9081, 10.6]),
            ("chile", 4.9, "reviewed", [-70.6202, -21.4265, 52.24]),
            ("hawaii", 2.0099, "automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605]),
            ("indonesia", 4.8, "reviewed", [126.419, 0.2661, 10]),
            ("nevada", 0.5, "automatic", [-116.242, 36.7564, 0.8]),
            ("arkansas", 1.9, "reviewed", [-91.4295, 35.863, 16.41]),
            ("montana", 1.33, "reviewed", [-110.434, 44.4718333, 2.21]),
            ("oklahoma", 1.58, "reviewed", [-98.53233333, 36.57083333, 6.31]),
            ("idaho", 2.6, "reviewed", [-115.186, 44.2666, 10])
        ], ["place", "mag", "status", "coordinates"]
        ).columns

        self.assertEqual(current_result, expected_result)

    def test_should_remove_two_columns_from_dataframe(self):
        transformation = Transformation(self.input_df)
        transformation.drop(['coordinates', 'alert'])

        current_result = transformation.dataframe.columns
        expected_result = self.spark.createDataFrame([
            ("california", 0.82, "automatic"),
            ("alaska", 1.1, None),
            ("chile", 4.9, "reviewed"),
            ("hawaii", 2.0099, "automatic"),
            ("indonesia", 4.8, "reviewed"),
            ("nevada", 0.5, "automatic"),
            ("arkansas", 1.9, "reviewed"),
            ("montana", 1.33, "reviewed"),
            ("oklahoma", 1.58, "reviewed"),
            ("idaho", 2.6, "reviewed")
        ], ["place", "mag", "status"]
        ).columns

        self.assertEqual(current_result, expected_result)


if __name__ == '__main__':
    unittest.main()

import unittest
import logging
from pyspark.sql import SparkSession
from src.EtlPipeline import Transformation
from pyspark.sql.types import DateType, IntegerType, ArrayType, StringType


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
        cls.test_data = cls.spark.createDataFrame([
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
        transformation = Transformation(self.test_data)
        transformation.to_lowercase([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_have_same_df_when_column_not_exist_in_dataframe(self):
        transformation = Transformation(self.test_data)
        transformation.to_lowercase(['xpto'])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_transform_one_column_in_lowercase(self):
        transformation = Transformation(self.test_data)
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
        transformation = Transformation(self.test_data)
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
        transformation = Transformation(self.test_data)
        transformation.drop([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_return_same_df_when_column_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.drop(['xpto'])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_remove_one_column_from_dataframe(self):
        transformation = Transformation(self.test_data)
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
        transformation = Transformation(self.test_data)
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


class TestRenameColumns(PySparkTest):

    def test_should_return_same_columns_when_column_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.rename_column({})

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_columns_when_column_not_exist_in_df(self):
        transformation = Transformation(self.test_data)
        transformation.rename_column(
            {
                "dt": "date"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_replace_one_column_name(self):
        transformation = Transformation(self.test_data)
        transformation.rename_column(
            {
                "mag": "magnitude"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = ["place", "magnitude", "status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)

    def test_should_replace_two_columns_name(self):
        transformation = Transformation(self.test_data)
        transformation.rename_column(
            {
                "mag": "magnitude",
                "status": "new_status"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = ["place", "magnitude", "new_status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)

    def test_should_replace_two_colums_names_when_one_column_name_not_exists(self):
        tranformation = Transformation(self.test_data)
        tranformation.rename_column(
            {
                "mag": "magnitude",
                "status": "new_status",
                "dt": "date"
            }
        )

        current_result = tranformation.dataframe.columns
        expected_result = ["place", "magnitude", "new_status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)


class TestReplaceNullValues(PySparkTest):

    def test_should_return_same_df_when_columns_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.replace_null_values({})

        current_result = transformation.dataframe.collect()
        expected_result = self.test_data.collect()

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_df_when_columns_not_exists_in_df(self):
        transformation = Transformation(self.test_data)
        transformation.replace_null_values(
            {
                "magnitude": 0
            }
        )

        current_result = transformation.dataframe.collect()
        expected_result = self.test_data.collect()

        self.assertEqual(current_result, expected_result)

    def test_should_replace_null_values_from_one_column(self):
        transformation = Transformation(self.test_data)
        transformation.replace_null_values(
            {
                "status": "Automatic"
            }
        )

        current_result = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
                ("California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
                ("Alaska", 1.1, "Automatic", [-148.942, 64.9081, 10.6], "green"),
                ("Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
                ("Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
                ("Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
                ("Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
                ("Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
                ("Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
                ("Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
                ("Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
                ], ["place", "mag", "status", "coordinates", "alert"]
            ).collect()

        self.assertEqual(current_result, expected_result)

    def test_should_replace_null_values_from_two_columns(self):
        transformation = Transformation(self.test_data)
        transformation.replace_null_values(
            {
                "status": "Automatic",
                "alert": "green"
            }
        )

        current_result = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
                ("California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], "green"),
                ("Alaska", 1.1, "Automatic", [-148.942, 64.9081, 10.6], "green"),
                ("Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], "green"),
                ("Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
                ("Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
                ("Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], "green"),
                ("Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
                ("Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], "green"),
                ("Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], "green"),
                ("Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
                ], ["place", "mag", "status", "coordinates", "alert"]
            ).collect()

        self.assertEqual(current_result, expected_result)


class TestColumnDataTypeTransformation(PySparkTest):

    def test_should_return_same_df_when_columns_params_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type({})

        current_result = transformation.dataframe.dtypes
        expected_result = self.test_data.dtypes

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_df_when_columns_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type(
            {
                "date": DateType()
            }
        )

        current_result = transformation.dataframe.dtypes
        expected_result = self.test_data.dtypes

        self.assertEqual(current_result, expected_result)

    def test_should_convert_data_type_one_column(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type(
            {
                "mag": IntegerType()
            }
        )

        current_result = transformation.dataframe.dtypes
        expected_result = [('place', 'string'),
                           ('mag', 'int'),
                           ('status', 'string'),
                           ('coordinates', 'array<double>'),
                           ('alert', 'string')
                           ]

        self.assertEqual(current_result, expected_result)

    def test_should_convert_data_type_two_columns(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type(
            {
                "mag": IntegerType(),
                "coordinates": ArrayType(StringType())
            }
        )

        current_result = transformation.dataframe.dtypes
        expected_result = [('place', 'string'),
                           ('mag', 'int'),
                           ('status', 'string'),
                           ('coordinates', 'array<string>'),
                           ('alert', 'string')
                           ]

        self.assertEqual(current_result, expected_result)

    def test_should_convert_data_type_two_columns_when_one_column_name_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type(
            {
                "mag": IntegerType(),
                "coordinates": ArrayType(StringType()),
                "date": DateType()
            }
        )

        current_result = transformation.dataframe.dtypes
        expected_result = [('place', 'string'),
                           ('mag', 'int'),
                           ('status', 'string'),
                           ('coordinates', 'array<string>'),
                           ('alert', 'string')
                           ]

        self.assertEqual(current_result, expected_result)


if __name__ == '__main__':
    unittest.main()

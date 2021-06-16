import unittest
from src.Pipeline import Transformation
from pyspark.sql.types import DateType, IntegerType, ArrayType, StringType, TimestampType
from tests.PySparkSetup import PySparkSetup


class SetupLowercaseTransformation(PySparkSetup):

    def test_should_have_same_df_when_column_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.lowercase([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_have_same_df_when_column_not_exist_in_dataframe(self):
        transformation = Transformation(self.test_data)
        transformation.lowercase(["xpto"])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_transform_one_column_in_lowercase(self):
        transformation = Transformation(self.test_data)
        transformation.lowercase(["place"])

        transformed_df = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            (1704567252, "california", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
             "yellow"),
            (1224994646, "indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
            (1262739669, "arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(transformed_df, expected_result)

    def test_should_transform_many_colums_in_lowercase(self):
        transformation = Transformation(self.test_data)
        transformation.lowercase(["place", "status"])

        transformed_df = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            (1704567252, "california", 0.82, "automatic", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "chile", 4.9, "reviewed", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "hawaii", 2.0099, "automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
             "yellow"),
            (1224994646, "indonesia", 4.8, "reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "nevada", 0.5, "automatic", [-116.242, 36.7564, 0.8], None),
            (1262739669, "arkansas", 1.9, "reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "montana", 1.33, "reviewed", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "oklahoma", 1.58, "reviewed", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "idaho", 2.6, "reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(transformed_df, expected_result)


class SetupDropColumns(PySparkSetup):

    def test_should_have_same_df_when_column_list_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.drop([])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_return_same_df_when_column_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.drop(["xpto"])

        transformed_df = transformation.dataframe.collect()

        self.assertEqual(transformed_df, self.test_data.collect())

    def test_should_remove_one_column_from_dataframe(self):
        transformation = Transformation(self.test_data)
        transformation.drop(["alert"])

        current_result = transformation.dataframe.columns
        expected_result = self.spark.createDataFrame([
            (1704567252, "California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04]),
            (1391707828, "Alaska", 1.1, None, [-148.942, 64.9081, 10.6]),
            (1435498694, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24]),
            (1609879110, "Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605]),
            (1224994646, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10]),
            (1801059964, "Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8]),
            (1262739669, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41]),
            (1890118874, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21]),
            (1025727100, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31]),
            (1834567116, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10])
        ], ["date", "place", "mag", "status", "coordinates"]
        ).columns

        self.assertEqual(current_result, expected_result)

    def test_should_remove_two_columns_from_dataframe(self):
        transformation = Transformation(self.test_data)
        transformation.drop(["coordinates", "alert"])

        current_result = transformation.dataframe.columns
        expected_result = self.spark.createDataFrame([
            (1704567252, "California", 0.82, "Automatic"),
            (1391707828, "Alaska", 1.1, None),
            (1435498694, "Chile", 4.9, "Reviewed"),
            (1609879110, "Hawaii", 2.0099, "Automatic"),
            (1224994646, "Indonesia", 4.8, "Reviewed"),
            (1801059964, "Nevada", 0.5, "Automatic"),
            (1262739669, "Arkansas", 1.9, "Reviewed"),
            (1890118874, "Montana", 1.33, "Reviewed"),
            (1025727100, "Oklahoma", 1.58, "Reviewed"),
            (1834567116, "Idaho", 2.6, "Reviewed")
        ], ["date", "place", "mag", "status"]
        ).columns

        self.assertEqual(current_result, expected_result)


class SetupRenameColumns(PySparkSetup):

    def test_should_return_same_columns_when_column_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.rename({})

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_columns_when_column_not_exist_in_df(self):
        transformation = Transformation(self.test_data)
        transformation.rename(
            {
                "dt": "date"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_replace_one_column_name(self):
        transformation = Transformation(self.test_data)
        transformation.rename(
            {
                "mag": "magnitude"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = ["date", "place", "magnitude", "status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)

    def test_should_replace_two_columns_name(self):
        transformation = Transformation(self.test_data)
        transformation.rename(
            {
                "mag": "magnitude",
                "status": "new_status"
            }
        )

        current_result = transformation.dataframe.columns
        expected_result = ["date", "place", "magnitude", "new_status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)

    def test_should_replace_two_colums_names_when_one_column_name_not_exists(self):
        tranformation = Transformation(self.test_data)
        tranformation.rename(
            {
                "mag": "magnitude",
                "status": "new_status",
                "dt": "date"
            }
        )

        current_result = tranformation.dataframe.columns
        expected_result = ["date", "place", "magnitude", "new_status", "coordinates", "alert"]

        self.assertEqual(current_result, expected_result)


class SetupReplaceNullValues(PySparkSetup):

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
            (1704567252, "California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "Alaska", 1.1, "Automatic", [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
             "yellow"),
            (1224994646, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
            (1262739669, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
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
            (1704567252, "California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], "green"),
            (1391707828, "Alaska", 1.1, "Automatic", [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], "green"),
            (1609879110, "Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
             "yellow"),
            (1224994646, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], "green"),
            (1262739669, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], "green"),
            (1025727100, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], "green"),
            (1834567116, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(current_result, expected_result)


class SetupColumnDataTypeTransformation(PySparkSetup):

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
                "dt": DateType()
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
        expected_result = [("date", "bigint"),
                           ("place", "string"),
                           ("mag", "int"),
                           ("status", "string"),
                           ("coordinates", "array<double>"),
                           ("alert", "string")
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
        expected_result = [("date", "bigint"),
                           ("place", "string"),
                           ("mag", "int"),
                           ("status", "string"),
                           ("coordinates", "array<string>"),
                           ("alert", "string")
                           ]

        self.assertEqual(current_result, expected_result)

    def test_should_convert_data_type_two_columns_when_one_column_name_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.convert_data_type(
            {
                "mag": IntegerType(),
                "coordinates": ArrayType(StringType()),
                "date": TimestampType()
            }
        )

        current_result = transformation.dataframe.dtypes
        expected_result = [("date", "timestamp"),
                           ("place", "string"),
                           ("mag", "int"),
                           ("status", "string"),
                           ("coordinates", "array<string>"),
                           ("alert", "string")
                           ]

        self.assertEqual(current_result, expected_result)


class SetupSpitColumnContent(PySparkSetup):

    def test_should_return_same_df_when_columns_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.split_content("", [])

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_df_when_column_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.split_content("time", ["day", "month", "year"])

        current_result = transformation.dataframe.columns
        expected_result = self.test_data.columns

        self.assertEqual(current_result, expected_result)

    def test_should_split_column_content_into_three_new_columns(self):
        transformation = Transformation(self.test_data)
        transformation.split_content("coordinates", ["longitude", "latitude", "depth"])

        current_result = transformation.dataframe.columns
        expected_result = ["date", "place", "mag", "status", "longitude", "latitude", "depth", "alert"]

        self.assertCountEqual(current_result, expected_result)


class SetupReplaceColumnContent(PySparkSetup):

    def test_should_return_same_df_when_columns_param_is_empty(self):
        transformation = Transformation(self.test_data)
        transformation.replace_content("mag", {})

        current_result = transformation.dataframe.collect()
        expected_result = self.test_data.collect()

        self.assertEqual(current_result, expected_result)

    def test_should_return_same_df_when_column_not_exists(self):
        transformation = Transformation(self.test_data)
        transformation.replace_content("xpto", {"value1": "value2"})

        current_result = transformation.dataframe.collect()
        expected_result = self.test_data.collect()

        self.assertEqual(current_result, expected_result)

    def test_should_replace_one_value_from_one_column(self):
        transformation = Transformation(self.test_data)
        transformation.replace_content("status", {"Automatic": "auto"})

        current_result = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            (1704567252, "California", 0.82, "auto", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "Hawaii", 2.0099, "auto", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
            (1224994646, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "Nevada", 0.5, "auto", [-116.242, 36.7564, 0.8], None),
            (1262739669, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(current_result, expected_result)

    def test_should_replace_two_values_from_one_column(self):
        transformation = Transformation(self.test_data)
        transformation.replace_content("status", {"Automatic": "auto", "Reviewed": "rev"})

        current_result = transformation.dataframe.collect()
        expected_result = self.spark.createDataFrame([
            (1704567252, "California", 0.82, "auto", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "Chile", 4.9, "rev", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "Hawaii", 2.0099, "auto", [-155.429000854492, 19.2180004119873, 33.2999992370605], "yellow"),
            (1224994646, "Indonesia", 4.8, "rev", [126.419, 0.2661, 10], "green"),
            (1801059964, "Nevada", 0.5, "auto", [-116.242, 36.7564, 0.8], None),
            (1262739669, "Arkansas", 1.9, "rev", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "Montana", 1.33, "rev", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "Oklahoma", 1.58, "rev", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "Idaho", 2.6, "rev", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        ).collect()

        self.assertEqual(current_result, expected_result)


if __name__ == "__main__":
    unittest.main()

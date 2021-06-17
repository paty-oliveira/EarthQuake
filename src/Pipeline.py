from pyspark.sql.functions import lower, regexp_replace
from pyspark.sql import SparkSession, functions as F
from box import Box
import yaml
import requests
import abc
import datetime
import json


class Input(metaclass=abc.ABCMeta):

    @classmethod
    def get_data(cls):
        pass


class ApiInput(Input):

    def __init__(self, url):
        self.url = url

    def get_data(self):
        try:
            response = requests.get(self.url)

            if response.status_code == 200:
                return response

        except Exception as error:
            print("API call with the following error: ", error)


class Storage(metaclass=abc.ABCMeta):

    @classmethod
    def save(cls, data):
        pass


class CsvStorage(Storage):

    def __init__(self, filepath):
        self.filepath = filepath

    def save(self, dataframe):
        dataframe.toPandas().to_csv(self.filepath,
                                    header=True,
                                    index=False)


class Extraction:

    def __init__(self, input: Input):
        self.input = input
        self.data = None

    def extract(self):
        self.data = self.input.get_data()


class Transformation:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def convert_data_type(self, columns: dict):
        return self.__process(columns,
                              lambda df, column, data_type: df.withColumn(column, df[column].cast(data_type)))

    def drop(self, columns: list):
        self.dataframe = self.dataframe.drop(*columns)

    def lowercase(self, columns: list):
        for column in columns:
            if column in self.dataframe.columns:
                self.dataframe = self.dataframe \
                    .withColumn(column, lower(self.dataframe[column]))
            else:
                return self

    def rename(self, columns: dict):
        return self.__process(columns,
                              lambda df, curr_column, new_column: df.withColumnRenamed(curr_column, new_column))

    def replace_content(self, column: str, to_replace: dict):
        if column in self.dataframe.columns:
            for old_content, new_content in to_replace.items():
                self.dataframe = self.dataframe \
                    .withColumn(column, regexp_replace(self.dataframe[column], old_content, new_content))
        else:
            return self

    def replace_null_values(self, columns: dict):
        return self.__process(columns, lambda df, column, value: df.fillna(columns))

    def split_content(self, current_column: str, new_columns: list):
        if current_column in self.dataframe.columns:
            self.dataframe = self.dataframe \
                .select("*", *[self.dataframe[current_column][index].alias(new_columns[index])
                               for index in range(len(new_columns))]) \
                .drop(current_column)
        else:
            return self

    def __process(self, columns: dict, transformation_fn):
        for key, value in columns.items():
            if key in self.dataframe.columns:
                self.dataframe = transformation_fn(self.dataframe, key, value)
            else:
                return self


class Loading:

    def __init__(self, storage: Storage):
        self.storage = storage

    def load(self, dataframe):
        self.storage.save(dataframe)


def run():
    # Update pipeline configurations
    with open("configs/pipeline-config.yaml", "r") as config_file:
        configs = Box(yaml.safe_load(config_file))

    current_date = datetime.datetime.today().strftime("%Y-%m-%d")
    url = configs.url.format(current_date=current_date)

    # Initialization of Spark Session
    spark = SparkSession \
        .builder \
        .master(configs.spark.master) \
        .getOrCreate()

    # Data Extraction from API
    api_input = ApiInput(url)
    extraction_step = Extraction(api_input)
    extraction_step.extract()
    api_response = extraction_step.data.json()

    # Creation of raw dataframe
    rdd = spark.sparkContext.parallelize([json.dumps(api_response)])
    raw_df = spark.read \
        .option("multiline", "true") \
        .option("mode", "PERMISSIVE") \
        .json(rdd)

    raw_df = raw_df.withColumn("Exp_RESULTS", F.explode(F.col("features"))) \
        .drop("features") \
        .select("Exp_RESULTS.geometry.coordinates",
                "Exp_RESULTS.id",
                "Exp_RESULTS.properties.*")

    # Data Transformation
    transformation_step = Transformation(raw_df)
    transformation_step.drop(configs.dataframe.transformation.to_drop)
    transformation_step.rename(configs.dataframe.transformation.to_rename)
    transformation_step.replace_null_values(configs.dataframe.transformation.null_values)
    transformation_step.convert_data_type(configs.dataframe.transformation.data_types)
    transformation_step.replace_content(configs.dataframe.transformation.to_replace.column,
                                        configs.dataframe.transformation.to_replace.content)
    transformation_step.split_content(configs.dataframe.transformation.to_split.column,
                                      configs.dataframe.transformation.to_split.content)
    transformed_df = transformation_step.dataframe

    # Load transformed data into CSV file
    csv_storage = CsvStorage(configs.filepath.transformed_data)
    loading_process = Loading(csv_storage)
    loading_process.load(transformed_df)

from src.Input import Input
from src.Output import Output
from pyspark.sql.functions import lower


class Extraction:

    def __init__(self, input: Input, output: Output):
        self.input = input
        self.output = output

    def extract(self):
        data = self.input.get_data()
        self.output.load_data(data)


class Transformation:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def drop(self, columns: list):
        self.dataframe = self.dataframe.drop(*columns)

    def to_lowercase(self, columns: list):
        for column in columns:
            if column in self.dataframe.columns:
                self.dataframe = self.dataframe \
                    .withColumn(column, lower(self.dataframe[column]))
            else:
                return self

    def rename_column(self, columns: dict):
        for current_name, new_name in columns.items():
            if current_name in self.dataframe.columns:
                self.dataframe = self.dataframe\
                    .withColumnRenamed(current_name, new_name)
            else:
                return self

    def replace_null_values(self, columns: dict):
        for column_name, value in columns.items():
            if column_name in self.dataframe.columns:
                self.dataframe = self.dataframe.fillna(columns)
            else:
                return self

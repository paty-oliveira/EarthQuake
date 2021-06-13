from pyspark.sql.functions import lower, regexp_replace
import requests
import abc


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


class Output(metaclass=abc.ABCMeta):

    @classmethod
    def load(cls, data):
        pass


class Repository(metaclass=abc.ABCMeta):

    @classmethod
    def save(cls, data):
        pass


class Extraction:

    def __init__(self, input: Input, output: Output):
        self.input = input
        self.output = output

    def extract(self):
        data = self.input.get_data()
        self.output.load(data)


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

    def __init__(self, repository: Repository):
        self.repository = repository

    def load(self, dataframe):
        self.repository.save(dataframe)

from src.Input import Input, ApiInput
from src.Output import Output, JsonOutput


class Extraction:

    def __init__(self, input: Input, output: Output):
        self.input = input
        self.output = output

    def extract(self):
        data = self.input.get_data()
        self.output.load_data(data)


class Transformation:

    def transform(self):
        pass


class Loading:

    def load(self):
        pass


def run(configurations):
    url = configurations.get("URL")
    raw_data_filepath = configurations.get("raw_data_filepath")

    api_input = ApiInput(url)
    json_output = JsonOutput(raw_data_filepath)

    Extraction(api_input, json_output).extract()
    # Transformation.convert_data_type()
    # Transformation.split_columns()
    # Loading().load()

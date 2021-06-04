from src.Input import Input, ApiInput
from src.Output import Output, JsonOutput
from datetime import datetime


class Extraction:

    def __init__(self, input: Input, output: Output):
        self.input = input
        self.output = output

    def extract(self):
        data = self.input.get_data()
        self.output.load_data(data)


class Transformation:

    def __init__(self, data):
        self.data = data


class Loading:

    def load(self):
        pass


def run():
    current_date = datetime.today().strftime("%Y-%m-%d")
    pipeline_configurations = {
        "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={current_date}".format(
            current_date=current_date),
        "raw_data_filepath": "data/raw/earthquake_raw.json"
    }

    api_input = ApiInput(
        pipeline_configurations.get("URL")
    )
    json_output = JsonOutput(
        pipeline_configurations.get("raw_data_filepath")
    )

    Extraction(api_input, json_output).extract()
    # Transformation.convert_data_type()
    # Transformation.split_columns()
    # Loading().load()

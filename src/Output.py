import json
import abc


class Output(metaclass=abc.ABCMeta):

    @classmethod
    def load_data(cls, data):
        pass


class JsonOutput(Output):

    def __init__(self, output):
        self.output = output

    def load_data(self, data):
        data = data.json()

        with open(self.output, 'w', encoding='utf-8') as output_file:
            return json.dump(data, output_file, ensure_ascii=False)

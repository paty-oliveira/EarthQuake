from src.Input import Input, ApiInput
from src.Output import Output


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

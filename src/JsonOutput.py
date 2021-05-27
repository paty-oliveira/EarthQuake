import json


class JsonOutput:

    def __init__(self, data, output):
        self.data = data
        self.output = output

    def save(self):
        with open(self.output, 'w') as output_file:
            json.dump(self.data, output_file)

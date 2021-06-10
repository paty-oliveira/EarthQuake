from src.Input import Input
from src.Output import Output
from src.Pipeline import Extraction
import unittest


class FakeInput(Input):
    def __init__(self, data):
        self.data = data

    def get_data(self):
        return self.data


class FakeOutput(Output):
    def __init__(self):
        self.data = None

    def load_data(self, data):
        self.data = data


class TestExtractionProcess(unittest.TestCase):

    def test_is_input_equals_to_output_data(self):
        # Given
        input_data = {"user": "ana",
                      "age": "26",
                      "company": "google"}
        fake_input = FakeInput(input_data)
        fake_output = FakeOutput()

        # When
        extraction = Extraction(fake_input, fake_output)
        extraction.extract()

        # Then
        self.assertEqual(fake_output.data, input_data)


if __name__ == '__main__':
    unittest.main()

from src.Input import Input
from src.Output import Output
import unittest


class FakeInput(Input):
    def __init__(self):
        self.data = {"user": "ana",
                     "age": "26",
                     "company": "google"}

    def get_data(self):
        return self.data


class FakeOutput(Output):

    def load_data(self, data):
        return data


class TestExtractionProcess(unittest.TestCase):

    def test_is_input_equals_to_output_data(self):
        fake_input_data = FakeInput().get_data()
        fake_output_data = FakeOutput().load_data(fake_input_data)

        self.assertEqual(fake_input_data, fake_output_data)


if __name__ == '__main__':
    unittest.main()

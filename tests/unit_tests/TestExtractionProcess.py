from src.Pipeline import Extraction, Input
import unittest


class FakeInput(Input):
    def __init__(self, input):
        self.data = input

    def get_data(self):
        return self.data


class TestExtractionProcess(unittest.TestCase):

    def test_is_input_equals_to_output_data(self):
        input_data = {"user": "ana",
                      "age": "26",
                      "company": "google"}
        fake_input = FakeInput(input_data)

        extraction = Extraction(fake_input)
        extraction.extract()

        current_result = extraction.data
        expected_result = fake_input.data

        self.assertEqual(current_result, expected_result)


if __name__ == '__main__':
    unittest.main()

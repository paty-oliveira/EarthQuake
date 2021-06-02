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

        except:
            raise Exception("API call with the following error: ")

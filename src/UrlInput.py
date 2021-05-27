import requests


class UrlInput:

    def __init__(self, url):
        self.url = url

    def get_data(self):
        try:
            response = requests.get(self.url)

            if response.status_code == 200:
                return response.json()

        except:
            raise Exception("API call with the following error: ")

from config import API_URL
import requests
import json
import os


def extraction(url):
    try:
        result = requests.get(url)

        if result.status_code == 200:
            json_data = result.json()
            directory_path = os.path.abspath("data/")
            file_name = "earthquake_raw.json"
            filepath = os.path.join(directory_path, file_name)

            with open(filepath, 'w') as output_file:
                json.dump(json_data, output_file)

    except:
        raise Exception("API call with errors")


if __name__ == "__main__":
    extraction(API_URL)

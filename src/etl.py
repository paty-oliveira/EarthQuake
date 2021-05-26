from config import API_URL
from datetime import datetime
import requests
import json
import os


def extraction(url):
    current_date = datetime.today().strftime("%Y-%m-%d")

    try:
        result = requests.get(
            url.format(current_date=current_date)
        )

        if result.status_code == 200:
            json_data = result.json()
            directory_path = os.path.abspath("data/raw/")
            file_name = "earthquake_raw.json"
            filepath = os.path.join(directory_path, file_name)

            with open(filepath, 'w') as output_file:
                json.dump(json_data, output_file)

    except:
        raise Exception("API call with errors")


if __name__ == "__main__":
    extraction(API_URL)

from config import API_TOKEN
import requests
import json


def extraction():
    try:
        result = requests.get(f"https://trefle.io/api/v1/plants?token={API_TOKEN}")

        if result.status_code == 200:
            json_data = result.json()
            file_name = "data/trefle_raw_data.json"

            with open(file_name, "w") as output_file:
                json.dump(json_data, output_file)

    except:
        raise Exception("API call with errors")


if __name__ == "__main__":
    extraction()

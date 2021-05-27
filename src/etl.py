from src.UrlInput import UrlInput
from src.JsonOutput import JsonOutput
from datetime import datetime


def ingestion(input, output):
    response = UrlInput(input).get_data()
    output = JsonOutput(response, output).save()


if __name__ == "__main__":
    current_date = datetime.today().strftime("%Y-%m-%d")
    input = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={current_date}".\
        format(current_date=current_date)

    output = "data/raw/earthquake_raw.json"
    ingestion(input, output)

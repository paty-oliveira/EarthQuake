from datetime import datetime

CURRENT_DATE = datetime.today().strftime("%Y-%m-%d")

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={current_date}".format(
    current_date=CURRENT_DATE)

RAW_DATA_FILEPATH = "data/raw/earthquake_raw.json"


CONFIGURATIONS = {
    "URL": API_URL,
    "raw_data_filepath": RAW_DATA_FILEPATH
}
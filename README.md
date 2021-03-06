[![CircleCI](https://circleci.com/gh/paty-oliveira/EarthQuake.svg?style=shield)](https://app.circleci.com/pipelines/github/paty-oliveira/EarthQuake)

# EarthQuake

The goal of this project is the building of a data pipeline to get information about Earthquake all around the world.
This project follows the traditional components of a data engineering pipelines such as:
    
* Data Ingestion
* Data Transformation
* Data Loading
* Workflow Management
* Automatic Testing of Core functionalities
* Continuous Integration

### Steps
1. [Analysis of raw data coming from Earthquake API](notebooks/data_analysis.ipynb)
2. Extraction of raw data from [USGS Earthquake Hazards Program API](https://earthquake.usgs.gov/fdsnws/event/1/)
3. Raw data transformation
4. Load transformed data

The philosophy of Test-Driven Development is applied during the building of the data pipeline. 
The test framework used is unittest.

### Technologies
- Apache Spark
- Apache Airflow
- CircleCI
- Docker
- Jupyter Notebook
- Python

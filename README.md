# oci-functions

This repository contains Python code that serves as the logic for serverless functions on Oracle Cloud Infastructure's Functions service and additional scripts related to those projects.

## Breakdown of files

## Python files
weather_from_OS_FINAL.py
* Code logic that was used to create the serverless function on Oracle's Function service

weather-upload.py
* Code that can be executed locally to retrieve weather data from specified date range at a specific location
* System arguments to be provided when executing this code are: Longitude, Latitude, start of date range, end of date range, hourly or daily weather data, and the name of the Oracle object storage bucket you want to upload this data to

### Datasets
supermarket_sales.csv
* The input dataset that was ennriched with third-party weather data using the code logic in weather_from_OS_FINAL.py
* This dataset includes the required input columns: Latitude, Longitude, City, Date

Austin_df.csv, Newark_df.csv, Denver_df.csv
* Weather data for each location from the supermarket_sales.csv dataset.
* This data was automatically retrieved from the meteostat API and uploaded to Oracle Object Storage via weather-upload.py



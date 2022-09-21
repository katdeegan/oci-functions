# Import required libraries
from tkinter.font import names
import oci
import os
import io
import sys
import pandas as pd
from datetime import datetime
from meteostat import Stations, Daily, Hourly
from pathlib import Path
from oci.config import validate_config
from oci.object_storage import ObjectStorageClient

# Check command line parameters
def check_params():
    # input parameters: <python_file_name> <location_latitude> <location_longitude> <bucket_name> <hourly / daily>
    num_params = 7
    current_num_params = len(sys.argv)

    if (current_num_params < num_params):
        print("\nUsage: weather-upload.py location_latitude location_longitude start_date end_date hourly/daily bucket_name\nDate format: MM-DD-YYYY\n")
        return False
    else:
        print("\nRunning with:")
        print("Location coordinates: {}, {}".format(sys.argv[1], sys.argv[2]))
        print("Start date: {}".format(sys.argv[3]))
        print("End date: {}".format(sys.argv[4]))
        print("Data type: {}".format(sys.argv[5]))
        print("Destination bucket name: {}".format(sys.argv[6]))
        return True

# Main
# Retrieve weather data from specified location, upload data to Oracle Object Storage bucket
def main():
    if check_params():

        # Update with desired location lat/long
        # returns nearby weather station as a Pandas DataFrame object
        lat, long = float(sys.argv[1]), float(sys.argv[2])
        stations = Stations()
        nearby_station = stations.nearby(lat, long).fetch(1)

        # Validate location of nearby weather station
        # TO DO - add timeout check in case no weather station exists
        print("\nRetrieving weather data from {}...".format(nearby_station['name'].values[0]))
        station_id = nearby_station.index.values[0]
        station_name = nearby_station['name'].values[0].split(' ')[0]
    
        # Set the time period (start and end dates)
        # datetime(YYYY, M, D, h, m)
        start_month, start_day, start_year = int(sys.argv[3].split("-")[0]), int(sys.argv[3].split("-")[1]), int(sys.argv[3].split("-")[2])
        end_month, end_day, end_year = int(sys.argv[4].split("-")[0]), int(sys.argv[4].split("-")[1]), int(sys.argv[4].split("-")[2])
        start = datetime(start_year, start_month, start_day)
        end = datetime(end_year, end_month, end_day, 23, 59)

        # Validate that the datetime range is included in the data available for this station
        if ((start < nearby_station['daily_start']) | (end > nearby_station['daily_end'])).bool():
            print("\nWeather data for this location is outside of the requested date range.\n")
            sys.exit(-1)

        # Fetch weather by day or by hour
        # return Pandas Dataframe objects
        if sys.argv[5] == "daily":
            weather = Daily(station_id, start, end).fetch()
        elif sys.argv[5] == "hourly":
            weather = Hourly(station_id, start, end).fetch()

        # Build csv weather file, verify data saved successfully
        # Update directory path and file name
        dir_path = "/Users/kdeegan/Documents/Python/weather-data-to-adw/"
        file_name = "{}_{}_{}_{}_{}_Weather.csv".format(station_name, start_month, start_year, end_month, end_year, sys.argv[5])
        path_name = dir_path + file_name
        print("\nSaving weather data to local file: {}...".format(file_name))
        weather.to_csv(path_name, sep=',')

        # Verify file was created successfully
        if os.path.exists(path_name):
            print("\nWeather data saved successfully.")

        # Upload csv to object store
        # Get config file with key
        config = oci.config.from_file("~/.oci/config")

        # Upload to object storage
        object_storage_client = ObjectStorageClient(config)
        bucket_name = sys.argv[6]
        namespace = object_storage_client.get_namespace().data

        object_storage_client.put_object(namespace, bucket_name, file_name, io.open(os.path.join(Path(dir_path), file_name),'rb'))

        print("\nWeather data successfully uploaded to Object Storage.")

if __name__ == "__main__":
    main()


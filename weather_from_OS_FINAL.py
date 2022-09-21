import io
import os
import json
import logging

import sys
from fdk import response

import oci
import pandas as pd
import numpy as np
import time

funcDefinition = {
    "status": {
        "returnCode": 0,
        "errorMessage": ""
    },
  "funcDescription": {
        "outputs": [
            {"name": "tavg_celcius", "dataType": "number"},
            {"name": "prcp_mm", "dataType": "number"}
        ],
        "parameters": [
            {"name": "Latitude", "displayName": "Latitude",
             "description": "Choose location Latitude column", "required": True,
             "value": {"type": "column"}},
            {"name": "Longitude", "displayName": "Longitude",
             "description": "Choose location Longitude column", "required": True,
             "value": {"type": "column"}},
            {"name": "Date", "displayName": "Date",
             "description": "Choose date column", "required": True,
             "value": {"type": "column"}},
            {"name": "City", "displayName": "City",
             "description": "Choose city column", "required": True,
             "value": {"type": "column"}}

        ],
        "bucketName": "<YOUR OCI BUCKET NAME HERE>",
        "isOutputJoinableWithInput": True
    }
}

def getWeather(sales_df, city, lat, long, date, bucketName):
    # combine Date + Time columns, convert to datetime field
    sales_df['DateTime'] = pd.to_datetime(sales_df[date])
    
    # create dataframe objects with unique location values
    locations_df = sales_df[[city, lat, long]]
    locations_df.drop_duplicates(inplace=True)
    cities = locations_df[city].tolist()

    # retrieve weather data based on supermarket location, date
    # create dict to store weather dfs
    loc_weather_frames = {}
    for city_name in cities:
        ## MODIFY fileName string based on the file naming convention in Object Storage
        fileName = "weather" + city_name + "_df.csv"

        # retrieve location-specific weather CSV file from Object Storage bucket, convert to Pandas Data Frame
        input_csv_path = read_from_objectstore(bucketName, fileName)
        city_df = pd.read_csv(input_csv_path, sep=",", quotechar="\"", encoding="utf-8", parse_dates=True, infer_datetime_format=True)

        # update dictionary
        loc_weather_frames[row[city_name]] = city_df

    # create and populate output temperature and precipitation columns
    temp_vals, prcp_vals = [], []
    for idx, row in sales_df.iterrows():
        city_name = row[city]
        day = str(row['DateTime'].date())
        temp_df = loc_weather_frames[city_name]
        weather_row = temp_df.loc[temp_df.index == day]

        if weather_row.empty:
            temp_vals.append(None)
            prcp_vals.append(None)
        else:
            temp_vals.append(weather_row['tavg'].values[0])
            prcp_vals.append(weather_row['prcp'].values[0])

    sales_df['tavg_celcius'] = temp_vals
    sales_df['prcp_mm'] = prcp_vals

    return sales_df[['tavg_celcius', 'prcp_mm']]



def handler(ctx, data: io.BytesIO = None):
    response_data = ""

    try:
        body = json.loads(data.getvalue())
        funcMode = body.get("funcMode")

        if funcMode == 'describeFunction':
           response_data = json.dumps(funcDefinition)

        elif funcMode == "executeFunction":
            input_method = body.get("input").get("method")
            
            if input_method == "csv":
                bucketName = body.get("input").get("bucketName")
                fileName = body.get("input").get("fileName") + body.get("input").get("fileExtension")
                rowID = body.get("input").get("rowID")
                args = body.get("args")

                # read input data from object store bucket
                input_csv_path = read_from_objectstore(bucketName, fileName)
                df = pd.read_csv(input_csv_path, sep=",", quotechar="\"", encoding="utf-8", parse_dates=True, infer_datetime_format=True)

                # update df
                # retrieve the three column input parameters
                lat, long, date, city = args.get("Latitude"), args.get("Longitude"), args.get("Date"), args.get("City")

                # call getWeather function to retrieve weather columns
                output_dat = getWeather(df, city, lat, long, date, bucketName)

                outputFile = body.get("output").get("fileName") + body.get("output").get("fileExtension")
                output_csv_path  = "/tmp/"+outputFile
                output_dat.to_csv(output_csv_path, index=True, index_label=rowID)
                write_to_objectstore(bucketName, outputFile, output_csv_path)

                os.remove(input_csv_path)
                os.remove(output_csv_path)
                response_data = prepareResponse(bucketName, outputFile)
            else:
                response_data = prepareResponseError("input method not supported: " + input_method)
        else:
            response_data = prepareResponseError("Invalid funcMode: " + funcMode)
    except (Exception, ValueError) as ex:
        response_data = prepareResponseError("Error while executing " + ex)

    return response.Response(
        ctx, response_data,
        headers={"Content-Type": "application/json"}
    )

def prepareResponse(bucketName, outputFile):
    ret_template = """{
        "status": {
            "returnCode": "",
            "errorMessage": ""
            }
        }"""
    ret_json = json.loads(ret_template)
    ret_json["status"]["returnCode"] = 0
    ret_json["status"]["errorMessage"] = ""
    return json.dumps(ret_json)

def prepareResponseError(errorMsg):
    ret_template = """{
        "status": {
            "returnCode": "",
            "errorMessage": ""
            }
        }"""
    ret_json = json.loads(ret_template)
    ret_json["status"]["returnCode"] = -1
    ret_json["status"]["errorMessage"] = errorMsg
    return json.dumps(ret_json)

def get_object(bucketName, objectName):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    try:
        print("Searching for bucket and object", flush=True)
        object = client.get_object(namespace, bucketName, objectName)
        print("found object", flush=True)
        if object.status == 200:
            print("Success: The object " + objectName + " was retrieved with the content: " + object.data.text, flush=True)
            message = object.data.text
        else:
            message = "Failed: The object " + objectName + " could not be retrieved."
    except Exception as e:
        message = "Failed: " + str(e.message)
    return { "content": message }


def read_from_objectstore(bucket_name, file_name):
    try:
        logging.getLogger().info(
            "reading from object storage {}:{}".format(bucket_name, file_name))
        signer = oci.auth.signers.get_resource_principals_signer()
        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
        namespace = object_storage.get_namespace().data
        obj = object_storage.get_object(namespace, bucket_name, file_name)
        file = open('/tmp/'+file_name, "wb")
        for chunk in obj.data.raw.stream(2048 ** 2, decode_content=False):
            file.write(chunk)
        file.close()
        return '/tmp/'+file_name
    except Exception as e:
        print("Error found\n")
        print(e)
        return None

def write_to_objectstore(bucket_name, file_name, source_file):
    logging.getLogger().info("Writing to object storage {}:{}".format(bucket_name, file_name))
    signer = oci.auth.signers.get_resource_principals_signer()
    object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = object_storage.get_namespace().data
    with open(source_file, 'rb') as f:
        obj = object_storage.put_object(namespace, bucket_name, file_name, f)
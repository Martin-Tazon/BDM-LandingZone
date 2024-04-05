###
###     # Not sure if should download files from HDFS or asue they are already in local
###

import pymongo
import json
from glob import glob
import sys
import datetime
import pandas as pd
from hdfs_temporal_landing import *
import shutil

# JSON Formaters:
KNOWN_FORMATS = ["csv", "json"]
# CSV to JSON
def csv_to_json(csv_file):
    lines = []
    df = pd.read_csv(csv_file)
    for i,row in df.iterrows():
        lines.append(row.to_dict())
    return lines 

# JSON to JSON
def json_to_json(json_file):
    with open(json_file) as f:
        return json.load(f)


# PREPARE PERSISTENT LANDING
def add_files_to_db(db, path):
    files = sorted(glob(path + f"/*"))

    for file in files:
        format = file.split(".")[-1]
        if format not in KNOWN_FORMATS:
            raise ValueError(f"Unknown format: {format}")
        fn = file.split("/")[-1].rstrip(format)

        # Create a collection for the CSV file
        current_time = datetime.datetime.now()
        collection = db[fn + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]

        # Insert the JSON data into the collection
        if format == ".csv":
            collection.insert_many(csv_to_json(file))
        elif format == ".json":
            collection.insert_many(json_to_json(file))



if __name__ == "__main__":
    # Set up the connection
    client_mongo = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")
    client_hdfs = InsecureClient('http://10.4.41.44:9870/', user='bdm')

    shutil.rmtree("temporal_local")
    os.mkdir("temporal_local")
    # Access the database
    for source in sources:
        os.mkdir(f"temporal_local/{source}/")
        db = client_mongo[source]

        # Download files from HDFS
        download_source_to_hdfs(client_hdfs, source, "temporal_local/")

        # Uplod to mongo
        path = "temporal_local/" + source + "/"
        add_files_to_db(db, path)

    client_mongo.close()



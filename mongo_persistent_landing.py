###
###     Not sure if should download files from HDFS,
###     asume they are already in local or read them directly form HDFS
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

def csv_to_json(csv_file):
    """
    Convert a CSV file to a list of JSON objects.

    Args:
        csv_file (str): The path to the CSV file.

    Returns:
        list: A list of JSON objects representing the data in the CSV file.
    """
    lines = []
    df = pd.read_csv(csv_file)
    for i, row in df.iterrows():
        lines.append(row.to_dict())
    return lines

def json_to_json(json_file):
    """
    Load JSON data from a file.

    Args:
        json_file (str): The path to the JSON file.

    Returns:
        dict: A dictionary representing the JSON data.
    """
    with open(json_file) as f:
        return json.load(f)

def add_files_to_db(db, path):
    """
    Add files to the MongoDB database.

    Args:
        db (pymongo.database.Database): The MongoDB database.
        path (str): The path to the directory containing the files.

    Raises:
        ValueError: If the file format is unknown.

    """
    files = sorted(glob(path + f"\*"))
    for file in files:
        format = file.split(".")[-1]
        if format not in KNOWN_FORMATS:
            raise ValueError(f"Unknown format: {format}")
        fn = file.split("/")[-1].rstrip(format)

        # Create a collection for the CSV file
        current_time = datetime.datetime.now()
        collection = db[fn + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]
        # Insert the JSON data into the collection
        if format == "csv":
            collection.insert_many(csv_to_json(file))
        elif format == "json":
            if json_to_json(file) == []: continue
            collection.insert_many(json_to_json(file))



if __name__ == "__main__":
    # Set up the connection
    client_mongo = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")
    client_hdfs = InsecureClient('http://10.4.41.44:9870/', user='bdm')

    os.mkdir("temporal_local")
    # Access the database
    for source in sources:
        os.mkdir(f"temporal_local/{source}/")
        db = client_mongo[source]

        # Download files from HDFS
        download_source_from_hdfs(client_hdfs, source, "temporal_local/")

        # Upload to MongoDB
        path = "temporal_local\\" + source + "\\"
        add_files_to_db(db, path)

    client_mongo.close()
    shutil.rmtree("temporal_local")



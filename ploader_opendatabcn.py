import csv
from glob import glob
import json
import pymongo
import datetime
import pandas as pd

# Set up the connection
client = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")

# Access the database
db = client["test"]

def csv_to_json(csv_file):
    lines = []

    df = pd.read_csv(csv_file)
    for i,row in df.iterrows():
        lines.append(row.to_dict())
    return lines 


opendatabcn_path = "../data/opendatabcn-income/"
files = sorted(glob(opendatabcn_path + "*.csv"))

for file in files:
    fn = file.split("/")[-1].split(".")[0]
    print(fn)

    # Create a collection for the CSV file
    current_time = datetime.datetime.now()
    collection = db[fn + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]

    # Insert the JSON data into the collection
    collection.insert_many(csv_to_json(file))

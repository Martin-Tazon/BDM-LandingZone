import csv
from glob import glob
import json
import pymongo
import datetime
import pandas as pd
import sys
import os

def csv_to_json(csv_file):
    lines = []

    df = pd.read_csv(csv_file)
    for i,row in df.iterrows():
        lines.append(row.to_dict())
    return lines 


# opendatabcn_path = "../data/opendatabcn-income/"
# files = sorted(glob(opendatabcn_path + "*.csv"))

def add_file_to_db(path):
    files = sorted(glob(path + "*.csv"))
    for file in files:
        fn = file.split("/")[-1].split(".")[0]
        print(fn)

        # Create a collection for the CSV file
        current_time = datetime.datetime.now()
        collection = db[fn + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]

        # Insert the JSON data into the collection
        collection.insert_many(csv_to_json(file))



def add_key(path):
    files = sorted(glob(path + "*.csv"))
    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(file)
            if 'incremental_ID' not in df.columns:
                df.insert(0, 'incremental_ID', range(0, len(df)))
                df.to_csv(str(file), index= False)


     
if __name__ == "__main__":
    # Set up the connection
    client = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")

    # Access the database
    db_name = sys.argv[1]
    db = client[db_name]
    print(db)

    path = sys.argv[2]
    # add_key(path)
    add_file_to_db(path)

    # Close the connection
    client.close()


# RUN EXAMPLES:
# python ploader_csvs.py "lookups" "data/lookup_tables/"
# python ploader_csvs.py "opendatabcn_income" "data/opendatabcn-income/"
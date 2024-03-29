import pymongo
import json
from glob import glob
import sys
import datetime

# idealista_path = "../data/idealista/"
# files = sorted(glob(idealista_path + "*.json"))

def add_json_to_db(path):
    files = sorted(glob(path + "*.json"))
    for myFile in files:
        date = "-".join(myFile.split("/")[-1].split("_")[0:3])

        current_time = datetime.datetime.now()
        # Create a collection
        # collection = db[f"idealista-{date}" + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]
        collection = db[f"{date}" + "-" + current_time.strftime("%Y-%m-%dT%H:%M:%S")]

        # Read the JSON file
        with open(myFile, "r") as file:
            data = json.load(file)

        # Skip empty files
        if data == []: continue

        # Upload the JSON document to the collection
        collection.insert_many(data)
        print("Uploaded idealista",date)


if __name__ == "__main__":
        
    # Set up the connection
    client = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")
    # print(client.list_database_names())
    # Access the database
    db_name = sys.argv[1]
    db = client[db_name]
    print(db)
    path = sys.argv[2]
    add_json_to_db(path)

    # Close the connection
    client.close()

    # RUN EXAMPLE :
    # python ploader_jsons.py "idealista" "data/idealista/"

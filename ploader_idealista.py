import pymongo
import json
from glob import glob

# Set up the connection
client = pymongo.MongoClient("mongodb+srv://alex_martin:1234@lab1.37jadij.mongodb.net/")

# Access the database
db = client["test"]

idealista_path = "../data/idealista/"
files = sorted(glob(idealista_path + "*.json"))

for myFile in files:
    date = "-".join(myFile.split("/")[-1].split("_")[0:3])

    # Create a collection
    collection = db[f"idealista-{date}"]

    # Read the JSON file
    with open(myFile, "r") as file:
        data = json.load(file)

    # Skip empty files
    if data == []: continue

    # Upload the JSON document to the collection
    collection.insert_many(data)
    print("Uploaded idealista",date)


# Close the connection
client.close()
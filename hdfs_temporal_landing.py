import pandas as pd
from hdfs import InsecureClient
import os
from glob import glob
import sys


def add_csv_to_hdfs(path):
    files = sorted(glob(path + "*.csv"))
    print(files)
    for file in files:
        fn = file.split("\\")[-1]
        path_v = path.split("/")[1]
        dest_path = f'user/bdm/temporal_landing/{path_v}/{fn}'
        
        # Check if the file already exists in HDFS, if so, skip
        if client_hdfs.status(dest_path, strict=False) is not None:
            print(f"File {fn} already exists in HDFS. Skipping...")
            continue

        df = pd.read_csv(file)
        with client_hdfs.write(f'user/bdm/temporal_landing/{path_v}/{fn}', encoding = 'utf-8') as writer:
            df.to_csv(writer)


def add_json_to_hdfs(path):
    files = sorted(glob(path + "*.json"))
    print(files)
    for file in files:
        fn = os.path.basename(file)  # Get the filename without the directory path
        path_v = os.path.basename(os.path.dirname(path))  # Get the second-level directory name
        dest_path = f'user/bdm/temporal_landing/{path_v}/{fn}'
        
        # Check if the file already exists in HDFS, if so, skip
        if client_hdfs.status(dest_path, strict=False) is not None:
            print(f"File {fn} already exists in HDFS. Skipping...")
            continue
        
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()  # Read the content of the JSON file
        with client_hdfs.write(dest_path, encoding='utf-8') as writer:
            writer.write(content)  # Write the content to HDFS

# with client_hdfs.read('user/bdm/temporal_landing/idealista/2020_12_31_idealista.json', encoding = 'utf-8') as reader:
#   df = pd.read_csv(reader,index_col=0)
#   print(df)


def empty_directory_hdfs(directory):
    # Recursively delete all files and subdirectories within the directory
    client_hdfs.delete(directory, recursive=True)

# Specify the directory you want to empty
directory_to_empty = 'user/bdm/temporal_landing/idealista'

# Empty the directory in HDFS
# empty_directory_hdfs(directory_to_empty)


if __name__ == "__main__":

    client_hdfs = InsecureClient('http://10.4.41.44:9870/', user='bdm')

    sources = ['idealista', 'lookup_tables', 'opendatabcn-income']

    for source in sources:
        client_hdfs.makedirs(f'user/bdm/temporal_landing/{source}')

    # Access the database
    path = sys.argv[1]
    format = sys.argv[2]

    if format=='csv':
        add_csv_to_hdfs(path)
    elif format=='json':
        add_json_to_hdfs(path)
    else:
        print('not supported file. give json or csv')


# RUN EXAMPLES:
# python hdfs_temporal_landing.py "data/opendatabcn-income/" "csv"
# python hdfs_temporal_landing.py "data/idealista/" "json" 
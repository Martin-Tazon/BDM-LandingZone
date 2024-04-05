import pandas as pd
from hdfs import InsecureClient
import os
from glob import glob

# To connect to WebHDFS by providing the IP of the HDFS host and the WebHDFS port.
client_hdfs = InsecureClient('http://10.4.41.44:9870/', user='bdm')
print(client_hdfs)

# # To create a simple pandas DataFrame.
# liste_hello = ['hello1','hello2']
# liste_world = ['world1','world2']
# df = pd.DataFrame(data = {'hello' : liste_hello, 'world': liste_world})



def read_local_file(local_path):
    with open(local_path, 'r') as file:
        data = file.read()
    return data

def write_to_hdfs(client, local_path, hdfs_path):
    with open(local_path, 'r') as reader:
        #  print(client, local_path, hdfs_path)
         with client.write(hdfs_path, encoding = 'utf-8') as writer:
            data = reader.read()
            print(data)
            writer.write(data)


sources = ['idealista', 'lookup_tables', 'opendatabcn-income']

for source in sources:
    client_hdfs.makedirs(f'user/bdm/temporal_landing/{source}')
    for file in os.listdir(f'data/{source}/'):
        write_to_hdfs(client_hdfs, f'data/{source}/{file}', f'user/bdm/temporal_landing/{source}/{file}')

    

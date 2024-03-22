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

def add_file_to_db(path):
    files = sorted(glob(path + "*.csv"))
    for file in files:
        fn = file.split("\\")[-1]
        path_v = path.split("/")[1]
        df = pd.read_csv(file)
        with client_hdfs.write(f'user/bdm/temporal_landing/{path_v}/{fn}', encoding = 'utf-8') as writer:
            df.to_csv(writer)

# To write a Dataframe to HDFS.
# with client_hdfs.write('/user/hdfs/wiki/helloworld.csv', encoding = 'utf-8') as writer:
#   df.to_csv(writer)

# with client_hdfs.read('user/bdm/temporal_landing/lookup_tables/income_opendatabcn_extended.csv', encoding = 'utf-8') as reader:
#   df = pd.read_csv(reader,index_col=0)
#   print(df)
        
# add_file_to_db('data/lookup_tables/')
add_file_to_db('data/opendatabcn-income/')
client_hdfs.makedirs('user/bdm/temporal_landing/opendatabcn')
client_hdfs.makedirs('user/bdm/temporal_landing/lookup_tables')
client_hdfs.makedirs('user/bdm/temporal_landing/idealista')
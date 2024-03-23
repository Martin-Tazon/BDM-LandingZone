from hdfs import InsecureClient
import os

sources = ['idealista', 'lookup_tables', 'opendatabcn-income']
path_temporal_hdfs='user/bdm/temporal_landing/'

def ls_hdfs(client, path: str) -> list[str]:
    if not path.startswith('user/bdm/'):
        path = 'user/bdm/' + path
    return client.list(path)

def du_hdfs(client, path: str) -> None:
    if not path.startswith('user/bdm/'):
        path = 'user/bdm/' + path
    content = client.content(path)
    print(f"Size of {path}: {content['length']} bytes")
    if content['directoryCount'] > 0:
        print(f"Path contains {content['directoryCount']} subdirectories.")
        subdirs = ls_hdfs(client,path)
        for sdir in subdirs:
            content_sd = client.content(path+sdir)
            print(f"    {sdir}: {content_sd['length']} bytes")

def rm_hdfs(client, path: str, recursive: bool = True):
    return client.delete(path, recursive=recursive)

def upload_source_to_hdfs(client, source: str) -> None:
    local_path = f"data/{source}/"
    hdfs_path = path_temporal_hdfs + source

    client.makedirs(hdfs_path)
    for f in os.listdir(local_path):
        client.upload(hdfs_path, local_path + f)




if __name__ == "__main__":

    # To connect to WebHDFS by providing the IP of the HDFS host and the WebHDFS port.
    client_hdfs = InsecureClient('http://10.4.41.44:9870/', user='bdm')
    print(client_hdfs)

    # Initialize temporal landing dir
    print("Initializing landing zone ...")
    rm_hdfs(client_hdfs, path_temporal_hdfs)
    client_hdfs.makedirs(path_temporal_hdfs)

    # Upload sources
    for source in sources:
        print("Uploading",source,"...")
        upload_source_to_hdfs(client_hdfs, source)
    print("Upload finished!")

    print("Landgin zone status:")
    du_hdfs(client_hdfs,path_temporal_hdfs)
# BDM-LandingZone

This repository contains the code used to handle the **Landing Zone** of a Data Management backbone for DataOps.
Recall that the structure of the Data Management Backbone consists of:
1. The data sources
2. The **Landing Zone**
3. The Formatted Zone
4. The Exploitation Zone.

The Landing Zone is divided into two distinct zones: Temporal and Persistent.
The first serves as a temporary location to store the data that is to be added to the Big Data System, and the second one is the persistent structure that collects and holds all the data that conform the Big Data system in its most unaltered manner. The other two mentioned zones will read from it and prepare the data to be used by the different Data Analysis backbones.

## Scripts

- `hdfs_temporal_landing.py`:

  Process of adding the data to be ingested to the temporal landing. This translates in uploading the data into an HDFS hosted in a virtual 
  machine.
  The script contains a set of functions to facilitate interaction with the HDFS. This functions are called like the linux commands that would be used in a regular file system followed by `_hdfs`. Note that only the basic functionalities of the commands have been implemented.
  Available commands:
  - ls
  - rm
  - du
  In addition, there are also functions to upload/download a complete source to/from the HDFS.
  The main usage of the script is to upload all sources listed in the sources list. The script accepts one system argument to decide if the temporal landing zone should be cleared before the upload.


- `mongo_persistent_landing.py`:

  Process of uploading the content of the temporal landing into the persistent landing zone. This requires downloading the data from the temporal landing into a temporary local directory, formatting the data as JSON objects and upload those into a MongoDB.
  The script imports the previous one to make use of the download source function. It also includes the function to upload local files to the database as well as a json formatter for each known format of our sources.
  Available formats:
  - json
  - csv
  The script creates a temporary local directory that is deleted at the end of the execution.

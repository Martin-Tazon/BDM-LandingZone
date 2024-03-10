# BDM-LandingZone

Does atlas needs new ips all the time?

## Temporal LZ
- Consists of the directories and files that were given to us in the data.zip ¿?
- For the future "new source" we will have to decide how we extract and store the data.
  
## Persistent LZ
- Upload to mongoDB the different files for each source together with a timestamp
  - Idealista: Upload json objects
  - OpenDataBCN: Parse CSV files into json objects
  - Lookup: ¿? We add in the objects the reconciled district and neighbourhood. Do we keep only the reconciled or the original as well?
  - Is this way of keeping timestamp correct?
  - If a file with the same name exists overwrite it 
  - When the loaders are done with reconciliation, drop database and add them again

# Overview / Purpose
ETL for a startup called Sparkify. Sparkify wants to analyze its data from songs and user activity on their streaming app.
To understand what songs are users listening to we're using user activity logs and song metadata.

# How to Run
The ETL uses conda environments with the latest python version 3.10.
* Create a Redshift Cluster 
* Setup a Database and take note of the Database name, user, password and port
* Make sure the Cluster's VPC is accessible from the computer running this
* Record cluster host and DB params at dwh.cfg at the [CLUSTER] section

* Create a Role that has access to read from S3 'aws:policy/AmazonS3ReadOnlyAccess' and record its ARN at dwh.cfg at the [IAM_ROLE] section

* Make sure [conda environment is active](#_notescmd)

* Run Create tables
    
    `python create_tables.py`

* Run ETL.py
    
    `python etl.py`

* [Run Test Notebook](#testipynb)
    
# File list

## [_notes.cmd](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/_notes.cmd)
Contains command line snippets, most of them to manage the conda environment

- [ ] conda env commands to be relative path
- [ ] conda env to automatically execute when.... 

* [Activate Conda](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L5)
* [Create conda environment from a yml file](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L8)
* [Activate, update, remove conda environment](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L10-L17)

## [create_tables.py](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/create_tables.py)
Python code to execute DDL statements that initialize the sparkify database

## [aws.cfg]()
Config file where we have the 

    - KEY
    - SECRET
    - PREFERRED_REGION

For the aws user (gitIgnored 😉)

![image](https://user-images.githubusercontent.com/11904085/166481419-dd62a376-234c-4a77-913f-ffebd3042ce3.png)


## [dwh.cfg](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/dwh.cfg)
Config file where we have the:

    - Redshift connection string
    - IAM ARN Role to read from S3 buckets
    - S3 buckets URI


## [environment.yml](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/environment.yml)
Environment config. Coontains the project dependencies for creating a conda environment

## [etl_draft.ipynb](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/etl_draft.ipynb)
Notebook for testing each step of the process

## [etl.py](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/etl.py)
ETL processing metadata and events into the songplays datamart.
Since we are working with a powerful database such as Redshift. We are transforming the staging tables into the facts and dimensions using INSERT - SELECT statements. This way we take advantage of the database cloud capabilities.
![image](https://user-images.githubusercontent.com/11904085/166488073-3c4c2d76-851b-4b66-a124-15162ba1e05e.png)
![image](https://user-images.githubusercontent.com/11904085/166488151-0400b170-c7e1-4fe1-b3ca-938a7ea84427.png) 
![image](https://user-images.githubusercontent.com/11904085/166488255-0516510e-8d15-4977-8fb7-49f03038534d.png) 

## [sql_queries.py](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/sql_queries.py)
DDL and DML SQL statements for the ETL

## [test.ipynb](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/test.ipynb)
Notebook querying the data inserted by the ETL

# Database Schema 

## Staging Table `staging_events`
* For staging events, there is character content larger at staging_events.artist than the default varchar default length (256)
![image](https://user-images.githubusercontent.com/11904085/166483856-5a7f2bef-020f-40e2-8e8e-39d6a29755b1.png)

## Staging Table `staging_songs`
* For staging songs, there is character content larger at staging_songs.title, staging_songs.artist_name and staging_songs.artist_location than the default varchar default length (256)
![image](https://user-images.githubusercontent.com/11904085/166484126-8e915a5f-4dd3-4168-89e5-e69c667d17ab.png)


## Fact Table `songplays`
* PK `songplay_id` has an autoincrement int column 
* Songplays fact table will have a distribution style by Key, having as its sort key the timestamp 
* The distribution key will be the song id, which corresponds to the largest dimension. This way songplays along with songs records are allocated in the same cluster
* Included the song title and artist name in the songplays facttable

    - So that an unexisting song at the song catalog/dimension, does not impact stream metrics
    - Avoid joins to artist or songs, just to get the artist name or song title

![image](https://user-images.githubusercontent.com/11904085/166486460-cb214639-dd52-4034-b928-55fdc98affe0.png)

## Dimension Table `songs`
* Songs dimension is the largest dimension
* It has a distribution style by song_id so it can distribute across clusters along with the songplays fact table records
![image](https://user-images.githubusercontent.com/11904085/166486672-921fd462-1f16-402d-bd75-fbb079011b0c.png)

## Dimension Table `users`
* User dimension table will be replicated in all clusters as comparatively has much less data
![image](https://user-images.githubusercontent.com/11904085/166487245-cd0904da-16e7-4176-9770-671fe09cd42f.png)

## Dimension Table  `artists`
* Artists dimension will be replicated in all clusters

![image](https://user-images.githubusercontent.com/11904085/166487403-798d23b2-517e-4a63-9332-f3b720c6360b.png)

## Dimension Table `time` 
* Calendar dimension to be able to query/aggregate easily blocks of time.
* Time dimension will be replicated in all clusters

![image](https://user-images.githubusercontent.com/11904085/166487515-60bd940a-eb28-4d99-84d3-9a8b27d377b7.png) 

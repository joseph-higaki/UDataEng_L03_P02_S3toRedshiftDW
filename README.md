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

*****************************************************************************************************
*****************************************************************************************************
********************************** AQUI ME QUEDÉ ****************************************************
*****************************************************************************************************
*****************************************************************************************************

## [_notes.cmd](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/_notes.cmd)
Contains command line snippets, most of them to manage the conda environment

- [ ] conda env commands to be relative path
- [ ] conda env to automatically execute when.... 

* [Activate Conda](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/69ed9c05d1cd25375997a5780f0d798445c6a4ae/_notes.cmd#L5)
* [Create conda environment from a yml file](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/69ed9c05d1cd25375997a5780f0d798445c6a4ae/_notes.cmd#L8)
* [Activate, update, remove conda environment](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/69ed9c05d1cd25375997a5780f0d798445c6a4ae/_notes.cmd#L10-L17)


## [create_tables.py](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/create_tables.py)
Python code to execute DDL statements that initialize the sparkify database

## [db_connection_config.py](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/db_connection_config.py)
Helper class that provides a PostgreSQL connection string from a config file 

- [ ] I would've liked this to follow a singleton pattern 😪

## [db_connection_config.yml](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/db_connection_config.yml)
Config file 

## [ddl.sql](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/ddl.sql)
DDL statements that initialize the sparkify database

I've created this file to get me going at the beggining, through a SQL console (DataGrip) before I placed the statements on [create_tables.py](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/create_tables.py)


## [environment.yml](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/environment.yml)
Environment config. Coontains the project dependencies for creating a conda environment

* Needed to use python 3.8  as I couldn't make psycopg2 work with 3.9
* I used pyyaml, spend too much time troubleshooting why previously I'd been able to use `pip install yaml` and `conda install yaml` doesn't do the trick
* ipython-sql to be able to execute inline SQL at test.ipynb is not available from conda default channels. I discovered the [channel]::[package] syntax 😊

## [etl.ipynb](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/etl.ipynb)
Notebook for testing each process

## [etl.py](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/etl.py)
ETL processing metadata and events into the songplays datamart

## [sparkifydb queries.sql](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/sparkifydb%20queries.sql)
Scrapbook for queries that are being executed on a PostgreSQL console (DataGrip)

## [sql_queries.py](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/sql_queries.py)
SQL statements for the ETL

## [test.ipynb](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/test.ipynb)
Notebook querying the data inserted by the ETL

# Database Schema 
* I wish I could've been able to have DataGrip build an ERD from my database. I was not succesful at doing this

## Fact Table `songplays`
* PK `songplay_id` has an autoincrement int column 
* Since we're working over a sample of the [million songs dataset](http://millionsongdataset.com/) most of the songs referenced at the stream event log are not at the song/artist metadata files. 
I included he song title and artist name in the songplays facttable
* Length at the event table is being interpreted as stream duration of the song. This would allow us to differenciate the length of the song according to the metadata vs how much odf the song was streamed. We could later answer questions like: how often this song is skipped or streamed partially.
![image](https://user-images.githubusercontent.com/11904085/153720167-477fd2ba-0d26-4d2f-97d5-65374bf091eb.png)

## Dimension Tables `users, songs, artists`
They use a PK from the source system, which will force us to follow a SCD Type 1 for these dimensions. 
No history, just overwrite on changes. 
I believe it is ok for the current scope of the problem.
![image](https://user-images.githubusercontent.com/11904085/153720185-7be954fa-4cc2-434e-abb1-e8479c4d8518.png)

## Dimension Table `time` 
Calendar dimension to be able to query/aggregate easily blocks of time.
![image](https://user-images.githubusercontent.com/11904085/153720199-d36ca5fc-41c4-4b4e-a2b8-d16459058d7b.png)

# Analytical Queries
Some of the questions we can now answer with our star schema model are included as examples within the [test.ipynb](https://github.com/joseph-higaki/UDataEng_L02P01_create-song-play-schema/blob/main/test.ipynb) 

![image](https://user-images.githubusercontent.com/11904085/153720710-153f60b9-bd39-464d-a889-8cdebb62fc0b.png)

![image](https://user-images.githubusercontent.com/11904085/153720725-b3b3562d-8294-4371-8780-93577a4ef1b3.png)

![image](https://user-images.githubusercontent.com/11904085/153720738-5c5dca09-6cfe-4986-a669-f06781955fc2.png)
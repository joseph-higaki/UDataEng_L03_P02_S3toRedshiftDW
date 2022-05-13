# Overview / Purpose
ETL for a startup called Sparkify. Sparkify wants to analyze its data from songs and user activity on their streaming app.
To understand what songs are users listening to we're using user activity logs and song metadata.

# How to Run
The ETL uses conda environments with the latest python version 3.10.
* Create a Redshift Cluster 
* Setup a Database and take note of the Database name, user, password and port
* Make sure the Cluster's VPC is accessible from the computer running this
* Record cluster host and DB params at [dwh.cfg](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/dwh.cfg) at the [CLUSTER] section
* Create a Role that has access to read from S3 'aws:policy/AmazonS3ReadOnlyAccess' and record its ARN at [dwh.cfg](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/dwh.cfg) at the [IAM_ROLE] section

* Make sure [conda environment is active](#_notescmd)

* Run Create tables
    
    `python create_tables.py`

* Run ETL.py
    
    `python etl.py`

* [Run Test Notebook](#testipynb)
    
# File list

## [_notes.cmd](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/_notes.cmd)
Contains command line snippets, most of them to manage the conda environment

* [Activate Conda](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L5)
* [Create conda environment from a yml file](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L8)
* [Activate, update, remove conda environment](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/1f8411efd470df52f82025f42bf81f6bfca5f0b0/_notes.cmd#L10-L17)

## [AWS Cluster Operations](aws_redshift.ipynb)
Contains snippets to Create, Pause, Resume, Delete the Redshift Cluster

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

## [artist_cleaning.ipynb](artist_cleaning.ipynb)
Notebook for analyzing, cleaning artists

## [song_cleaning.ipynb](song_cleaning.ipynb)
Notebook for analyzing, cleaning songs

## [user_cleaning.ipynb](user_cleaning.ipynb)
Notebook for analyzing, cleaning users

## [etl.py](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/etl.py)
ETL processing metadata and events into the songplays datamart.
Since we are working with a powerful database such as Redshift. We are transforming the staging tables into the facts and dimensions using INSERT - SELECT statements. This way we take advantage of the database cloud capabilities.

Example: 

![image](https://user-images.githubusercontent.com/11904085/168319131-6ff60bd7-4ba8-4048-9dc2-14286a1cfe9e.png)

## [sql_queries.py](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/sql_queries.py)
DDL and DML SQL statements for the ETL

## [test.ipynb](https://github.com/joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW/blob/main/test.ipynb)
Notebook querying the data inserted by the ETL

# Raw Staging
Staging tables will have string fields only to have the raw data captured in from the source. 
Any data formatting, data conversion, deduplication or filtering is done when loading the data from staging to the datawarehouse tables

## Raw Staging Table `staging_events`
* For staging events, there is character content larger at staging_events.artist than the default varchar default length (256)

![image](https://user-images.githubusercontent.com/11904085/166483856-5a7f2bef-020f-40e2-8e8e-39d6a29755b1.png)

## Raw Staging Table `staging_songs`
* For staging songs, there is character content larger at staging_songs.title, staging_songs.artist_name and staging_songs.artist_location than the default varchar default length (256)

![image](https://user-images.githubusercontent.com/11904085/166484126-8e915a5f-4dd3-4168-89e5-e69c667d17ab.png)

# Intermediate Staging

## Artists
I went into the rabbit hole of deduplicating artist rows that have a many to many relationship between artist_name and artist_id

![image](https://user-images.githubusercontent.com/11904085/167831693-087bf502-0b37-4031-8021-91319789084a.png)

![image](https://user-images.githubusercontent.com/11904085/167870549-3babc47f-03d4-4011-b06a-73c704e19880.png)

### Intermediate Staging Table `staging_artist_row`
A row from the songs dataset representing an artist, removing duplicates from same artist songs
* Latitude and Longitude are casted into the right data type
* As there are multiple records for the same artist entity, we assign a score (int) for how well recorded is:
    * Latitude
    * Longitude
    * Location

![image](https://user-images.githubusercontent.com/11904085/168320495-55665b9d-5700-48fd-a3ac-c543e91f8af1.png)


### Intermediate Staging Table `staging_artist_id_name`
Represents an artist ID / Name combination

* multiple_name_indicatorand multiple_id_indicator flags are calculated to easily process in phases each set of artist rows

![image](https://user-images.githubusercontent.com/11904085/167831693-087bf502-0b37-4031-8021-91319789084a.png)

![image](https://user-images.githubusercontent.com/11904085/168321013-ade89127-8bed-4b6c-bf6c-e643499c66de.png)

### Intermediate Staging Table `staging_artist_names`
Working table that will become Artist Name dimension
Stores the transformation records in 6 steps
![image](https://user-images.githubusercontent.com/11904085/167870549-3babc47f-03d4-4011-b06a-73c704e19880.png)

![image](https://user-images.githubusercontent.com/11904085/168321606-75313046-f874-429a-8ca7-44b57c7113b9.png)

# Datawarehouse 

## Dimension Table  `artist_names`
* Artists dimension will be replicated in all clusters
* name is the natural primary key and sort key
This is because songplays or streams reference song titles and artists by title/names

![image](https://user-images.githubusercontent.com/11904085/168321783-ffe2b71d-a8a4-436c-b152-c97d5e279bdc.png)

## Dimension Table `song_titles`
Represents a sont title. 
Song id has been disregarded as it adds no value to the analysis
* Song titles dimension is the largest dimension
* It has a distribution style by title so it can distribute across clusters along with the songplays fact table records
* Sort Key is by artist_name and title, as it is the natural composed primary key

![image](https://user-images.githubusercontent.com/11904085/168322322-aa2f398c-3821-467d-bc34-8cee38e1ea9c.png)

## Dimension Table `users`
* User dimension table will be replicated in all clusters as comparatively has much less data

![image](https://user-images.githubusercontent.com/11904085/166487245-cd0904da-16e7-4176-9770-671fe09cd42f.png)

## Dimension Table `time` 
* Calendar dimension to be able to query/aggregate easily blocks of time.
* The time dimension key is a int generated key that represents Year, Month, Day, Hour
* Time dimension will be replicated in all clusters

![image](https://user-images.githubusercontent.com/11904085/168322462-344e45d7-53c8-4ffa-afef-0710b7080f31.png)


## Fact Table `songplays`
* PK `songplay_id` has an autoincrement int column 
* Songplays fact table will have a distribution style by Key, having as its sort key the timestamp 
* The distribution key will be the song title which corresponds to the largest dimension. This way songplays along with songs records are allocated in the same cluster
* start_time_key is the int representation of Year, Month, Day, Hour to link to the time dimension

![image](https://user-images.githubusercontent.com/11904085/168322743-9691b807-1517-47a9-b578-e8d8c8530c81.png)



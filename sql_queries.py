import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songplays;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# STAGING TABLES
# Will have string fields only to have the raw data captured in from the source. 
# Any data formatting, data conversion, deduplication or filtering is done when loading
# the data from staging to the datawarehouse tables

# CREATE TABLES
# For staging events, 
# there is character content larger at staging_events.artist 
# than the default varchar default length (256)
staging_events_table_create= ("""
CREATE TABLE if not exists staging_events 
(
  artist varchar(1000),
  auth varchar,
  firstName varchar,
  gender varchar,
  itemInSession varchar,
  lastName varchar,
  length varchar,
  level varchar,
  location varchar,
  method varchar,
  page varchar,
  registration varchar,
  sessionId varchar,
  song varchar(1000),
  status varchar,
  ts varchar,
  userAgent varchar,
  userId varchar
);
""")

# For staging songs, 
# there is character content larger at staging_songs.title, staging_songs.artist_name and staging_songs.artist_location
# than the default varchar default length (256)
staging_songs_table_create = ("""
CREATE TABLE if not exists staging_songs 
(
  song_id varchar,
  num_songs varchar,
  title varchar(1000), 
  artist_name varchar(1000),
  artist_latitude varchar,
  year varchar,
  duration varchar,
  artist_id varchar,
  artist_longitude varchar,
  artist_location  varchar(1000)
);
""")

# Songplays fact table will have a distribution style by Key
# Having its sort key the timestamp
# The distribution key will be the song id, which corresponds to the largest dimension
# This way songplays along with songs are allocated in the same cluster
songplay_table_create = ("""
create table if not exists songplays
(
    songplay_id int IDENTITY(0,1) primary key,
    start_time timestamp without time zone not null sortkey,
    user_id int not null,
    level varchar not null,
    song_id varchar distkey,
    song_title varchar(1000) not null,
    artist_id varchar,
    artist_name varchar(1000) not null,
    session_id int  not null,
    location varchar(1000) not null,
    user_agent varchar  not null,
    stream_duration decimal 
) diststyle KEY;
""")

# User dimension table will be replicated in all clusters
# as comparatively has much less data
user_table_create = ("""
create table if not exists users
(
    user_id int not null primary key sortkey,
    first_name varchar not null,
    last_name varchar not null,
    gender varchar not null,
    level varchar not null
) diststyle ALL;
""")

# Songs dimension is the largest dimension
# It has a distribution style by song_id 
# so it can distribute across clusters along with the songplays fact table records
song_table_create = ("""
create table if not exists songs
(
    song_id varchar not null primary key distkey,
    title varchar(1000) not null sortkey,
    artist_id varchar not null,
    year int not null,
    duration decimal not null
) diststyle KEY;
""")

# Artists dimension will be replicated in all clusters
artist_table_create = ("""
create table if not exists artists
(
    artist_id varchar not null primary key,
    name varchar(1000) not null sortkey,
    location varchar(1000) null,
    latitude decimal null,
    longitude decimal null
) diststyle ALL;
""")

# Time dimension will be replicated in all clusters
time_table_create = ("""
create table if not exists time
(
  start_time timestamp without time zone not null primary key sortkey,
  hour int not null,
  day int not null,
  week int not null,
  month int not null,
  year int not null,
  day_name varchar not null,
  weekday bool not null
) diststyle ALL;
""")

# STAGING TABLES

staging_events_copy = (f"""
copy staging_events 
from '{config['S3']['LOG_DATA']}' 
iam_role '{config['IAM_ROLE']['ARN']}'
region '{config['S3']['BUCKET_REGION']}'
json '{config['S3']['LOG_JSONPATH']}';
""")

staging_songs_copy = (f"""
copy staging_songs 
from '{config['S3']['SONG_DATA']}' 
iam_role '{config['IAM_ROLE']['ARN']}'
region '{config['S3']['BUCKET_REGION']}'
json 'auto ignorecase';
""")

# FINAL TABLES
# Load first the song and artist tables 
# We will use them to load song and artist id
# into the songplay fact table
song_table_insert = ("""
insert into songs
(song_id,title,artist_id,year,duration)
select 
song_id,
title,
artist_id,
year::int,
duration
from staging_songs;
""")

artist_table_insert = ("""
insert into artists
(artist_id, name, location, latitude, longitude)
select 
artist_id,
artist_name as name,
artist_location as location,
artist_latitude::decimal as latitude,
artist_longitude::decimal as longitude
from staging_songs;
""")

songplay_table_insert = ("""
insert into songplays
(start_time, user_id, level, song_id, song_title, 
artist_id, artist_name, session_id, location, user_agent, stream_duration)
select 
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 Second ' as start_time,
    e.userid::int as user_id,
    e.level,
    s.song_id,
    e.song as song_title,
    s.artist_id,
    e.artist as artist_name,
    e.sessionid::int as session_id,
    e.location,
    e.userAgent as user_agent,
    e.length::decimal as stream_duration
from staging_events e
left join staging_songs s on 
    e.song = s.title 
    and e.artist = s.artist_name
where page = 'NextSong'
""")

user_table_insert = ("""
insert into users
(user_id, first_name, last_name, gender, level)
select distinct
    userid::int as user_id,
    firstname as first_name,
    lastname as last_name,
    gender,
    level
from staging_events e
where page = 'NextSong'
""")

time_table_insert = ("""
insert into time 
(start_time, hour, day, week, month, year, day_name, weekday)
select distinct
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 Second ' as start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year, 
    to_char(start_time, 'Day') as day_name,
    extract(dayofweek from start_time) in (0,6) as weekday
from staging_events e
where page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# Load first song and artist
insert_table_queries = [song_table_insert, artist_table_insert, songplay_table_insert, user_table_insert, time_table_insert]

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES RAW STAGING TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"

# DROP TABLES INTERMEDIATE STAGING TABLES
staging_artist_row_table_drop = "DROP TABLE IF EXISTS staging_artist_row;"
staging_artist_id_name_table_drop = "DROP TABLE IF EXISTS staging_artist_id_name;"
staging_artist_names_table_drop = "DROP TABLE IF EXISTS staging_artist_names;"

# DROP TABLES DWH TABLES

artist_names_table_drop = "drop table if exists artist_names;"
song_titles_table_drop = "drop table if exists song_titles;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
time_table_drop = "drop table if exists time;"

# ********************************************************************
# ************************ RAW STAGING TABLES ************************
# ********************************************************************
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

# *************************************-***************************************
# ************************ INTERMEDIATE STAGING TABLES ************************
# *****************************************************************************

# Represents a row from the songs dataset representing an artist
staging_artist_row_table_create = ("""
CREATE TABLE staging_artist_row 
(   
  artist_id varchar,
  artist_name varchar(1000),
  artist_latitude decimal,  
  artist_latitude_score int,
  artist_longitude decimal,
  artist_longitude_score int,
  artist_lat_long_score int,
  artist_location  varchar(1000),
  artist_location_score int
);
""")

# Represents an ID / Name combination
staging_artist_id_name_table_create = ("""
CREATE TABLE staging_artist_id_name 
(   
  artist_id varchar,
  artist_name varchar(1000),
  multiple_name_indicator int,
  multiple_id_indicator int  
);
""")

# Working table that will become Artist Name dimension
staging_artist_names_table_create = ("""
CREATE TABLE staging_artist_names 
(   
  original_artist_id varchar,
  artist_name varchar(1000),
  recalculated_artist_id varchar,
  artist_latitude decimal,  
  artist_longitude decimal,
  artist_location  varchar(1000),
  multiple_name_indicator int,
  multiple_id_indicator int,
  step int
);
""")

# ***********************************************************
# ************************ DW TABLES ************************
# ***********************************************************
# Songplays fact table will have a distribution style by Key
# Having its sort key the timestamp
# The distribution key will be the song id, which corresponds to the largest dimension
# This way songplays along with songs are allocated in the same cluster



# Artist Names dimension will be replicated in all clusters
artist_names_table_create = ("""
create table if not exists artist_names
(
    name varchar(1000) not null primary key sortkey,
    artist_id varchar not null,        
    latitude decimal null,
    longitude decimal null,
    location varchar(1000) null
) diststyle ALL;
""")

# Songs dimension is the largest dimension
# It has a distribution style by title
# so it can distribute across clusters along with the songplays fact table records
song_titles_table_create = ("""
create table if not exists song_titles
(    
    artist_name varchar(1000) not null,
    title varchar(1000) not null,    
    year int not null,
    duration decimal not null,
    primary key (artist_name, title)    
) 
diststyle KEY
distkey (title)
sortkey (artist_name, title)
;
""")

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
  day_of_week int not null,
  day_of_week_name varchar not null,
  is_weekend bool not null
) diststyle ALL;
""")

# LOADING STAGING TABLES

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

# LOADING INTERMEDIATE STAGING TABLES
staging_artist_row_insert = ("""
insert into staging_artist_row (
    artist_id, 
    artist_name, 
    artist_latitude,
    artist_latitude_score,
    artist_longitude, 
    artist_longitude_score,
    artist_lat_long_score,
    artist_location,
    artist_location_score
)
select distinct
artist_id,
artist_name as name,
artist_latitude::decimal as artist_latitude,
case when artist_latitude ~ '^(([-+]?[0-9]+(\.[0-9]+)?)|([-+]?\.[0-9]+))$' then 1 else 0 end as artist_latitude_score,
artist_longitude::decimal as artist_longitude,
case when artist_longitude ~ '^(([-+]?[0-9]+(\.[0-9]+)?)|([-+]?\.[0-9]+))$' then 1 else 0 end as artist_longitude_score,
artist_latitude_score + artist_longitude_score as artist_lat_long_score, 
artist_location as artist_location,
case when not trim(artist_location) = '' then 1 else 0 end as artist_location_score
from staging_songs;
""")

staging_artist_id_name_insert = ("""
insert into staging_artist_id_name 
( artist_id, artist_name, multiple_name_indicator, multiple_id_indicator)
with artists as (
        select distinct 
        artist_id,
        artist_name
        from staging_artist_row 
    ),
artists_multiple_names as (    
    select 
        artist_id,
        count(1) as artist_name_count
    from artists
    group by artist_id
    having count(1) > 1
),
artists_multiple_ids as (
    select 
        artist_name,
        count(1) as artist_id_count
    from artists
    group by artist_name
    having count(1) > 1
)
select 
a.artist_id,
a.artist_name,
case when exists (select 1 from artists_multiple_names b where b.artist_id = a.artist_id) then 1 else 0 end  as multiple_name_indicator,
case when exists (select 1 from artists_multiple_ids c where c.artist_name = a.artist_name) then 1 else 0 end as multiple_id_indicator
from artists a;
""")

staging_artist_names_insert_01 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
select distinct
    a.artist_id,
    a.artist_name,
    a.artist_id as recalculated_artist_id,
    first_value(ar.artist_latitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_latitude,
    first_value(ar.artist_longitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_longitude,
    first_value(ar.artist_location) over (
        partition by a.artist_id, a.artist_name order by ar.artist_location_score desc
        rows unbounded preceding
    ) as artist_location,
    a.multiple_name_indicator,
    a.multiple_id_indicator,
    1 as step
from staging_artist_id_name a 
left join staging_artist_row ar on 
    a.artist_id = ar.artist_id 
    and a.artist_name = ar.artist_name
where a.multiple_id_indicator = 0 
and a.multiple_name_indicator = 0;
""")

staging_artist_names_insert_02 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
select
    distinct
    a.artist_id,
    a.artist_name,    
    coalesce(
        first_value(an.recalculated_artist_id) over (
            partition by a.artist_id, a.artist_name 
            order by an.recalculated_artist_id desc nulls last 
            rows unbounded preceding
        )
        , a.artist_id
    ) as recalculated_artist_id,
    first_value(ar.artist_latitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_latitude,    
    first_value(ar.artist_longitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as new_artist_longitude,    
    first_value(ar.artist_location) over (
        partition by a.artist_id, a.artist_name order by ar.artist_location_score desc
        rows unbounded preceding
    ) as artist_location,
    a.multiple_name_indicator,
    a.multiple_id_indicator,
    2 as step
from staging_artist_id_name a 
left join staging_artist_row ar on
    a.artist_id = ar.artist_id 
    and a.artist_name = ar.artist_name
left join staging_artist_names an on
    a.artist_name = an.artist_name
where 1=1
and  a.multiple_id_indicator = 0
and a.multiple_name_indicator = 1;
""")

staging_artist_names_insert_03 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
select
    distinct
    a.artist_id,
    a.artist_name,
    coalesce(
        first_value(an.recalculated_artist_id) over (
            partition by a.artist_id, a.artist_name 
            order by an.recalculated_artist_id desc nulls last 
            rows unbounded preceding
        )
        , a.artist_id
    ) as recalculated_artist_id,
    first_value(ar.artist_latitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_latitude,    
    first_value(ar.artist_longitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as new_artist_longitude,    
    first_value(ar.artist_location) over (
        partition by a.artist_id, a.artist_name order by ar.artist_location_score desc
        rows unbounded preceding
    ) as artist_location,
    a.multiple_name_indicator,
    a.multiple_id_indicator,
    3 as step
from staging_artist_id_name a 
left join staging_artist_row ar on
    a.artist_id = ar.artist_id 
    and a.artist_name = ar.artist_name
left join staging_artist_names an on
    a.artist_name = an.artist_name
where 1=1
and  a.multiple_id_indicator = 1
and a.multiple_name_indicator = 0;
""")

staging_artist_names_insert_04 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
select
    distinct
    a.artist_id,
    a.artist_name,
    coalesce(
        first_value(an.recalculated_artist_id) over (
            partition by a.artist_id, a.artist_name 
            order by an.recalculated_artist_id desc nulls last 
            rows unbounded preceding
        )
        , a.artist_id
    ) as recalculated_artist_id,
    first_value(ar.artist_latitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_latitude,    
    first_value(ar.artist_longitude) over (
        partition by a.artist_id, a.artist_name order by ar.artist_lat_long_score desc
        rows unbounded preceding
    ) as new_artist_longitude,    
    first_value(ar.artist_location) over (
        partition by a.artist_id, a.artist_name order by ar.artist_location_score desc
        rows unbounded preceding
    ) as artist_location,
    a.multiple_name_indicator,
    a.multiple_id_indicator,
    4 as step
from staging_artist_id_name a 
left join staging_artist_row ar on
    a.artist_id = ar.artist_id 
    and a.artist_name = ar.artist_name
left join staging_artist_names an on
    a.artist_name = an.artist_name
where 1=1
and  a.multiple_id_indicator = 1
and a.multiple_name_indicator = 1;
""")

staging_artist_names_insert_05 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
select    
    an.original_artist_id,
    an.artist_name,    
    first_value(an.recalculated_artist_id) over (
        partition by an.artist_name 
        order by an.recalculated_artist_id desc nulls last 
        rows unbounded preceding
    ) as recalculated_artist_id,
    an.artist_latitude,    
    an.artist_longitude,    
    an.artist_location,
    cast( null as int) as multiple_name_indicator,
    cast( null as int) as multiple_id_indicator,
    5 as step
from staging_artist_names an 
where step in (1,2,3,4);
""")

staging_artist_names_insert_06 = ("""
insert into staging_artist_names (
    original_artist_id,
    artist_name,
    recalculated_artist_id,
    artist_latitude,  
    artist_longitude,
    artist_location,
    multiple_name_indicator,
    multiple_id_indicator,
    step
)
with staging_artist_names_5 as (
    select
    an.original_artist_id,
    an.artist_name,
    an.recalculated_artist_id,
    an.artist_latitude,
    case when an.artist_latitude is not null then 1 else 0 end as artist_latitude_score,
    an.artist_longitude,    
    case when an.artist_longitude is not null then 1 else 0 end as artist_longitude_score,
    artist_latitude_score + artist_longitude_score as artist_lat_long_score,
    an.artist_location,
    case when not trim(an.artist_location) = '' then 1 else 0 end as artist_location_score,
    an.multiple_name_indicator,
    an.multiple_id_indicator
    from staging_artist_names an
    where step = 5
)
select    
    an5.original_artist_id,
    an5.artist_name,
    an5.recalculated_artist_id,
    first_value(an5.artist_latitude) over (
        partition by an5.recalculated_artist_id order by an5.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_latitude,    
    first_value(an5.artist_longitude) over (
        partition by an5.recalculated_artist_id order by an5.artist_lat_long_score desc
        rows unbounded preceding
    ) as artist_longitude,    
    first_value(an5.artist_location) over (
        partition by an5.recalculated_artist_id order by an5.artist_location_score desc
        rows unbounded preceding
    ) as artist_location,
    an5.multiple_name_indicator,
    an5.multiple_id_indicator,
    6 as step
from staging_artist_names_5 an5;
""")

# FINAL DWH TABLES

# Load Artist names dimension based on the last step of the staging table
artist_table_insert = ("""
insert into artist_names (
    name,
    artist_id,
    latitude,  
    longitude,
    location
)
select distinct
an.artist_name,
an.recalculated_artist_id,
an.artist_latitude,
an.artist_longitude,    
an.artist_location    
from staging_artist_names an
where step = 6;
""")

# Load song title  tables 
song_titles_table_insert = ("""
insert into song_titles
(artist_name, title, year, duration)
select     
    s.artist_name,
    s.title,    
    max(s.year)::int as year,
    max(s.duration)::decimal as duration
from staging_songs s
group by s.artist_name, s.title;
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

create_table_queries = [
    # RAW STAGING TABLES
    staging_events_table_create,
    staging_songs_table_create,
    # INTERMEDIATE STAGING TABLES
    staging_artist_row_table_create,
    staging_artist_id_name_table_create,
    staging_artist_names_table_create, 
    # DWH TABLES
    artist_names_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,    
    time_table_create]

drop_table_queries = [
    # RAW STAGING TABLES
    staging_events_table_drop, 
    staging_songs_table_drop,
    staging_artist_row_table_drop,
    staging_artist_id_name_table_drop,
    staging_artist_names_table_drop,
    artist_names_table_drop, 
    songplay_table_drop,
    user_table_drop,
    song_table_drop,    
    time_table_drop]

# RAW STAGING TABLES
copy_table_queries = [staging_events_copy, staging_songs_copy]

# Load first song and artist
insert_table_queries = [
    # INTERMEDIATE STAGING TABLES
    staging_artist_row_insert,    
    staging_artist_id_name_insert,
    staging_artist_names_insert_01,
    staging_artist_names_insert_02,
    staging_artist_names_insert_03,
    staging_artist_names_insert_04,
    staging_artist_names_insert_05,
    staging_artist_names_insert_06,
    # DWH TABLES
    artist_table_insert,
    song_table_insert,
    songplay_table_insert,
    user_table_insert,
    time_table_insert]
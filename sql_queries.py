import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_PATH = config.get("S3","LOG_JSONPATH")
PUBLIC = config.get('IAM_ROLE','KEY')
SECRET = config.get('IAM_ROLE','SECRET')


# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists times"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status varchar,
    ts varchar,
    userAgent varchar,
    userId varchar
)
diststyle auto
""")

staging_songs_table_create = ("""
create table staging_songs(
    num_songs integer,
    artist_id varchar, 
    artist_lattitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
)
diststyle auto
""")

songplay_table_create = ("""
create table songplays(
    primary key(songplay_id),
    songplay_id integer identity(0,1) not null,
    start_time timestamp not null,
    user_id integer not null,
    level varchar not null, 
    song_id varchar,
    artist_id varchar, 
    session_id integer,
    location varchar, 
    user_agent varchar  
)
distkey(song_id)
sortkey(start_time)
""")

user_table_create = ("""
create table users(
    primary key(user_id),
    user_id integer,
    first_name varchar,
    last_name varchar,
    gender varchar, 
    level varchar
)
diststyle all
""")

song_table_create = ("""
create table songs(
    primary key(song_id),
    song_id varchar,
    title varchar,
    artist_id varchar,
    year integer,
    duration float
)
distkey(song_id)
""")

artist_table_create = ("""
create table artists(
    primary key(artist_id),
    artist_id varchar,
    name varchar,
    location varchar,
    lattitude float,
    longitude float
)
diststyle even
""")

time_table_create = ("""
create table times(
    primary key(start_time),
    start_time timestamp,
    hour smallint, 
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
)
diststyle even
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_access_key_id = {} aws_secret_access_key = {}'
JSON {};
""").format(LOG_DATA,PUBLIC,SECRET,LOG_PATH)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_access_key_id = {} aws_secret_access_key = {}';
""").format(SONG_DATA,PUBLIC,SECRET)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select timestamp 'epoch' + e.ts::bigint / 1000 * interval '1 second' as start_time,
       cast(e.userId as integer) as userId,
       se.level,
       se.song_id,
       se.artist_id,
       se.sessionId,
       se.location,
       se.userAgent,
from staging_events se
where se.page = 'NextSong';

""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) 
select cast(userId as integer) as userId,firstName,lastName,gender,level
from staging_events
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select song_id,title,artist_id,year,duration
from staging_songs
""")

artist_table_insert = ("""
insert into artists(artist_id,name,location,lattitude,longitude)
select artist_id,artist_name,artist_location,artist_lattitude,artist_longitude
from staging_songs
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select start_time,
extract('hour' from start_time) as hour,
extract('day' from start_time) as day,
extract('week' from start_time) as week,
extract('month' from start_time) as month,
extract('year' from start_time) as year,
extract('weekday' from start_time) as weekday
from
(
  select distinct timestamp 'epoch' + ts::bigint / 1000 * interval '1 second' as start_time
  from staging_events
) event;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
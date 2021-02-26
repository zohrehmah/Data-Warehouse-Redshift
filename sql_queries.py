import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS song;"
artist_table_drop = "DROP table IF EXISTS artist;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                  (artist varchar,
                                    auth varchar,
                                    firstName varchar(50),
                                    gender char,
                                    itemInSession int,
                                    lastName varchar(50),
                                    length float,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration float,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts bigint,
                                    userAgent varchar,
                                    userId int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (   num_songs int,
                                    artist_id varchar,
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration float,
                                    year float
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                             (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                              start_time bigint not null SORTKEY DISTKEY, 
                              user_id int not null, 
                              level varchar,
                              song_id varchar, 
                              artist_id varchar,
                              session_id int,
                              location varchar, 
                              user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                               (user_id int SORTKEY PRIMARY KEY  , 
                                 first_name varchar, 
                                 last_name varchar,
                                 gender varchar, 
                                 level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                               (song_id varchar SORTKEY PRIMARY KEY  , 
                                title varchar, 
                                artist_id varchar, 
                                year int,
                                duration numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                                 (artist_id varchar SORTKEY  PRIMARY KEY  , 
                                  name varchar, 
                                  location varchar,
                                  latitude varchar, 
                                  longitude varchar)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                                  (start_time time SORTKEY DISTKEY PRIMARY KEY  , 
                                    hour int, 
                                    day int, 
                                    week int, 
                                    month int, 
                                    year int, 
                                    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
region 'us-west-2' FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
region 'us-west-2' FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

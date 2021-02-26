# Project Overview
Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# Project Repository files
1. song_date file contains metadata about a song and the artist of that song. 
2. log_data file contains activity logs from a music streaming app based on specified configurations.

# Database Design
1. Song Dataset:
1.1 songs - song_id, title, artist_id, year, duration
1.2 artists - artist_id, name, location, latitude, longitude

2. Log dataset:
2.1 songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
2.2 users - user_id, first_name, last_name, gender, level
2.3 Time -  start_time, hour, day, week, month, year, weekday

The songplays is a fact table and other tables are dimension.

# Project Structure
1. test.ipynb runs the project
2. create_tables.py drops and creates your tables.
3. etl.ipynb reads and processes a single file from song_data and log_data and loads the data into the tables.
4. etl.py reads and processes files from song_data and log_data and loads them into the tables.
5. sql_queries.py contains all your sql queries, and is imported into the last three files above.

# How To Run the Project
Use Redshift_Cluster_IaC.py from Data_Engineering_Projects to launch Redshift Cluster.
Setup Configurations
Setup the dwh.cfg file (File not added in this repository). File format for dwh.cfg

[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=<IAM Role arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

Then run the test.ipynb

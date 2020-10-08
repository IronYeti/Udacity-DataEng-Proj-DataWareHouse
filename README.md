# Sparkify Moves To The Cloud

## Overview

This program will do an ETL of Sparkify data, copying it from an S3 bucket to stage it on an AWS RedShift cluster, then transform that data into analytics tables also on RedShift.

This program satifies requirements of the Udacity Data Engineering Nanodegree program for Data Warehouse Project #3.


## Prequisites

1. Make sure the raw data files are properly located in S3 bucket with path `s3://udacity-dend/`.
2. The configuration file contains an AWS key & secret. The key & secret must come from an admin user that was set up with _programmatic access_ enabled.
3. The files `etl.py`, `dwh.cfg` & `sql_queries.py` are in the same diectory.

## How to run

1. Run the python script `etl.py` from terminal.

## Expected output is

1. AWS role is created
2. Temporary Redshift cluster is created
3. Connect to the database
4. Create all database tables (staging, dimensional and fact tables)
5. Copy data from S3 to staging tables
6. Transform data from staging tables to dimension and fact tables
7. Validate that the tables were populated correctly
8. Delete the cluster and role (since this is an academic exercise we don't want to persist the data)

---

## Configuration file

A file named `dwh.cfg` must contain these entries:

```
[AWS]
key = {your_aws_key}
secret = {your_aws_secret}

[DWH]
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large
dwh_iam_role_name = dwhRole
dwh_cluster_identifier = dwhCluster
dwh_db = dwh
dwh_db_user = dwhadmin
dwh_db_password = Passw0rd
dwh_port = 5439

[S3]
LOG_DATA = s3://udacity-dend/log_data
LOG_JSONPATH = s3://udacity-dend/log_json_path.json
SONG_DATA = s3://udacity-dend/song_data
```


---

## Raw Data

Files must be stored in `s3://udacity-dend/` bucket.


### Song Files
- Must be loca.  They can be optionally be organized in subfolders under that (e.g. data/song_data/A/B/C)
- Must be JSON formatted and contain at least the following keys: song_id, title, artist_id, year, duration, artist_id, artist_name, artist_location, artist_latitude, artist_longitude


### Log files
- Must be placed into the 'data/log_data' folder.  They can be optionally be organized in subfolders under that (e.g. data/log_data/A/B/C)
- Must be JSON formatted and contain at least the following keys: userId, firstName, lastName, gender, level, ts (timestamp in milliseconds), page, sessionId, location, userAgent, artist, song


### Log data json path
- Must be a JSONPaths file named `log_json_path.json` 


## Database schema

Tables are organized into a star schema with **songplays** as the fact table at the center and dimension tables **users, songs, artists,** and **time** surrounding that.

- **songplays**: records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- **songs**: songs in music database
    - song_id, title, artist_id, year, duration
- **artists**: artists in music database
    - artist_id, name, location, latitude, longitude
- **users**: users in the app
    - user_id, first_name, last_name, gender, level
- **time**: timestamps of records in **songplays** broken down into specific units
    - start_time, hour, day, week, month, year, weekday

![](images/sparkify_schema.png)


## Repo file descriptions

The following files are included in this repository:

- /images - images for the readme file
- etl.py - main python script that creates the cloud instance and does the ETL
- etl.ipynb - Jupyter notebook for exploratory data analysis and testing of functions
- create_tables.py - helper functions to connect to PostGreSQL and create the sparkify database
- sql_queries.py - helper functions to put all SQL commands together
- test.ipynb - notebook for validating the database records
- .gitignore - list of files not tracked by git
- README.md - this file


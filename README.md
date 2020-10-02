# Sparkify - Song Play Analysis

## Overview

This program will *extract* data from song & log files, *transform* the data, and then *load* it into a database. A star schema was chosen to make data analysis easier for the user.  


## How to run

1. Make sure the data files are properly located (see Technical Overview)
2. Run the python script `!python3 create_tables.py`
3. Run the python script `!python3 etl.py`
    

## Technical Overview

### Raw data

Files must be in JSON format. 
Files must be stored as follows:

#### Song Files
- Must be placed into the 'data/song_data' folder.  They can be optionally be organized in subfolders under that (e.g. data/song_data/A/B/C)
- Must be JSON formatted and contain at least the following keys: song_id, title, artist_id, year, duration, artist_id, artist_name, artist_location, artist_latitude, artist_longitude


#### Log files
- Must be placed into the 'data/log_data' folder.  They can be optionally be organized in subfolders under that (e.g. data/log_data/A/B/C)
- Must be JSON formatted and contain at least the following keys: userId, firstName, lastName, gender, level, ts (timestamp in milliseconds), page, sessionId, location, userAgent, artist, song


### Database schema

Tables are organized into a star schema with **songplays** as the fact table at the center and dimension tables **users, songs, artists,** and **time** surroundind that.

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


### Repo file descriptions

The following files are included in this repository:

- /data - directory holding all of the raw data (see above for details)
- /images - images for the readme file
- create_tables.py - helper functions to connect to PostGreSQL and create the sparkify database
- etl.ipynb - notebook for exploratory data analysis and testing of functions
- etl.py - python functions that perform the extraction, transformation and loading of data
- README.md - this file
- sql_queries.py - helper functions to put all SQL commands together
- test.ipynb - notebook for validating the database records


import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
# IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

S3_LOG_DATA             = config.get("S3", "LOG_DATA")
S3_SONG_DATA            = config.get("S3", "SONG_DATA")
S3_EVENT_DATA_JSON_PATH = config.get("S3", "LOG_JSONPATH")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop           = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop           = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop         = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop           = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

#   id              INT GENERATED ALWAYS AS IDENTITY,

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   SMALLINT,
    lastName        VARCHAR,
    length          NUMERIC,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    sessionId       SMALLINT,
    song            VARCHAR,
    status          SMALLINT,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INT
);
""")

#    id                  INT GENERATED ALWAYS AS IDENTITY,

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT,
    artist_id           VARCHAR,
    artist_latitude     NUMERIC,
    artist_longitude    NUMERIC,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            NUMERIC,
    year                SMALLINT
);
""")



songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    id          INT IDENTITY(0, 1) PRIMARY KEY,
    start_time  TIMESTAMP,
    user_id     VARCHAR REFERENCES users(user_id) NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR REFERENCES songs(song_id) NOT NULL,
    artist_id   VARCHAR REFERENCES artists(artist_id) NOT NULL,
    session_id  INT,
    location    VARCHAR,
    user_agent  VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR NOT NULL PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR NOT NULL PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR,
    year        SMALLINT,
    duration    VARCHAR
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR NOT NULL PRIMARY KEY,
    name        VARCHAR,
    location    VARCHAR,
    latitude    NUMERIC,
    longitude   NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP NOT NULL PRIMARY KEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    weekday     SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events 
    FROM '{S3_LOG_DATA}' 
    CREDENTIALS 'aws_iam_role=_arnrole_'
    JSON '{S3_EVENT_DATA_JSON_PATH}'
    REGION 'us-west-2';
"""

staging_songs_copy = f"""
    COPY staging_songs
    FROM '{S3_SONG_DATA}' 
    CREDENTIALS 'aws_iam_role=_arnrole_'
    JSON 'auto'
    REGION 'us-west-2';
    """


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        e.userId AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
    FROM staging_songs s
    RIGHT JOIN staging_events e ON s.artist_name = e.artist AND s.title = e.song AND e.length = s.duration
    WHERE e.page = 'NextSong' AND song_id IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events se
    WHERE userId IS NOT NULL AND ts = (SELECT MAX(ts) FROM staging_events se2 WHERE se.userId = se2.userId)
    ORDER BY userId DESC
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        CAST(artist_latitude AS NUMERIC) as latitude,
        CAST(artist_longitude AS NUMERIC) as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR from start_time) as hour,
        EXTRACT(DAY from start_time) as day,
        EXTRACT(WEEK from start_time) as week,
        EXTRACT(MONTH from start_time) as month,
        EXTRACT(YEAR from start_time) as year,
        EXTRACT(WEEKDAY from start_time) as weekday
    FROM staging_events
""")

# VALIDATION QUERIES

check_staging_events = "SELECT COUNT(*) FROM staging_events;"
check_staging_songs  = "SELECT COUNT(*) FROM staging_songs;"
check_songplays      = "SELECT COUNT(*) FROM songplays;"
check_users          = "SELECT COUNT(*) FROM users;"
check_songs          = "SELECT COUNT(*) FROM songs;"
check_artists        = "SELECT COUNT(*) FROM artists;"
check_time           = "SELECT COUNT(*) FROM time;"

# QUERY LISTS

create_table_queries = [
    artist_table_create, 
    user_table_create, 
    song_table_create, 
    time_table_create, 
    songplay_table_create,
    staging_events_table_create, 
    staging_songs_table_create
    ]

drop_table_queries = [
    user_table_drop, 
    song_table_drop, 
    time_table_drop, 
    songplay_table_drop, 
    artist_table_drop,
    staging_events_table_drop, 
    staging_songs_table_drop,
    ]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
    ]

insert_table_queries = [
    time_table_insert,
    user_table_insert, 
    song_table_insert, 
    artist_table_insert,
    songplay_table_insert 
    ]

validation_queries = [
    check_staging_events,
    check_staging_songs,
    check_songplays,
    check_users,
    check_songs,
    check_artists,
    check_time
]
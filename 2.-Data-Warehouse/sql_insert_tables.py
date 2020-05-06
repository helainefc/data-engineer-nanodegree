import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# ACESS INFO FROM CONFIG FILE
arn = config['IAM_ROLE']['ARN']
log_data = config['S3']['LOG_DATA']
log_json_path = config['S3']['LOG_JSONPATH']
song_data = config['S3']['SONG_DATA']
region = config['GEO']['REGION']


# COPY STAGING TABLES FROM S3
#---------------------------------------------------------------------------------------------
staging_log_copy = ("""
   copy staging_log
   from {}
   iam_role {}
   json {}
""").format(log_data, arn, log_json_path)

# Load from JSON Data Using the 'auto' Option
staging_song_copy = ("""
    copy staging_song
    from {}
    iam_role {}
    json 'auto'
""").format(song_data, arn)
#---------------------------------------------------------------------------------------------

# INSERT TABLES
#----------------------------------------------------------------------------------------------
songplay_table_insert = ("""
    INSERT INTO fact_songplay (       
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + sl.ts/1000 \
                * INTERVAL '1 second' as start_time,
            sl.userId  as user_id,
            sl.level as level,
            ss.song_id  as song_id,
            ss.artist_id as artist_id,
            sl.sessionId as session_id,
            sl.location  as location,
            sl.userAgent as user_agent
    FROM staging_log AS sl
    JOIN staging_song AS ss
        ON (sl.artist = ss.artist_name)
    WHERE sl.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO dim_song ( 
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT  DISTINCT ss.song_id  AS song_id,
            ss.title   AS title,
            ss.artist_id  AS artist_id,
            ss.year   AS year,
            ss.duration   AS duration
    FROM staging_song AS ss;
""")

user_table_insert = ("""
    INSERT INTO dim_user (                 
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT  DISTINCT sl.userId AS user_id,
            sl.firstName  AS first_name,
            sl.lastName   AS last_name,
            sl.gender     AS gender,
            sl.level      AS level
    FROM staging_log AS sl
    WHERE sl.page = 'NextSong';
""")

artist_table_insert = ("""
    INSERT INTO dim_artist ( 
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_song AS ss;
""")

time_table_insert = ("""
    INSERT INTO dim_time (  
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + sl.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_log             AS sl
    WHERE sl.page = 'NextSong';
""")
#---------------------------------------------------------------------------------

#LIST

copy_table_queries   = [staging_log_copy,staging_song_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

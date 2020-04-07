# DROP COLUMN FAMILIES

song_by_session_table_drop           = "DROP TABLE IF EXISTS song_by_session;"
song_user_by_user_session_table_drop = "DROP TABLE IF EXISTS song_user_by_user_session;"
song_by_title_table_drop             = "DROP TABLE IF EXISTS song_by_title;"

#CREATE COLUMN FAMILIES
song_by_session_table_create = ("""
CREATE TABLE IF NOT EXISTS song_by_session (
    session_id int,
    item_in_session bigint,
    artist_name text, 
    song_title text, 
    duration float,
    PRIMARY KEY (session_id, item_in_session)
);
""")
song_user_by_user_session_create = ("""
CREATE TABLE IF NOT EXISTS song_user_by_user_session (
    user_id int , 
    session_id int, 
    item_in_session bigint,
    artist_name text, 
    song_title text, 
    user_name text,
    PRIMARY KEY ((user_id, session_id), item_in_session)
);
""")

song_by_title_table_create = ("""
CREATE TABLE IF NOT EXISTS song_by_title (
    song_title text,
    user_id int,
    first_name text,
    last_name text,
    PRIMARY KEY ((song_title), user_id)
);
""")

# INSERT RECORDS

song_by_session_table_insert = ("""
INSERT INTO song_by_session (
    session_id,
    item_in_session,
    artist_name, 
    song_title, 
    duration
) 
VALUES (?, ?, ?, ?, ?)
""")

song_user_by_use_session_insert = ("""
INSERT INTO song_user_by_user_session (
    user_id, 
    session_id , 
    item_in_session,
    artist_name, 
    song_title, 
    user_name
) 
VALUES (?, ?, ?, ?, ?, ?)
""")

song_by_title_table_insert = ("""
INSERT INTO song_by_title (
    song_title,
    user_id,
    first_name,
    last_name
) 
VALUES (?, ?, ?, ?)
""")

#SELECT
query_by_session_itemInSession = (""" SELECT artist_name, song_title, duration FROM song_by_session WHERE session_id = ? AND item_in_session = ? """)
query_by_userId_sessionId      = (""" SELECT artist_name, song_title, user_name FROM INSERT INTO song_user_by_user_session ( WHERE user_id = ?  AND session_id = ? """)
query_by_title                 = (""" SELECT first_name, last_name FROM song_by_title WHERE song_title = ? """)

#LIST
create_table_queries = [song_by_session_table_create,song_user_by_user_session_create, song_by_title_table_create]
drop_table_queries   = [song_by_session_table_drop, song_user_by_user_session_table_drop, song_by_title_table_drop]
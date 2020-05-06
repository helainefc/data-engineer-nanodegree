CREATE TABLE IF NOT EXISTS public.staging_events (
    artist      VARCHAR                 NULL,
    auth        VARCHAR                 NULL,
    firstName   VARCHAR                 NULL,
    gender      VARCHAR                 NULL,
    itemInSession VARCHAR               NULL,
    lastName    VARCHAR                 NULL,
    length      VARCHAR                 NULL,
    level       VARCHAR                 NULL,
    location    VARCHAR                 NULL,
    method      VARCHAR                 NULL,
    page        VARCHAR                 NULL,
    registration VARCHAR                NULL,
    sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,
    song        VARCHAR                 NULL,
    status      INTEGER                 NULL,
    ts          BIGINT                  NOT NULL,
    userAgent   VARCHAR                 NULL,
    userId      INTEGER                 NULL
);

CREATE TABLE IF NOT EXISTS public.staging_song (
    num_songs           INTEGER         NULL,
    artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
    artist_latitude     VARCHAR         NULL,
    artist_longitude    VARCHAR         NULL,
    artist_location     VARCHAR(500)    NULL,
    artist_name         VARCHAR(500)    NULL,
    song_id             VARCHAR         NULL,
    title               VARCHAR(500)    NULL,
    duration            DECIMAL(9)      NULL,
    year                INTEGER         NULL
);

CREATE TABLE IF NOT EXISTS public.songplay (
    start_time  TIMESTAMP               NOT NULL,
    user_id     VARCHAR(50)             NOT NULL DISTKEY,
    level       VARCHAR(10)             NOT NULL,
    song_id     VARCHAR(40)             NULL,
    artist_id   VARCHAR(50)             NULL,
    session_id  VARCHAR(50)             NULL,
    location    VARCHAR(100)            NULL,
    user_agent  VARCHAR(255)            NULL
);

CREATE TABLE IF NOT EXISTS public.users (
    user_id     INTEGER                 NOT NULL SORTKEY,
    first_name  VARCHAR(50)             NULL,
    last_name   VARCHAR(80)             NULL,
    gender      VARCHAR(10)             NULL,
    level       VARCHAR(10)             NULL
) diststyle ALL;

CREATE TABLE IF NOT EXISTS public.time (
    start_time  TIMESTAMP               NOT NULL SORTKEY,
    hour        SMALLINT                NULL,
    day         SMALLINT                NULL,
    week        SMALLINT                NULL,
    month       SMALLINT                NULL,
    year        SMALLINT                NULL,
    weekday     SMALLINT                NULL
)diststyle all;

CREATE TABLE IF NOT EXISTS public.artist (
    artist_id   VARCHAR(50)            NOT NULL SORTKEY,
    name        VARCHAR(500)           NULL,
    location    VARCHAR(500)           NULL,
    latitude    DECIMAL(9)             NULL,
    longitude   DECIMAL(9)             NULL
);



CREATE TABLE IF NOT EXISTS public.song (
    song_id     VARCHAR(50)             NOT NULL SORTKEY,
    title       VARCHAR(500)            NOT NULL,
    artist_id   VARCHAR(50)             NOT NULL,
    year        INTEGER                 NOT NULL,
    duration    DECIMAL(9)              NOT NULL
);


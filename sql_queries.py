# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop     = "DROP TABLE IF EXISTS users;"
song_table_drop     = "DROP TABLE IF EXISTS songs;"
artist_table_drop   = "DROP TABLE IF EXISTS artists;"
time_table_drop     = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  serial     PRIMARY KEY,
    start_time   timestamp  NOT NULL REFERENCES time,
    user_id      int        NOT NULL REFERENCES users,
    song_id      varchar    REFERENCES songs,
    artist_id    varchar    REFERENCES artists,
    level        varchar,
    session_id   int,
    location     varchar,
    user_agent   varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     int      PRIMARY KEY,
    first_name  varchar,
    last_name   varchar,
    gender      char,
    level       varchar
);
""")

# not marking artist_id as FK because of Star schema
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id    varchar  PRIMARY KEY,
    title      varchar  NOT NULL,
    artist_id  varchar,
    year       int      CHECK (year >= 0),
    duration   decimal  NOT NULL
);
""")

# implemented data quality checks
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id  varchar  PRIMARY KEY,
    name       varchar  NOT NULL,
    location   varchar,
    latitude   float8   CHECK (latitude >= -90 AND latitude <= 90),
    longitude  float8   CHECK (longitude >= -180 AND longitude <= 180)
);
""")

# implemented data quality checks
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  timestamp  PRIMARY KEY,
    hour        smallint   CHECK (hour >= 0 AND hour < 24),
    day         smallint   CHECK (day >= 1 AND day <= 31),
    week        smallint   CHECK (week >= 0 AND week <= 53),
    month       smallint   CHECK (month >= 1 AND month <= 12),
    year        smallint   CHECK (year >= 0),
    weekday     smallint   CHECK (weekday >= 0 AND weekday <= 7)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
    (start_time, user_id, song_id, artist_id, level, session_id, location, user_agent)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT
    DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
VALUES
    (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
    DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs
    (song_id, title, artist_id, year, duration)
VALUES
    (%s, %s, %s, %s, %s)
ON CONFLICT
    DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
VALUES
    (%s, %s, %s, %s, %s)
ON CONFLICT
    DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT
    S.song_id, S.artist_id
FROM songs S
    JOIN artists A
    ON S.artist_id=A.artist_id
WHERE S.title=%s
    AND A.name=%s
    AND S.duration=%s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
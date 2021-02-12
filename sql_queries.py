import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stg_events(
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    itemInSession int,
                                    lastName varchar,
                                    length decimal(18,5),
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration timestamp,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts timestamp,
                                    userAgent text,
                                    userId int
                                    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stg_songs(
                                    artist_id varchar,
                                    artist_latitude decimal(18,5),
                                    artist_location varchar,
                                    artist_longitude decimal(18,5),
                                    artist_name varchar,
                                    duration decimal(18,5),
                                    num_songs int,
                                    song_id varchar,
                                    title varchar,
                                    year int
                                    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
                                start_time timestamp not null sortkey, 
                                user_id int not null, 
                                level varchar not null, 
                                song_id varchar not null distkey, 
                                artist_id varchar not null, 
                                session_id int, 
                                location varchar, 
                                user_agent text,
                                CONSTRAINT user_id
                                    FOREIGN KEY (user_id)
                                        REFERENCES users(user_id),
                                CONSTRAINT song_id
                                    FOREIGN KEY (song_id)
                                        REFERENCES songs(song_id),
                                CONSTRAINT artist_id
                                    FOREIGN KEY (artist_id)
                                        REFERENCES artists(artist_id),
                                CONSTRAINT start_time
                                    FOREIGN KEY (start_time)
                                        REFERENCES time(start_time)
                                );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id int PRIMARY KEY sortkey, 
                            first_name varchar, 
                            last_name varchar, 
                            gender char(1), 
                            level varchar
                            )
                            diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                            song_id varchar PRIMARY KEY distkey sortkey, 
                            title varchar, 
                            artist_id varchar, 
                            year int, 
                            duration numeric
                            );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id varchar PRIMARY KEY sortkey, 
                            name varchar, 
                            location varchar, 
                            latitude numeric, 
                            longitude numeric
                            )
                            diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time timestamp PRIMARY KEY sortkey, 
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int, 
                            weekday int
                            )
                            diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy stg_events
                        from {}
                        iam_role {}
                        JSON {}
                        ROUNDEC
                        TIMEFORMAT 'epochmillisecs'
                        region 'us-west-2';
""").format( config.get('S3','LOG_DATA'), 
             config.get('IAM_ROLE', 'ARN'), 
             config.get('S3', 'LOG_JSONPATH') )

staging_songs_copy = ("""
                        copy stg_songs
                        from {} 
                        iam_role {}
                        JSON 'auto'
                        ROUNDEC
                        region 'us-west-2';
""").format( config.get('S3','SONG_DATA'), 
             config.get('IAM_ROLE', 'ARN') )

# FINAL TABLES

songplay_table_insert = ("""
                INSERT INTO songplays(
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent     
                )
                SELECT
                    se.ts              AS start_time,
                    se.userId          AS user_id,
                    se.level           AS level,
                    so.song_id         AS song_id,
                    so.artist_id       AS artist_id,
                    se.sessionId       AS session_id,
                    se.location        AS location,
                    se.userAgent       AS user_agent
                FROM stg_events se
                INNER JOIN stg_songs so
                    ON UPPER(so.title) = UPPER(se.song)
                    AND so.duration = se.length
                WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
                INSERT INTO users(
                    user_id, 
                    first_name, 
                    last_name, 
                    gender, 
                    level
                )
                SELECT 
                    userid            AS user_id,
                    firstName         AS first_name,
                    lastName          AS last_name,
                    gender            AS gender,
                    level             AS level
                FROM (
                      SELECT
                          userid,          
                          firstName,       
                          lastName,        
                          gender,
                          level,           
                          ts,
                          ROW_NUMBER() OVER 
                            (PARTITION BY userid ORDER BY ts DESC) as row_num
                      FROM stg_events
                      WHERE page = 'NextSong'
                ) 
                WHERE row_num = 1;        
""")

song_table_insert = ("""
                INSERT INTO songs(
                    song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration
                ) 
                SELECT 
                    DISTINCT
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                FROM stg_songs;
""")

artist_table_insert = ("""
                INSERT INTO artists(
                    artist_id, 
                    name, 
                    location, 
                    latitude, 
                    longitude
                ) 
                SELECT
                    artist_id,
                    artist_name as name,
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
                FROM (
                    SELECT
                     artist_id,
                     artist_name,
                     artist_location,       
                     artist_latitude,        
                     artist_longitude,
                     year,
                    ROW_NUMBER() OVER 
                      (PARTITION BY artist_id ORDER BY year DESC) as row_num
                    FROM stg_songs
                    )
                WHERE row_num = 1;   
""")

time_table_insert = ("""
                INSERT INTO time(
                    start_time,
                    hour,
                    day,
                    week,
                    month,
                    year,
                    weekday
                    )
                SELECT
                    DISTINCT
                    ts                      AS start_time,
                    EXTRACT(hour from ts)   AS hour,
                    EXTRACT(day from ts)    AS day,
                    EXTRACT(week from ts)   AS week,
                    EXTRACT(month from ts)  AS month,
                    EXTRACT(year from ts)   AS year,
                    EXTRACT(dow from ts)    AS weekday
                FROM stg_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create, 
                        songplay_table_create ]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop ]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy ]

insert_table_queries = [user_table_insert, 
                        artist_table_insert, 
                        time_table_insert, 
                        song_table_insert, 
                        songplay_table_insert ]
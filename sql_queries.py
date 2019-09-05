import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES staging_events, staging_songs, songplays, users, songs, artists, time
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays" 
user_table_drop = "DROP TABLE IF EXISTS users"  
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES staging_events.This table will load data of 's3://udacity-dend/song_data'
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                 artist text ,
                                 auth text,
                                 firstName text,
                                 gender text,
                                 itemInSession int8,
                                 lastName text,
                                 length double precision,
                                 level text ,
                                 location text ,
                                 method text ,
                                 page text ,
                                 registration int8 ,
                                 sessionId int8 ,
                                 song text,
                                 status int8 ,
                                 ts int8 ,
                                 userAgent text ,
                                 userId int8)
                              """) 

#Create the table staging_songs.This table will load data of 's3://udacity-dend/log_data'
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 artist_id text,
                                 artist_latitude text,
                                 artist_location text,
                                 artist_longitude text ,
                                 artist_name text ,	
                                 duration text ,
                                 num_songs text ,
                                 song_id text ,	
                                 title text ,
                                 year text )
                              """)


#Create the table songplays with primary key songplay_id and this table will be order by songplay_id.It's autoincrement
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id int IDENTITY(1,1) sortkey primary key,
                            start_time timestamp NOT NULL ,
                            user_id integer NOT NULL,
                            level text,
                            song_id text NOT NULL,
                            artist_id text NOT NULL,
                            session_id text NOT NULL,
                            location text ,
                            user_agent text)
                         """)

#Create the table users with primary key user_id and this table will be order by user_id
user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
						user_id integer sortkey primary key,
						first_name text NOT NULL,
						last_name text NOT NULL,
						gender text NOT NULL, 
						level text NOT NULL)
                    """)

#Create the table songs with primary key song_id and this table will be order by song_id
song_table_create = ("""CREATE TABLE  IF NOT EXISTS songs(
						song_id text sortkey primary key,
						title text NOT NULL,
						artist_id text NOT NULL,
						year integer NOT NULL,
						duration double precision  NOT NULL)
                    """)

#Insert table artists with primary key artist_id and this table will be order by artist_id
artist_table_create = ("""CREATE TABLE  IF NOT EXISTS artists(
						  artist_id text sortkey primary key,
						  name text NOT NULL,
						  location text ,
						  lattitude text,
						  longitude text)
                       """)

#Insert table time with primary key start_time and this table will be order by start_time 
time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
						start_time timestamp sortkey primary key,
						hour integer NOT NULL,
						day integer NOT NULL,
						week integer NOT NULL,
						month integer NOT NULL,
						year integer NOT NULL,
						weekday integer NOT NULL)
                     """)

# STAGING TABLES.

#It will load data into the tables staging_events of the file 's3://udacity-dend/log_data' 
staging_events_copy = ("""copy staging_events
					      from {}
						  credentials 'aws_iam_role={}'
						  json {}
						  region 'us-west-2'
					   """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

#It will load data into the tables staging_songs of the file 's3://udacity-dend/song_data'
staging_songs_copy = ("""copy staging_songs 
                         from {}
						 credentials 'aws_iam_role={}'
						 compupdate off region 'us-west-2'
						 FORMAT as JSON 'auto'
					  """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

#Insert data into the table songplays with information of teh tables staging_events, songs, and artists
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
							SELECT 
								DATEADD(MILLISECOND,ts%1000,
								DATEADD(SECOND,ts/1000,'19700101')),
								userId,
								level,
								song_id,
								artist_id,
								sessionId,
								location,
								userAgent
							FROM (
								SELECT 
									DISTINCT se.ts, 
									se.userId, 
									se.level, 
									sa.song_id, 
									sa.artist_id, 
									se.sessionId, 
									se.location, 
									se.userAgent
								FROM 
									staging_events se
								JOIN
									(SELECT 
										songs.song_id, 
										artists.artist_id, 
										songs.title, 
										artists.name,
										songs.duration
									FROM 
										songs
									JOIN artists
									ON songs.artist_id = artists.artist_id) AS sa
									ON (sa.title = se.song
									AND sa.name = se.artist)
								WHERE 
									se.page = 'NextSong'
								);
						""")

#Insert data into the table user with information of teh table staging_events
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
						SELECT 
							DISTINCT userId, 
							firstName,
							lastName, 
							gender,
							level 
						FROM 
							staging_events 
                        WHERE 
                            userId IS NOT NULL and page = 'NextSong';
						""")

#Insert data into the table songs with information of teh table staging_songs
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
						SELECT 
							DISTINCT song_id, 
							title, 
							artist_id, 
							cast(year as integer), 
							cast(duration as numeric) 
						FROM 
                            staging_songs;
					""")

#Insert data into the table artists with information of teh table staging_songs
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) 
						  SELECT 
                              DISTINCT artist_id, 
                              artist_name, 
                              artist_location, 
                              artist_latitude, 
                              artist_longitude 
						  FROM 
                              staging_songs;
					   """)


#Insert data into the table time with information of teh table staging_events
time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
						SELECT 
							DATEADD(MILLISECOND,ts_ev.ts%1000,DATEADD(SECOND,ts_ev.ts/1000,'19700101')),
							DATE_PART(h,DATEADD(MILLISECOND,ts_ev.ts% 1000,DATEADD(SECOND, ts_ev.ts/1000,'19700101'))),
							DATE_PART(d,DATEADD(MILLISECOND,ts_ev.ts% 1000,DATEADD(SECOND,ts_ev.ts/ 1000,'19700101'))),
							DATE_PART(w,DATEADD(MILLISECOND,ts_ev.ts%1000,DATEADD(SECOND,ts_ev.ts/1000,'19700101'))),
							DATE_PART(mon,DATEADD(MILLISECOND,ts_ev.ts%1000,DATEADD(SECOND,ts_ev.ts/1000, '19700101'))),
							DATE_PART(y,DATEADD(MILLISECOND,ts_ev.ts%1000,DATEADD(SECOND,ts_ev.ts/1000,'19700101'))),
							DATE_PART(dw,DATEADD(MILLISECOND,ts_ev.ts% 1000, DATEADD(SECOND,ts_ev.ts/1000,'19700101')))
						FROM 
                            (SELECT 
                                DISTINCT ts,
                                page
                            FROM 
                                staging_events) ts_ev
                        WHERE 
                            page = 'NextSong';     
					""")


# QUERY LISTS
#List create tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#List drop create tables
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#List for load table staging
copy_table_queries = [staging_events_copy, staging_songs_copy]
#List for insert into the tables not staging
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
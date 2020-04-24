class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO public.songplays
                (playid,
	            start_time,
	            userid,
	            "level",
	            songid,
	            artistid,
	            sessionid,
	            location,
	            user_agent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT DISTINCT userid, 
                firstname, 
                lastname, 
                gender, 
                level 
        FROM   staging_events 
        WHERE  page = 'NextSong' 
    """)

    song_table_insert = ("""
        SELECT DISTINCT song_id, 
                title, 
                artist_id, 
                year, 
                duration 
        FROM   staging_songs 
    """)

    artist_table_insert = ("""
        SELECT DISTINCT artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude 
        FROM   staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, 
               Extract(hour FROM start_time), 
               Extract(day FROM start_time), 
               Extract(week FROM start_time), 
               Extract(month FROM start_time), 
               Extract(year FROM start_time), 
               Extract(dayofweek FROM start_time) 
        FROM   songplays 
    """)
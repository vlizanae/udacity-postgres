import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Inserts data in song file to database
    - Reads and parses provided file <filepath>
    - Formats data for songs table and inserts the record
    - Formats data for artists table and inserts the record 
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].replace({np.nan: None}).values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Inserts data in log file to database and creates Fact Table songplays
    - Reads and parses provided file <filepath>
    - Filters out logs with a page different than NextSong
    - Transform timestamps into datetime objects
    - Formats data for time table and inserts the record
    - Formats data for users table and inserts the record
    - Queries users and artists table in order to get song and artist ids
    - Merges all these data into Fact Table songplays
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong'].copy()

    # convert timestamp column to datetime
    t = df.ts.astype('datetime64[ms]')
    df['timestamp'] = t
    
    # insert time data records
    time_data = (df.timestamp, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({label: series for label, series in zip(column_labels, time_data)})

    # for some reasons timestamps get casted to integers on the tolist method.
    time_records = time_df.to_records(index=False).tolist()
    cur.executemany(time_table_insert, map(lambda row: (pd.to_datetime(row[0]), *row[1:]), time_records))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates().copy()

    # insert user records
    cur.executemany(user_table_insert, user_df.to_records(index=False).tolist())

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.timestamp, row.userId, songid, artistid, row.level, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Handles finding data files on directories
    - Gets all data files in <filepath> dir
    - Applies provided <func> to insert data in files
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Runs the full routine.
    - Connects to the database.
    - Process all song files.
    - Process all log files.
    - Closes connection.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' Opens a song file and inserts required information into songs and artists tables of a sparkifydb database
     Input: 
        cur: cursor to execute PostgreSQL command in a database session
        filepath (str): filepath to a folder containing  songs files
     Output:
         None
    
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', \
                           'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''Opens a log file and inserts required information into users, time and songplays tables of a sprakifydb database
      Input:
        cur: cursor to execute PostgreSQL command in a database session
        filepath (str): filepath to a folder containing  logs files
      Output:
        None
    
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms', origin='unix')
    
    # insert time data records
    time_data = [t.dt.time, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, \
                 t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(list(zip(*time_data)), columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # create songid and artistid columns
    df['songid'] = None
    df['artistid'] = None
    
    # insert songplay records
    for index, row in df.iterrows():
       # get songid and artistid from song and artist tables        
        cur.execute(song_select, (row.song, row.artist, row.length))
        res = cur.fetchone()
        if res:
            df['songid'][index] = res[0]
            df['artistid'][index] = res[1]

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms', origin='unix').time(), row.userId, \
                         row.level, df['songid'][index], df['artistid'][index], row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''Processes all json files and prints information about the processing state
      Input:
        cur: cursor to execute PostgreSQL command in a database session
        conn: connection to sparkifydb 
        filepath (str): filepath to a folder containing files
        func: name of a function to be used in main()
      Output:
        None
    
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
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''Creates a connection to sparkifydb and processes all json files 
      Input: None
      Output: None
    
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
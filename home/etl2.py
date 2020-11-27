import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
import csv
import vars


def get_files(filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files

def extract_song_data(filepath, data):
    # read song json file via pandas
    song_df = pd.read_json(filepath, lines=True)
    # grab song columns and associated values
    song_data = list(song_df.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']].to_numpy()[0])
    # if song_id not already written to csv, process and write to csv
    song_id = song_data[0]
    if song_id not in data:
        # write data extracted from json file to new csv file for easier loading into table
        with open('songs_file.csv', 'a', newline='') as song_csv:
            song_writer = csv.writer(song_csv, delimiter=',')
            song_writer.writerow(song_data)
    data.append(song_id)


def extract_artist_data(filepath, data):
    # read song json file via pandas
    artist_df = pd.read_json(filepath, lines=True)
    # grab song columns and associated values
    artist_data = list(artist_df.loc[:, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    # artist_df.to_csv('artists_file.csv', columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'], header=False, index=False, mode='a')
    # if artist_id not already written to csv, process and write to csv
    artist_id = artist_data[0]
    if artist_id not in data:
        # write data extracted from json file to new csv file for easier loading into table
        with open('artists_file.csv', 'a', newline='') as artist_csv:
            # use quote minimal to limit quoting to just delimiter, lineterminator; to avoid quoting NaN, for example
            artist_writer = csv.writer(artist_csv, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            artist_writer.writerow(artist_data)
    data.append(artist_id)


def process_song_file(cur, filepath, data):
    extract_song_data(filepath, data)
    extract_artist_data(filepath, data)


def copy_song_and_artist_data_to_postgres(cur):

    with open('songs_file.csv') as f:
        cur.copy_from(f, 'songs', sep=',')

    with open('artists_file.csv') as f:
        cur.copy_expert("COPY artists from STDIN WITH DELIMITER ',' CSV", f)
        # cur.copy_from(f, 'artists', sep=',', columns=('artist_id', 'name', 'location', 'latitude', 'longitude'))


def process_data(cur, conn, filepath, func, data=[]):
    """function to iterate through each file within the log and song datasets and call the process_song_file and process_log_file to insert data into the respective tables in our Sparkify database"""

    all_files = get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    # iterate over files and write all json objects to a single file for processing
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, data)
        # print('{}/{} files processed.'.format(i, num_files))

    copy_song_and_artist_data_to_postgres(cur)


def main():
    # connect to db
    conn = psycopg2.connect(f"host=127.0.0.1 dbname={vars.sparkify_creds['dbname']} user={vars.sparkify_creds['user']} password={vars.sparkify_creds['password']}")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

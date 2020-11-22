import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
import csv
import vars


def process_song_file(cur, filepath, data):
    """function to copy each song csv file and insert records into the artist and song tables based on the values in the file"""

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


def get_files(filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def process_data(cur, conn, filepath, func, data=[]):
    """function to iterate through each file within the log and song datasets and call the process_song_file and process_log_file to insert data into the respective tables in our Sparkify database"""

    all_files = get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    # iterate over files and write all json objects to a single file for processing
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, data)
        print('{}/{} files processed.'.format(i, num_files))

    with open('songs_file.csv') as f:
        cur.copy_from(f, 'songs', sep=',')


def main():
    conn = psycopg2.connect(f"host=127.0.0.1 dbname={vars.sparkify_creds['dbname']} user={vars.sparkify_creds['user']} password={vars.sparkify_creds['password']}")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

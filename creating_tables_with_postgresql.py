import psycopg2
import vars

try:
    conn = psycopg2.connect(f"host=127.0.0.1 dbname={vars.creds['dbname']} user={vars.creds['user']} password={vars.creds['password']}")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);")

except psycopg2.Error as e:
    print('could not create table')
    print(e)

try:
    cur.execute("select count(*) from music_library")
except psycopg2.Error as e:
    print('could not create table')
    print(e)

print(cur.fetchall())

try:
    cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)",
                 ("Let It Be", "The Beatles", 1970))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)",
                 ("Rubber Soul", "The Beatles", 1965))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("SELECT * FROM music_library;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

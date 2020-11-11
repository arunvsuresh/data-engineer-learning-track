import psycopg2

# connect to db
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)


# create de-normalized to further normalize
try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id \
    int, customer_name varchar, cashier_name varchar, \
    year int, albums_purchased text[])")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO music_store (transaction_id, \
    customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, ['Rubber Soul', \
                 'Let it Be']))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)


# move data to 1st normal form
try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (\
    transaction_id int, customer_name varchar, \
    cashier_name varchar, \
    year int, songname varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, \
    customer_name, cashier_name, year, songname) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, 'Rubber Soul'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, \
    customer_name, cashier_name, year, songname) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, 'Let it Be'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)


# move data to 2nd normal form, creating a unique primary key for each row
# break up data into two tables, transactions & albums sold

try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
    customer_name varchar, cashier_name varchar, year int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, album_name varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, 'Rubber Soul'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, 'Let it Be'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

# move to 3rd normal form
try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, \
    cashier_id int, customer_name varchar, year int);")

except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, employee_name varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, \
    cashier_id, customer_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, 1, 'Amanda', 2000 ))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1, 'Sam'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

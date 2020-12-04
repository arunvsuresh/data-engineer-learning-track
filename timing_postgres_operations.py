import timeit
import psycopg2
from home.etl2 import main as copy_etl

from home.create_tables import main as create_tables

from home.etl import main as insert_etl


def timer(func):
    def wrapper(*args):
        start_time = timeit.default_timer()
        # delete_rows()
        func()
        end_time = timeit.default_timer()
        runtime = end_time - start_time
        print('runtime', runtime)
    return wrapper


# @timer
# def operations():
#     create_tables()
#     # insert_etl()
#     copy_etl()


@timer
def copy_operation():
    create_tables()
    copy_etl()


@timer
def insert_operation():
    create_tables()
    insert_etl()


def main():
    copy_operation()
    insert_operation()


if __name__ == '__main__':
    main()





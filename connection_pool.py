import psycopg2.pool
import psycopg2.extensions
import threading
from db_config import config
from fake_data_generator import generate_record
import time

config_data = config()


def get_connection_pool(n_threads):
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=n_threads,
        maxconn=n_threads,
        database=config_data['database'],
        user=config_data['user'],
        password=config_data['password'],
        host=config_data['host'],
        port=config_data['port'])
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    return connection, cursor, connection_pool


def insert_records(n, n_threads, query, params=None):
    connection, cursor, connection_pool = get_connection_pool(n_threads)
    for _ in range(n//n_threads):
        cursor.execute(query, params)
    connection_pool.putconn(connection)


def create_records(n=100000, n_threads=5):
    query = "INSERT INTO customers (first_name, last_name, email, phone_number, address) VALUES (%s, %s, %s, %s, %s);"
    params = generate_record()
    start = time.perf_counter()
    threads = []
    for _ in range(n_threads):
        thread = threading.Thread(target=insert_records, args=(n, n_threads, query, params))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stop = time.perf_counter()
    print(f"Time of execution: {stop-start}")


if __name__ == '__main__':
    create_records()


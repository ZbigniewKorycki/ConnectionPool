import psycopg2.pool
import psycopg2.extensions
import threading
from db_config import config
from fake_data_generator import generate_record
import time

config_data = config()


def get_connection_pool():
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=10,
        database=config_data['database'],
        user=config_data['user'],
        password=config_data['password'],
        host=config_data['host'],
        port=config_data['port'])


def insert_records(n, n_threads, query, params, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    try:
        for _ in range(n//n_threads):
            cursor.execute(query, params)
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()
    finally:
        cursor.close()
        connection_pool.putconn(connection)


def create_records(n=100000, n_threads=10):
    query = "INSERT INTO customers (first_name, last_name, email, phone_number, address) VALUES (%s, %s, %s, %s, %s)"
    params = generate_record()
    start = time.perf_counter()
    connection_pool = get_connection_pool()
    threads = []
    for _ in range(n_threads):
        thread = threading.Thread(target=insert_records, args=(n, n_threads, query, params, connection_pool))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print(threads)
    stop = time.perf_counter()
    print(f"Time of execution: {stop-start}")


if __name__ == '__main__':
    create_records()


from connection_pool import ConnectionPool
import time
import concurrent.futures


def execute_query_test(pool, thread):
    try:
        query = """SELECT * FROM employees;"""
        connection = pool.get_connection_from_pool()
        cursor = connection.cursor
        cursor.execute(query)
        result = cursor.fetchall()
        data = [
            dict(zip([key[0] for key in cursor.description], row)) for row in result
        ]
    except Exception as error:
        print(f"{thread}: {error}")
    else:
        pool.release_connection_to_pool(connection)
        print(f"{thread}: {data}")


connection_pool = ConnectionPool()


test_duration_sec = 15
num_threads = 8

start_time = time.time()

while (time.time() - start_time) < test_duration_sec:
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(execute_query_test, connection_pool, f"Thread-{x}")
            for x in range(num_threads)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as error:
                print(f"Exception in future: {error}")


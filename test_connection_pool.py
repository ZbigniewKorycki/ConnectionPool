import threading
from connection_pool import ConnectionPool
import time
import concurrent.futures


def test_execute_query(pool, thread):
    try:
        query = """select * FROM employees;"""
        connection = pool.get_connection_from_pool()
        cursor = connection.cursor
        cursor.execute(query)
        result = cursor.fetchall()
        data = [
            dict(zip([key[0] for key in cursor.description], row)) for row in result
        ]
    except Exception as e:
        print(f"Thread: {thread}, {e}")
    else:
        pool.release_connection_to_pool(connection)
        print(f"Thread: {thread}, Data:{data[0]}")
        print(f"Quantity of connections: {len(connection_pool.connection_pool)}")


if __name__ == "__main__":
    connection_pool = ConnectionPool()

    scheduler_thread = threading.Thread(target=connection_pool.run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    test_duration_sec = 300
    num_threads = 8
    start_time = time.time()

    while (time.time() - start_time) < test_duration_sec:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(test_execute_query, connection_pool, thread_nr)
                for thread_nr in range(num_threads)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    print(f"Exception in future: {error}")

    connection_pool.stop_scheduler()

def execute_query(self, query, params=None, fetch="all"):
    to_read = query.split()[0].upper() == "SELECT"
    connection = None
    try:
        connection = self.acquire_connection()
        cursor = connection.cursor()
        connection.autocommit = False
        cursor.execute(query, params)
        if to_read:
            if fetch == "one":
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            data = [
                dict(zip([key[0] for key in cursor.description], row))
                for row in result
            ]
        else:
            data = None
    except Exception as error:
        if connection:
            connection.rollback()
        return error
    else:
        print(connection)
        self.connections.append(connection)
        connection.commit()
        cursor.close()
        self.release_connection(connection)
        return data


def worker_function(self, query):
    try:
        data = self.execute_query(query)
        if data:
            print(data)
    except Exception as error:
        print(f"Error in worker_function: {error}")


def execute_queries_using_threads(self, queries, num_threads=5):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(self.worker_function, query) for query in queries
        ]
        for future in futures:
            future.result()
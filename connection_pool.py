import psycopg2
import psycopg2.pool
import psycopg2.extensions
from db_config import config
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

config_data = config()


class Connection:
    def __init__(
        self,
        database=config_data["database"],
        user=config_data["user"],
        password=config_data["password"],
        host=config_data["host"],
        port=config_data["port"],
    ):
        self.connection = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )


class ConnectionPool:
    def __init__(
        self,
        database=config_data["database"],
        user=config_data["user"],
        password=config_data["password"],
        host=config_data["host"],
        port=config_data["port"],
        min_connections=5,
        max_connections=100,
    ):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_pool = None
        self.num_available_connections = 0
        self.create_connection_pool()
        self.connections = list()

    def create_connection_pool(self):
        new_connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=self.min_connections,
            maxconn=self.max_connections,
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.num_available_connections = self.max_connections
        self.connection_pool = new_connection_pool

    def acquire_connection(self):
        if self.num_available_connections > 0:
            try:
                connection = self.connection_pool.getconn()
                if connection is not None:
                    self.num_available_connections -= 1
                    return connection
            except Exception as error:
                print(f"Error acquiring connection: {error}")
        else:
            raise Exception(
                "Max connections limit reached. Cannot acquire more connections."
            )

    def release_connection(self, connection):
        try:
            self.connection_pool.putconn(connection)
        except Exception as error:
            print(f"Error releasing connection: {error}")
        else:
            self.num_available_connections += 1

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

    def close_connection_pool(self):
        self.connection_pool.closeall()


if __name__ == "__main__":
    connection_pool = ConnectionPool()

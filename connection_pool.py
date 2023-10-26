import psycopg2
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
    def __init__(self, min_connections=5, max_connections=100):
        self.connection_pool = list()
        self.get_starting_connections(min_connections)

    def get_starting_connections(self, num_connections):
        for _ in range(num_connections):
            connection = Connection()
            self.connection_pool.append(connection)

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


    def close_connection_pool(self):
        self.connection_pool.closeall()


if __name__ == "__main__":
    connection_pool = ConnectionPool()

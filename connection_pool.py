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
        self.initiate_starting_connections(min_connections)
        self.min_connections = min_connections
        self.max_connections = max_connections

    def initiate_starting_connections(self, num_connections):
        for _ in range(num_connections):
            connection = Connection()
            self.connection_pool.append(connection)

    def create_connection(self):
        if len(self.connection_pool) < self.max_connections:
            connection = Connection()
            self.connection_pool.append(connection)
        else:
            print(f"Max connections ({self.max_connections}) limit reached. Cannot create more connections.")

    def get_connection(self):
        connection = self.connection_pool[0]
        self.connection_pool.remove(connection)
        return connection

    def return_connection(self, connection):
        self.connection_pool.append(connection)



if __name__ == "__main__":
    connection_pool = ConnectionPool()

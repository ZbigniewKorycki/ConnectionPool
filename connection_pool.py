import psycopg2
from db_config import config
import time
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import schedule

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
        self.conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )


class ConnectionPool:
    def __init__(self, min_connections=5, max_connections=100):
        self.connection_pool = list()
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.add_connections_to_minimum_amount()
        self.scheduled_job()
        self.lock = threading.Lock()

    def add_connections_to_minimum_amount(self):
        self.lock.acquire()
        try:
            while len(self.connection_pool) < self.min_connections:
                connection = self.create_connection()
                self.add_connection_to_pool(connection)
        finally:
            self.lock.release()

    def create_connection(self, max_retries=3):
        self.lock.acquire()
        try:
            if len(self.connection_pool) < self.max_connections:
                retries = 0
                while retries < max_retries:
                    try:
                        connection = Connection()
                    except Exception as error:
                        print(f"Error when creating new connection: {error}")
                        retries += 1
                    else:
                        return connection
                return None
            else:
                print(
                    f"Max connections ({self.max_connections}) limit reached. Cannot create more connections."
                )
        finally:
            self.lock.release()

    def add_connection_to_pool(self, connection):
        self.connection_pool.append(connection)

    def delete_connection_from_pool(self, connection):
        self.connection_pool.remove(connection)

    def get_connection_from_pool(self):
        connection = self.connection_pool[0]
        self.connection_pool.remove(connection)
        return connection

    def remove_inactive_connections_add_minimum_connections(self):
        print("checking")
        self.lock.acquire()
        try:
            for connection in self.connection_pool:
                if connection.conn.closed == 1 or connection.conn is None:
                    self.connection_pool.remove(connection)
            if len(self.connection_pool) < self.max_connections:
                self.add_connections_to_minimum_amount()
        finally:
            self.lock.release()

    def scheduled_job(self):
        schedule.every(1).minute.do(
            self.remove_inactive_connections_add_minimum_connections
        )


if __name__ == "__main__":
    connection_pool = ConnectionPool()

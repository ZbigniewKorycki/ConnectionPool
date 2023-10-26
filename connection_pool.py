import psycopg2
from db_config import config
import time
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Lock

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
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.add_connections_to_minimum_amount()
        self.lock = Lock()

    def add_connections_to_minimum_amount(self):
        self.lock.acquire()
        try:
            while len(self.connection_pool) < self.min_connections:
                connection = self.create_connection()
                self.add_connection_to_pool(connection)
        finally:
            self.lock.release()

    def create_connection(self, max_retries=3):
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

    def add_connection_to_pool(self, connection):
        self.lock.acquire()
        try:
            self.connection_pool.append(connection)
        finally:
            self.lock.release()

    def delete_connection_from_pool(self, connection):
        self.lock.acquire()
        try:
            self.connection_pool.remove(connection)
        finally:
            self.lock.release()

    def get_connection_from_pool(self):
        self.lock.acquire()
        try:
            connection = self.connection_pool[0]
            self.connection_pool.remove(connection)
            return connection
        finally:
            self.lock.release()

    def remove_inactive_connections(self):
        self.lock.acquire()
        try:
            for connection in self.connection_pool:
                if len(self.connection_pool) > self.min_connections:
                    if not connection.closed():
                        self.connection_pool.remove(connection)
        finally:
            self.lock.release()


if __name__ == "__main__":
    connection_pool = ConnectionPool()
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        connection_pool.remove_inactive_connections, "interval", minutes=1
    )
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

import psycopg2
from db_config import config
from threading import RLock
import schedule
import time

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
        self.cursor = self.conn.cursor()
        self.is_use = False


class ConnectionPool:
    def __init__(self, min_connections=5, max_connections=100):
        self.connection_pool = list()
        self.lock = RLock()
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.add_connections_to_minimum_quantity()

        schedule.every(5).minutes.do(self.manage_and_refresh_connections)

    def add_connections_to_minimum_quantity(self):
        self.lock.acquire()
        try:
            while len(self.connection_pool) < self.min_connections:
                self.add_connection_to_pool()
        finally:
            self.lock.release()

    def add_connection_to_pool(self):
        self.lock.acquire()
        try:
            to_add = False
            if len(self.connection_pool) < self.max_connections:
                to_add = True
            if to_add:
                try:
                    connection = Connection()
                except Exception as error:
                    print(f"Error when creating new connections: {error}")
                else:
                    self.connection_pool.append(connection)
            else:
                print(
                    f"Max connections ({self.max_connections}) limit reached. Cannot create more connections."
                )
        finally:
            self.lock.release()

    def get_connection_from_pool(self):
        self.lock.acquire()
        try:
            if len(self.connection_pool) < self.max_connections:
                self.add_connection_to_pool()
            available_connections = [connection for connection in self.connection_pool if connection.is_use is False]
            connection = available_connections[0]
        except Exception as error:
            print(f"Error when getting connection from pool: {error}")
        else:
            connection.is_use = True
            return connection
        finally:
            self.lock.release()

    def release_connection_to_pool(self, connection):
        self.lock.acquire()
        try:
            connection.is_use = False
        finally:
            self.lock.release()

    def manage_and_refresh_connections(self):
        active_connections = [connection for connection in self.connection_pool if connection.is_use is True]
        if len(active_connections) >= self.min_connections:
            self.connection_pool = active_connections
        else:
            self.connection_pool = active_connections
            self.add_connections_to_minimum_quantity()

    def run_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def stop_scheduler(self):
        print("Scheduler stopped")


if __name__ == "__main__":
    connection_pool = ConnectionPool()

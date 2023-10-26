import psycopg2
from db_config import config
import time
from apscheduler.schedulers.background import BackgroundScheduler

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
            print(
                f"Max connections ({self.max_connections}) limit reached. Cannot create more connections."
            )

    def get_connection(self):
        connection = self.connection_pool[0]
        self.connection_pool.remove(connection)
        return connection

    def release_connection(self, connection):
        self.connection_pool.append(connection)

    def delete_connection(self, connection):
        self.connection_pool.remove(connection)

    def remove_inactive_connections(self):
        print("checking")
        for connection in self.connection_pool:
            if len(self.connection_pool) > self.min_connections:
                if not connection.closed():
                    self.connection_pool.remove(connection)


if __name__ == "__main__":
    connection_pool = ConnectionPool()
    scheduler = BackgroundScheduler()
    scheduler.add_job(connection_pool.remove_inactive_connections, 'interval', minutes=1)
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

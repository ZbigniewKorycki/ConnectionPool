import psycopg2.pool
import psycopg2.extensions
from db_config import config

config_data = config()


class ConnectionPool:
    def __init__(self, database=config_data['database'], user=config_data['user'], password=config_data['password'],
                 host=config_data['host'], port=config_data['port'], min_connections=5, max_connections=100):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_pool = None
        self.available_connections = 0
        self.create_connection_pool()

    def create_connection_pool(self):
        new_connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=self.min_connections,
            maxconn=self.max_connections,
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        self.available_connections = self.max_connections
        self.connection_pool = new_connection_pool

    def acquire_connection(self):
        if self.available_connections > 0:
            connection = self.connection_pool.getconn()
            self.available_connections -= 1
            return connection
        else:
            raise Exception("Max connections limit reached. Cannot acquire more connections.")

    def release_connection(self, connection):
        self.connection_pool.putconn(connection)
        self.available_connections += 1

    def execute_query(self, query, params=None, fetch="all"):
        to_read = False
        try:
            connection = self.acquire_connection()
            cursor = connection.cursor()
            connection.autocommit = False
        except Exception as e:
            return e
        if query.split()[0].upper() == "SELECT":
            to_read = True
        try:
            cursor.execute(query, params)
            if to_read:
                if fetch == "one":
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()
                data = [dict(zip([key[0] for key in cursor.description], row)) for row in result]
        except (Exception, psycopg2.DatabaseError) as error:
            connection.rollback()
            print(error)
            return False
        else:
            connection.commit()
            if to_read:
                return data
            else:
                return True
        finally:
            cursor.close()
            self.release_connection(connection)

    def close_connection_pool(self):
        self.connection_pool.closeall()


if __name__ == '__main__':
    connection_pool = ConnectionPool()

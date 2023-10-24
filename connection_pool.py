import psycopg2.pool
import psycopg2.extensions
from db_config import config

config_data = config()


class ConnectionPool:
    def __init__(self, database=config_data['database'], user=config_data['user'], password=config_data['password'],
                 host=config_data['host'], port=config_data['port'], min_connections=5, max_connections=10):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_pool = None
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
        self.connection_pool = new_connection_pool

    def read_data(self, query, params=None, fetch=all):
        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        connection.autocommit = False
        try:
            cursor.execute(query, params)
            if fetch == "one":
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            data = [dict(zip([key[0] for key in cursor.description], row)) for row in result]
        except (Exception, psycopg2.DatabaseError) as error:
            connection.rollback()
            return error
        else:
            connection.commit()
            return data
        finally:
            cursor.close()
            self.connection_pool.putconn(connection)

    def write_data(self, query, params=None):
        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        connection.autocommit = False
        try:
            cursor.execute(query, params)
        except (Exception, psycopg2.DatabaseError) as error:
            connection.rollback()
            return False
        else:
            connection.commit()
            return True
        finally:
            cursor.close()
            self.connection_pool.putconn(connection)

    def close_connection_pool(self):
        self.connection_pool.closeall()


if __name__ == '__main__':
    connection_pool = ConnectionPool()

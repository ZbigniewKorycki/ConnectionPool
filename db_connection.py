import psycopg2
from db_config import config

config_data = config()


class PostgresSQLConnection:

    def __init__(self, dbname=config_data['database']):
        self.dbname = dbname
        self.user = config_data['user']
        self.password = config_data['password']
        self.host = config_data['host']
        self.port = config_data['port']
        self.connection = None

    def connect_with_db(self):
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)

    def execute_query(self, query, params=None, fetch="all"):
        if not self.connection:
            self.connect_with_db()
        self.connection.autocommit = False
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        try:
            if fetch == "all":
                fetched_data = cursor.fetchall()
            elif fetch == "one":
                fetched_data = cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None
        else:
            return fetched_data
        finally:
            cursor.close()

    def close_connection_with_db(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def database_transaction(self, query, params=None):
        try:
            self.connect_with_db()
            fetched_data = self.execute_query(query, params)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction. Reverting all other operations of a transaction ", error)
            self.connection.rollback()
            return False
        else:
            self.connection.commit()
            return fetched_data
        finally:
            self.close_connection_with_db()

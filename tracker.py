import psycopg2
from psycopg2.extensions import connection, cursor
from configparser import ConfigParser
from datetime import datetime


class Tracker:

    def __init__(self, name: str, config: ConfigParser) -> None:
        self._name = name
        self._config = config
        self._trade_date = config.get('PRODUCTION', 'ProcessingDate')
        self._connection: connection = self._get_db_connection()

    def update_job_status(self, status: str) -> None:
        job_id = self._assign_job_id()
        update_time = datetime.now()
        if self._connection is not None:
            insert_string = "INSERT into %s (job_id, update_time, status) VALUES (%s,%s,%s);"
            try:
                cur: cursor = self._connection.cursor()
                cur.execute(insert_string, self._config.get('PRODUCTION', 'TrackingTableName'),
                            job_id, update_time, status)
                self._connection.commit()
                cur.close()
            except psycopg2.Error as e:
                print("Error inserting status record: ", e.pgerror)
        else:
            print(f"Error: Connection unavailable for status record: {job_id} {update_time} {status}")

    def _assign_job_id(self) -> str:
        return self._name + self._trade_date

    def get_job_status(self, job_id: str):
        if self._connection is not None:
            query_string = "SELECT job_id, update_time, status FROM %s WHERE job_id = %s;"
            try:
                cur: cursor = self._connection.cursor()
                result = cur.execute(query_string, self._config.get('PRODUCTION', 'TrackingTableName'), job_id)
                self._connection.commit()
                cur.close()
                return result
            except psycopg2.Error as e:
                print("Error reading tracking table: ", e.pgerror)
        else:
            print("Error: Connection to tracking table not available")

    def _get_db_connection(self):
        try:
            conn: connection = psycopg2.connect(database=self._config.get('DATABASE', 'Database'),
                                                host=self._config.get('DATABASE', 'Host'),
                                                user=self._config.get('DATABASE', 'User'),
                                                password=self._config.get('DATABASE', 'Password')
                                                )
            return conn
        except psycopg2.Error as e:
            print("Error creating connection: ", e.pgerror)
            return None

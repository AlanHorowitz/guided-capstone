from typing import Tuple
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
            insert_string = f"INSERT into {self._config.get('PRODUCTION', 'TrackingTableName')}" + \
                            " (job_id, update_time, status) VALUES (%s,%s,%s);"
            try:
                cur: cursor = self._connection.cursor()
                cur.execute(insert_string, (job_id, update_time, status))
                self._connection.commit()
                cur.close()
            except psycopg2.Error as e:
                print("Error inserting status record: ", e.pgerror)
        else:
            print(f"Error: Connection unavailable for status record: {job_id} {update_time} {status}")

    def _assign_job_id(self) -> str:
        return self._name + '_' + self._trade_date

    def get_job_status(self, job_id: str) -> Tuple:
        if self._connection is not None:
            query_string = f"SELECT job_id, update_time, status FROM {self._config.get('PRODUCTION', 'TrackingTableName')}" + \
                           " WHERE job_id = %s;"
            try:
                cur: cursor = self._connection.cursor()
                cur.execute(query_string, (job_id,))
                result = cur.fetchone()
                self._connection.commit()
                cur.close()
                return result
            except psycopg2.Error as e:
                print("Error reading tracking table: ", e.pgerror)
                return None
        else:
            print("Error: Connection to tracking table not available")
            return None

    def _get_db_connection(self):
        try:
            conn: connection = psycopg2.connect(database=self._config.get('PRODUCTION', 'Database'),
                                                host=self._config.get('PRODUCTION', 'Host'),
                                                user=self._config.get('PRODUCTION', 'User'),
                                                password=self._config.get('PRODUCTION', 'Password')
                                                )
            return conn
        except psycopg2.Error as e:
            print("Error creating connection: ", e.pgerror)
            return None

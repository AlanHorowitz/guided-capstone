import psycopg2
import configparser
from datetime import datetime


class Tracker:

    def __init__(self, name, config):
        self._name = name
        self._config = config
        parser = configparser.ConfigParser()
        parser.read(config)
        self._trade_date = parser.get('PRODUCTION', 'ProcessingDate')
        self._connection = psycopg2.connect(database=parser.get('DATABASE', 'Database'),
                                            host=parser.get('DATABASE', 'Host'),
                                            user=parser.get('DATABASE', 'User'),
                                            password=parser.get('DATABASE', 'Password')
                                            )

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        update_time = datetime.now()

        pass

    def assign_job_id(self):
        return self._name + self._trade_date

    def get_job_status(self):
        pass

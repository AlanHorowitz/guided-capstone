import sys
import configparser
from reports import Reports
from tracker import Tracker

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Error: Use run_reports.py config_file")
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    trade_ingestion_tracker = Tracker('reports_etl', config)
    try:
        Reports(config).run()
        trade_ingestion_tracker.update_job_status("success")
    except Exception as e:
        print(e)
        trade_ingestion_tracker.update_job_status("failed")

import sys
import configparser
from ingestion import Ingestion
from tracker import Tracker

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Error: Use run_ingestion.py config_file")
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    ingestion_tracker = Tracker('ingestion', config)
    try:
        Ingestion(config).ingest()
        ingestion_tracker.update_job_status("success")
    except Exception as e:
        print(e)
        ingestion_tracker.update_job_status("failed")



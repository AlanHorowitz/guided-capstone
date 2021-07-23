import sys
import configparser
from equity_market_data_analysis.ingestion import Ingestion
from equity_market_data_analysis.tracker import Tracker

if len(sys.argv) == 2:
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    ingestion_tracker = Tracker('ingestion', config)
    try:
        Ingestion(config).ingest()
        ingestion_tracker.update_job_status("success")
    except Exception as e:
        print(e)
        ingestion_tracker.update_job_status("failed")
else:
    print("Error: Use run_ingestion.py config_file")



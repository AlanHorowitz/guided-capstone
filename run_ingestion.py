import sys
from ingestion import Ingestion
from tracker import Tracker

if __name__ == '__main__':
    if len(sys.argv != 2):
        print("Error: Use run_ingestion.py config_file")
    config = sys.argv[1]
    trade_date = config.get('production', 'processing_date')
    trade_ingestion_tracker = Tracker('ingestion_etl', config)
    try:
        Ingestion(config).ingest()
        trade_ingestion_tracker.update_job_status("success")
    except Exception as e:
        print(e)
        trade_ingestion_tracker.update_job_status("failed")



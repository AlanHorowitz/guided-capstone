import sys
import configparser
from equity_market_data_analysis import Reports
from equity_market_data_analysis import Tracker

if len(sys.argv) == 2:
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    reports_tracker = Tracker('reports', config)
    try:
        Reports(config).run()
        reports_tracker.update_job_status("success")
    except Exception as e:
        print(e)
        reports_tracker.update_job_status("failed")
else:
    print("Error: Use run_reports.py config_file")

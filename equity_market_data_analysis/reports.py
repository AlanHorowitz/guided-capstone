from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date
from .eod_report import EODReport
from equity_market_data_analysis.analytical_report import AnalyticalReport


class Reports:
    """  Produce eod_report and analytical_report from staging data for processing date """
    def __init__(self, config):
        self._config = config
        self._output_container = config.get('APP_CONFIG', 'OutputContainer')
        self._staging_dir = config.get('APP_CONFIG', 'StagingDir')
        try:
            processing_date = config.get('PRODUCTION', 'ProcessingDate')
            self._processing_date = date.fromisoformat(processing_date)
        except ValueError:
            raise ValueError(f"Invalid ProcessingDate in config.ini {processing_date}")

    def run(self):

        spark = SparkSession.builder.master('local').appName('guided_capstone').getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", 5)  # avoid unneeded shuffling
        trade_common_df = spark.read.parquet(self._output_container + "/" + self._staging_dir)
        trade_common_df.cache()

        EODReport(self._config).run(trade_common_df.where(col('trade_dt') == self._processing_date))
        AnalyticalReport(self._config).run(trade_common_df, spark)





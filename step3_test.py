from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T
from datetime import date, datetime
from decimal import Decimal
from step3 import apply_latest

TEST_SCHEMA = StructType([StructField("trade_dt", T.DateType(), False),
                          StructField("symbol", T.StringType(), False),
                          StructField("exchange", T.StringType(), False),
                          StructField("event_tm", T.TimestampType(), False),
                          StructField("event_seq_nb", T.IntegerType(), False),
                          StructField("arrival_tm", T.TimestampType(), False),
                          StructField("trade_pr", T.DecimalType(10, 2), True)]
                         )


def test_apply_latest():
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()

    records = ((date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
                1, datetime(2021, 6, 1, 8, 30, 0, 0), Decimal(100.00)),
               (date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
                1, datetime(2021, 6, 1, 8, 45, 0, 0), Decimal(101.00)),
               (date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
                1, datetime(2021, 6, 1, 8, 35, 0, 0), Decimal(99.00)),
               (date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
                2, datetime(2021, 6, 1, 10, 30, 0, 0), Decimal(104.00)),
               (date(2021, 6, 2), 'IBM', 'NYSE', datetime(2021, 6, 2, 8, 0, 0, 0),
                1, datetime(2021, 6, 2, 8, 30, 0, 0), Decimal(150.00)),
               (date(2021, 6, 2), 'IBM', 'NYSE', datetime(2021, 6, 2, 8, 0, 0, 0),
                1, datetime(2021, 6, 2, 8, 40, 0, 0), Decimal(160.00))
               )

    latest_records = (
        (date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
         1, datetime(2021, 6, 1, 8, 45, 0, 0), Decimal(101.00)),
        (date(2021, 6, 1), 'MSFT', 'NYSE', datetime(2021, 6, 1, 8, 0, 0, 0),
         2, datetime(2021, 6, 1, 10, 30, 0, 0), Decimal(104.00)),
        (date(2021, 6, 2), 'IBM', 'NYSE', datetime(2021, 6, 2, 8, 0, 0, 0),
         1, datetime(2021, 6, 2, 8, 40, 0, 0), Decimal(160.00))
    )

    df = spark.createDataFrame(records, schema=TEST_SCHEMA)
    df_applied = apply_latest(df)

    assert set(df_applied.collect()) == set(latest_records)

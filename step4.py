from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType
from datetime import date
import os

spark = SparkSession.builder.master('local').appName('guided-capstone').getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 4)  # avoid unneeded shuffling

INPUT_DIRECTORY = "output_dir"
file_input = 'file://' + os.getcwd() + '/' + INPUT_DIRECTORY

trade_common_df = spark.read.parquet(file_input)

# create trades table
trade_common_df.where(trade_common_df.partition == 'T') \
    .createOrReplaceTempView("trades")

# create quotes_extended with null moving average column
trade_common_df.where(trade_common_df.partition == 'Q') \
    .withColumn('mov_avg_trade_pr', lit(None).cast(DecimalType(10, 2))) \
    .createOrReplaceTempView("quotes_extended")

# extend trades with 30 day moving average column
spark.sql(
    """
    SELECT *, 
       mean(trade_pr) OVER  
       (
           PARTITION BY symbol, exchange ORDER BY event_tm
           RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
       ) AS mov_avg_trade_pr 
    FROM trades;
    """
).createOrReplaceTempView("trades_extended")

# combine two extended tables
spark.sql(
    """
    SELECT * FROM trades_extended
    UNION
    SELECT * from quotes_extended
    """
).createOrReplaceTempView("quotes_trades_extended")

# use ignoreNulls option of last function to populate prior trade price and moving average
spark.sql(
    """
    SELECT *,
        last(trade_pr, True) OVER 
            (PARTITION BY symbol, exchange ORDER  BY event_tm) AS prior_trade_pr,
        last(mov_avg_trade_pr, True) OVER
            (PARTITION BY symbol, exchange ORDER BY event_tm) AS prior_mov_avg_trade_pr
    FROM quotes_trades_extended;
    """
).createOrReplaceTempView("quotes_trades_extended")

# Compute closing trade price from prior day
spark.sql(
    """
    SELECT trade_dt,
           symbol, 
           exchange,
           lag(close_trade_pr, 1) OVER 
               (PARTITION BY symbol, exchange ORDER BY trade_dt) AS prior_close_trade_pr
    FROM 
    (
        SELECT trade_dt,
               symbol,
               exchange,           
               first(trade_pr) OVER 
                   (PARTITION BY symbol, exchange, trade_dt ORDER BY event_tm DESC) 
                   AS close_trade_pr,
               row_number() OVER
                   (PARTITION BY symbol, exchange, trade_dt ORDER BY event_tm DESC) AS row
        from trades
    ) a
    WHERE row = 1;
    """
).createOrReplaceTempView("prior_day_close")

# add prior close price, filter out trades, format and write output
spark.sql(
    """
    SELECT /*+ BROADCAST(b) */ 
       a.trade_dt,
       a.symbol, 
       a.exchange, 
       a.event_tm, 
       a.prior_trade_pr, 
       cast(a.prior_mov_avg_trade_pr as decimal(2)),
       b.prior_close_trade_pr 
    FROM quotes_trades_extended a
    JOIN prior_day_close b 
    WHERE
       a.trade_dt = b.trade_dt AND
       a.symbol = b.symbol AND
       a.exchange = b.exchange AND
       rec_type = 'Q'
    ORDER BY symbol, exchange, event_tm;
    """
).write.partitionBy('trade_dt').mode('overwrite').parquet('test_output_dir')
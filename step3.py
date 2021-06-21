from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, countDistinct
import pyspark.sql.functions as f


def apply_latest(df):
    return df


spark = SparkSession.builder.master('local').appName('app').getOrCreate()
trade_common = spark.read.parquet("output_dir/partition=T")

trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")

trade.createOrReplaceTempView('trade_v')
r = spark.sql("select trade_dt from trade_v")
r.show(5)
trade_corrected = apply_latest(trade)

trade_common.show(5, False)
print(trade_common.count())
trade_common.printSchema()

gr = trade_common.groupBy(col('trade_dt'), col('symbol'), col('exchange'), col('event_tm'))

gr.agg(f.max('arrival_tm').alias('arrival_tm'), f.avg("ask_pr")).show(5)

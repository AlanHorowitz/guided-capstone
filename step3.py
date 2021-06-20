from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, countDistinct

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
trade_common = spark.read.parquet("output_dir/partition=Q")

trade_common.show(5, False)
print(trade_common.count())
trade_common.printSchema()

trade_common.groupBy(col('trade_dt'), col('symbol'), col('exchange'), col('event_tm'))\
    .agg(count('arrival_tm').alias('arr')).orderBy(desc('arr')).show(10)

trade_common.select(countDistinct(col('event_tm'))).show()
from pyspark.sql import SparkSession, Window
from datetime import date
import pyspark.sql.functions as f


def apply_latest(df):

    key_partition = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb")
    max_column = "arrival_tm"

    return df.withColumn('tmp', f.max(max_column).over(key_partition)) \
        .filter(f.col('tmp') == f.col(max_column)) \
        .drop(f.col('tmp'))


spark = SparkSession.builder.master('local').appName('app').getOrCreate()

trade_common_df = spark.read.parquet("output_dir/partition=T")
trade_df = trade_common_df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb",
                                  "arrival_tm", "trade_pr")
quote_common_df = spark.read.parquet("output_dir/partition=Q")
quote_df = trade_common_df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb",
                                  "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

dataframes = {'trade': trade_df,
              'quote': quote_df}

for df_name in dataframes:
    cloud_storage_path = f'wasbs://test@guidedcapstonesa.blob.core.windows.net'
    apply_latest(dataframes[df_name]) \
        .write.mode('overwrite').parquet(f"{cloud_storage_path}/{df_name}/{df_name}_dt={date.today()}")

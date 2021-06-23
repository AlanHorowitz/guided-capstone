from pyspark.sql import SparkSession, Window
from datetime import date
import pyspark.sql.functions as f


def apply_latest(df):
    """ Return Quote or Trade dataframe filtering for most recent arrival per key. """
    key_partition = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb")
    max_column = "arrival_tm"

    return df.withColumn('tmp', f.max(max_column).over(key_partition)) \
        .filter(f.col('tmp') == f.col(max_column)) \
        .drop(f.col('tmp'))


if __name__ == "__main__":
    """ Recreate Quote and Trade dataframes, filter out-of-date records, and write to cloud storage."""
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 5)  # avoid unneeded shuffling

    trade_common_df = spark.read.parquet("output_dir/partition=T")
    trade_df = trade_common_df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb",
                                      "arrival_tm", "trade_pr")
    quote_common_df = spark.read.parquet("output_dir/partition=Q")
    quote_df = trade_common_df.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb",
                                      "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

    dataframes = {'trade': trade_df,
                  'quote': quote_df}

    for df_name in dataframes:
        cloud_storage_path = "wasbs://test@guidedcapstonesa.blob.core.windows.net"
        apply_latest(dataframes[df_name]) \
            .write.mode('overwrite') \
            .parquet(f"{cloud_storage_path}/{df_name}/{df_name}_dt={date.today()}")

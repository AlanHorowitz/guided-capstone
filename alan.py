#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json


def get_common_event(record: dict):
    common_data = ('trade_dt', 'rec_type', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm',
                   'trade_pr', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size', 'partition')

    return [record.get(k, "") for k in common_data]


def parse_csv(line: str):
    pass


def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']
    record_keys = set(record.keys())

    def translate(s):
        translations = {'event_type': 'rec_type', 'file_tm': 'arrival_tm', 'price': 'trade_pr'}
        return s if s not in translations else translations[s]

    quote_columns = {'trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                     'exchange', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size'}

    trade_columns = {'trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                     'exchange', 'price', 'size', 'execution_id'}

    is_valid_record = (record_type == 'Q' and record_keys == quote_columns) or (
            record_type == 'T' and record_keys == trade_columns)

    if is_valid_record:
        translated_record = {translate(k): v for k, v in record.items()}
        translated_record['partition'] = record_type
        return get_common_event(translated_record)
    else:
        translated_record = dict()
        translated_record['partition'] = 'B'
        translated_record['line'] = line
        return get_common_event(translated_record)


def get_local_files():

    local_files = ["file:///home/alan/guided-capstone/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.json",
                   "file:///home/alan/guided-capstone/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.json",
                   "file:///home/alan/guided-capstone/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.csv",
                   "file:///home/alan/guided-capstone/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.csv"]
    return local_files


if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    sc = spark.sparkContext

    string_schema = StructType([StructField("trade_dt", T.StringType(), False),
                                StructField("rec_type", T.StringType(), False),
                                StructField("symbol", T.StringType(), False),
                                StructField("exchange", T.StringType(), False),
                                StructField("event_tm", T.StringType(), False),
                                StructField("event_seq_nb", T.StringType(), False),
                                StructField("arrival_tm", T.StringType(), False),
                                StructField("trade_pr", T.StringType(), False),
                                StructField("bid_pr", T.StringType(), False),
                                StructField("bid_size", T.StringType(), False),
                                StructField("ask_pr", T.StringType(), False),
                                StructField("ask_size", T.StringType(), False),
                                StructField("partition", T.StringType(), False)]
                               )

    common_schema = StructType([StructField("trade_dt", T.DateType(), False),
                                StructField("rec_type", T.StringType(), False),
                                StructField("symbol", T.StringType(), False),
                                StructField("exchange", T.StringType(), False),
                                StructField("event_tm", T.TimestampType(), False),
                                StructField("event_seq_nb", T.IntegerType(), False),
                                StructField("arrival_tm", T.TimestampType(), False),
                                StructField("trade_pr", T.DecimalType(10, 2), False),
                                StructField("bid_pr", T.DecimalType(10, 2), False),
                                StructField("bid_size", T.IntegerType(), False),
                                StructField("ask_pr", T.DecimalType(10, 2), False),
                                StructField("ask_size", T.IntegerType(), False),
                                StructField("partition", T.StringType(), False)]
                               )

    all_data = spark.createDataFrame([], common_schema)  # empty dataframe to accumulate all files

    for file_location in get_local_files():
        raw = sc.textFile(file_location)
        print(file_location[file_location.rfind('.'):])

        if file_location[file_location.rfind('.'):] == '.json':
            parser = parse_json
        elif file_location[file_location.rfind('.'):] == '.csv':
            parser = parse_csv
            continue
        else:
            print("unsupported file type")
            continue

        parsed = raw.map(lambda line: parser(line))
        data = spark.createDataFrame(parsed, schema=string_schema)
        data.show(4)

        data = data.withColumn('trade_dt', F.col('trade_dt').cast(T.DateType()))
        data = data.withColumn('event_tm', F.col('event_tm').cast(T.TimestampType()))
        data = data.withColumn('arrival_tm', F.col('arrival_tm').cast(T.TimestampType()))
        data = data.withColumn('event_seq_nb', F.col('event_seq_nb').cast(T.IntegerType()))
        data = data.withColumn('bid_size', F.col('bid_size').cast(T.IntegerType()))
        data = data.withColumn('ask_size', F.col('ask_size').cast(T.IntegerType()))
        data = data.withColumn('trade_pr', F.col('trade_pr').cast(T.DecimalType(10, 2)))
        data = data.withColumn('bid_pr', F.col('bid_pr').cast(T.DecimalType(10, 2)))
        data = data.withColumn('ask_pr', F.col('ask_pr').cast(T.DecimalType(10, 2)))

        print(data.count())
        data.printSchema()
        all_data = all_data.union(data)

    print(all_data.count())
    all_data.printSchema()
    all_data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

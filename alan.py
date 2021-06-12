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


spark = SparkSession.builder.master('local').appName('app').getOrCreate()

sc = spark.sparkContext

raw = sc.textFile("file:///home/alan/guided-capstone/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt")

parsed = raw.map(lambda line: parse_json(line))

struct1 = StructType([StructField("trade_dt", T.DateType(), False),
                      StructField("rec_type", T.StringType(), False),
                      StructField("symbol", T.StringType(), False),
                      StructField("exchange", T.StringType(), False),
                      StructField("event_tm", T.TimestampType(), False),
                      StructField("event_seq_nb", T.IntegerType(), False),
                      StructField("arrival_tm", T.TimestampType(), False),
                      StructField("trade_pr", T.DecimalType(), True),
                      StructField("bid_pr", T.DecimalType(), False),
                      StructField("bid_size", T.IntegerType(), False),
                      StructField("ask_pr", T.DecimalType(), False),
                      StructField("ask_size", T.IntegerType(), False),
                      StructField("partition", T.StringType(), False)]
                     )

struct2 = StructType([StructField("trade_dt", T.StringType(), False),
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

data = spark.createDataFrame(parsed, schema=struct2)

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


data.show(4)

data.printSchema()

data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

# In[ ]:

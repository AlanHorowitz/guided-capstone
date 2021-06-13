#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json

QUOTE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size')

TRADE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'price', 'size', 'execution_id')

COMMON_COLUMNS = ('trade_dt', 'rec_type', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm',
                  'trade_pr', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size', 'partition')

STRING_SCHEMA = StructType([StructField("trade_dt", T.StringType(), True),
                            StructField("rec_type", T.StringType(), False),
                            StructField("symbol", T.StringType(), False),
                            StructField("exchange", T.StringType(), False),
                            StructField("event_tm", T.StringType(), False),
                            StructField("event_seq_nb", T.StringType(), False),
                            StructField("arrival_tm", T.StringType(), False),
                            StructField("trade_pr", T.StringType(), True),
                            StructField("bid_pr", T.StringType(), True),
                            StructField("bid_size", T.StringType(), True),
                            StructField("ask_pr", T.StringType(), True),
                            StructField("ask_size", T.StringType(), True),
                            StructField("partition", T.StringType(), False)]
                           )

COMMON_SCHEMA = StructType([StructField("trade_dt", T.DateType(), True),
                            StructField("rec_type", T.StringType(), False),
                            StructField("symbol", T.StringType(), False),
                            StructField("exchange", T.StringType(), False),
                            StructField("event_tm", T.TimestampType(), False),
                            StructField("event_seq_nb", T.IntegerType(), False),
                            StructField("arrival_tm", T.TimestampType(), False),
                            StructField("trade_pr", T.DecimalType(10, 2), True),
                            StructField("bid_pr", T.DecimalType(10, 2), True),
                            StructField("bid_size", T.IntegerType(), True),
                            StructField("ask_pr", T.DecimalType(10, 2), True),
                            StructField("ask_size", T.IntegerType(), True),
                            StructField("partition", T.StringType(), False)]
                           )


def translate(s):
    translations = {'event_type': 'rec_type', 'file_tm': 'arrival_tm', 'price': 'trade_pr'}
    return s if s not in translations else translations[s]


def get_common_event(record: dict):
    return [record.get(k, "XXX") for k in COMMON_COLUMNS]


def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(',')
    record_type = record[record_type_pos]

    if record_type == 'Q' and len(record) == len(QUOTE_COLUMNS):
        translated_record = {translate(c): record[i] for i, c in enumerate(QUOTE_COLUMNS)}
        translated_record['partition'] = record_type
        return get_common_event(translated_record)
    elif record_type == 'T' and len(record) == len(TRADE_COLUMNS):
        translated_record = {translate(c): record[i] for i, c in enumerate(TRADE_COLUMNS)}
        translated_record['partition'] = record_type
        return get_common_event(translated_record)
    else:
        translated_record = dict()
        translated_record['partition'] = 'B'
        translated_record['line'] = line
        return get_common_event(translated_record)


def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']
    record_keys = set(record.keys())
    is_valid_record = (record_type == 'Q' and record_keys == set(QUOTE_COLUMNS)) or (
                       record_type == 'T' and record_keys == set(TRADE_COLUMNS))

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


def cast_to_common_schema(df):
    df = df.withColumn('trade_dt', F.col('trade_dt').cast(T.DateType()))
    df = df.withColumn('event_tm', F.col('event_tm').cast(T.TimestampType()))
    df = df.withColumn('arrival_tm', F.col('arrival_tm').cast(T.TimestampType()))
    df = df.withColumn('event_seq_nb', F.col('event_seq_nb').cast(T.IntegerType()))
    df = df.withColumn('bid_size', F.col('bid_size').cast(T.IntegerType()))
    df = df.withColumn('ask_size', F.col('ask_size').cast(T.IntegerType()))
    df = df.withColumn('trade_pr', F.col('trade_pr').cast(T.DecimalType(10, 2)))
    df = df.withColumn('bid_pr', F.col('bid_pr').cast(T.DecimalType(10, 2)))
    df = df.withColumn('ask_pr', F.col('ask_pr').cast(T.DecimalType(10, 2)))
    return df


if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    sc = spark.sparkContext
    all_data = spark.createDataFrame([], COMMON_SCHEMA)  # empty dataframe to accumulate all files

    for file_location in get_local_files():
        raw = sc.textFile(file_location)
        if file_location[file_location.rfind('.'):] == '.json':
            parser = parse_json
        elif file_location[file_location.rfind('.'):] == '.csv':
            parser = parse_csv
        else:
            print("unsupported file type")
            continue
        parsed = raw.map(lambda line: parser(line))
        data = spark.createDataFrame(parsed, schema=STRING_SCHEMA)
        data = cast_to_common_schema(data)
        all_data = all_data.union(data)

    empty_td = all_data.filter(F.col('trade_dt').isNull()).count()
    print("Empty Dates", empty_td)
    print(all_data.count())
    all_data.select('trade_dt').orderBy('trade_dt').show(20)
    # all_data.printSchema()
    # all_data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import date, datetime
from decimal import Decimal
import json
import dateutil.parser


QUOTE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size')

TRADE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'price', 'size')

COMMON_SCHEMA = StructType([StructField("trade_dt", T.DateType(), False),
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


def get_error_event(struct_type: StructType):
    error_event_row = []
    for struct_field in struct_type:

        d = struct_field.jsonValue()
        field_type = d['type']
        field_name = d['name']
        if field_name == 'partition':
            converted_field = 'B'
        elif field_type == 'date':
            converted_field = date(2000, 1, 1)
        elif field_type == 'timestamp':
            converted_field = datetime(2000, 1, 1, 0, 0, 0, 0)
        elif field_type == 'integer':
            converted_field = -1
        elif field_type.startswith('decimal'):
            converted_field = Decimal(-1.0)
        elif field_type == 'string':
            converted_field = 'INVALID'
        error_event_row.append(converted_field)
    if len(error_event_row) != 13:
        print("error event row", error_event_row)
        raise ValueError
    return error_event_row


def set_value_from_record(record, field_type):
    if field_type == 'date':
        converted_field = date.fromisoformat(record.strip())

    elif field_type == 'timestamp':
        converted_field = dateutil.parser.parse(record.strip())

    elif field_type == 'integer':
        if isinstance(record, str):
            converted_field = int(record.strip())
        else:
            converted_field = record

    elif field_type.startswith('decimal'):
        if isinstance(record, str):
            converted_field = Decimal(record.strip())
        else:
            converted_field = Decimal.from_float(record)

    elif field_type == 'string':
        converted_field = record.strip()
    return converted_field


def get_common_event(record: dict, struct_type: StructType):
    """ Validated event.  NonNull fields must be populated; conversions to correct data formats must succeed"""
    
    common_event_row = []
    try:
        for struct_field in struct_type:

            d = struct_field.jsonValue()
            field_name = d['name']
            field_type = d['type']
            field_nullable = d['nullable']

            if field_name in record:
                converted_field = set_value_from_record(record[field_name], field_type)
            elif field_nullable:
                converted_field = None
            else:
                raise ValueError(f'NonNullable field missing from record {record}')

            common_event_row.append(converted_field)
    except:
        print(common_event_row)
        raise RuntimeError

    return common_event_row


def parse_csv(line: str):
    try:
        record_type_pos = 2
        record = line.split(',')
        record_type = record[record_type_pos]

        if record_type == 'Q' and len(record) == len(QUOTE_COLUMNS):
            translated_record = {translate(c): record[i] for i, c in enumerate(QUOTE_COLUMNS)}
            translated_record['partition'] = record_type
            return get_common_event(translated_record, COMMON_SCHEMA)
        elif record_type == 'T' and len(record) == len(TRADE_COLUMNS):
            translated_record = {translate(c): record[i] for i, c in enumerate(TRADE_COLUMNS)}
            translated_record['partition'] = record_type
            return get_common_event(translated_record, COMMON_SCHEMA)
        else:
            return get_error_event(COMMON_SCHEMA)
    except ValueError:
        return get_error_event(COMMON_SCHEMA)


def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']

    if record_type == 'Q' or record_type == 'T':
        translated_record = {translate(k): v for k, v in record.items()}
        translated_record['partition'] = record_type
        return get_common_event(translated_record, COMMON_SCHEMA)
    else:
        return get_error_event(COMMON_SCHEMA)


def get_local_files():
    local_files = ["file:///home/alan/guided-capstone/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.json",
                   "file:///home/alan/guided-capstone/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.json",
                   "file:///home/alan/guided-capstone/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.csv",
                   "file:///home/alan/guided-capstone/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.csv"]
    return local_files


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
        parsed = raw.map(parser)
        data = spark.createDataFrame(parsed, schema=COMMON_SCHEMA)
        all_data = all_data.union(data)

    all_data.printSchema()
    all_data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

#!/usr/bin/env python
# coding: utf-8
"""
Guided Capstone Step 2

Read json and csv files from a directory, conform them to a common schema, and write the output to parquet,
partitioned by event type.
"""

from datetime import date, datetime
from decimal import Decimal
import json
import os
from typing import List, Union

import dateutil.parser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T


QUOTE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size')

TRADE_COLUMNS = ('trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm', 'event_seq_nb',
                 'exchange', 'price', 'size')

COLUMN_MAPPINGS = {'event_type': 'rec_type', 'file_tm': 'arrival_tm', 'price': 'trade_pr'}

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
                            StructField("partition", T.StringType(), False),
                            StructField("line", T.StringType(), True)]
                           )

INPUT_DIRECTORY = 'step2-input'


def get_local_files() -> str:
    """Yield a URL for each file in input directory"""
    for entry in os.listdir(INPUT_DIRECTORY):
        if os.path.isfile(os.path.join(INPUT_DIRECTORY, entry)):
            yield 'file://' + os.getcwd() + '/' + INPUT_DIRECTORY + '/' + entry


def map_column(col: str) -> str:
    """Map column name from csv and json to common schema name"""
    return col if col not in COLUMN_MAPPINGS else COLUMN_MAPPINGS[col]


def convert_value_to_common_type(value: Union[str, int, float], value_type: str) -> Union[str, int, Decimal, date, datetime]:
    """
    Convert value to its required type. From csv, str is expected.  From json: string, int and float.

    Conversion functions Raise ValueError on failure.
    """
    if value_type == 'date':
        converted_value = date.fromisoformat(value.strip())
    elif value_type == 'timestamp':
        converted_value = dateutil.parser.parse(value.strip())
    elif value_type == 'integer':
        if isinstance(value, int):  # json
            converted_value = int(value)
        else:
            converted_value = int(value.strip())
    elif value_type.startswith('decimal'):
        if isinstance(value, float):  # json
            converted_value = Decimal.from_float(value)
        else:
            converted_value = Decimal(value.strip())
    elif value_type == 'string':
        converted_value = value.strip()

    return converted_value


def common_event(record_dict: dict, struct_type: StructType, partition: str) -> List[Union[str, int, Decimal, date, datetime]]:
    """ Validated event.  NonNull fields must be populated; conversions to correct data formats must succeed"""
    # dictionary contains columns appearing in json or csv
    common_event_row = []
    try:
        for struct_field in struct_type:
            d = struct_field.jsonValue()
            field_name = d['name']
            field_type = d['type']
            field_nullable = d['nullable']

            if field_name == 'partition':
                value = partition
            elif field_name in record_dict:
                value = convert_value_to_common_type(record_dict[field_name], field_type)
            elif field_nullable:
                value = None
            else:
                raise ValueError(f'NonNullable field missing from record {record_dict}')
            common_event_row.append(value)

        return common_event_row

    except Exception:
        raise ValueError(f'Value conversion error from record {record_dict}')


def error_event(struct_type: StructType, partition: str, line: str) -> List[Union[str, int, Decimal, date, datetime]]:
    """Return an error event containing default values and the input record to the specified partition"""
    error_event_row = []
    for struct_field in struct_type:

        d = struct_field.jsonValue()
        field_type = d['type']
        field_name = d['name']

        if field_name == 'partition':
            value = partition
        elif field_name == 'line':
            value = line
        elif field_type == 'date':
            value = date(2000, 1, 1)
        elif field_type == 'timestamp':
            value = datetime(2000, 1, 1, 0, 0, 0, 0)
        elif field_type == 'integer':
            value = -1
        elif field_type.startswith('decimal'):
            value = Decimal(-1.0)
        elif field_type == 'string':
            value = 'INVALID'

        error_event_row.append(value)

    return error_event_row


def parse_csv(line: str) -> List[Union[str, int, Decimal, date, datetime]]:
    try:
        record_type_pos = 2
        record = line.split(',')
        record_type = record[record_type_pos]

        if record_type == 'Q' and len(record) == len(QUOTE_COLUMNS):
            record_dict = {map_column(c): record[i] for i, c in enumerate(QUOTE_COLUMNS)}
            return common_event(record_dict, COMMON_SCHEMA, 'Q')
        elif record_type == 'T' and len(record) == len(TRADE_COLUMNS):
            record_dict = {map_column(c): record[i] for i, c in enumerate(TRADE_COLUMNS)}
            return common_event(record_dict, COMMON_SCHEMA, 'T')
        else:
            raise ValueError(f'Unknown record type or incorrect number of fields from record {record}')

    except ValueError:
        return error_event(COMMON_SCHEMA, 'B', line)


def parse_json(line: str) -> List[Union[str, int, Decimal, date, datetime]]:
    try:
        record = json.loads(line)
        record_type = record['event_type']

        if record_type == 'Q' or record_type == 'T':
            translated_record = {map_column(k): v for k, v in record.items()}
            return common_event(translated_record, COMMON_SCHEMA, record_type)
        else:
            raise ValueError(f'Unknown record type from record {record}')

    except ValueError:
        return error_event(COMMON_SCHEMA, 'B', line)


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
            print("Warning: unsupported file type detected")
            continue
        parsed = raw.map(parser)
        data = spark.createDataFrame(parsed, schema=COMMON_SCHEMA)
        all_data = all_data.union(data)

    all_data.printSchema()
    all_data.show(50, truncate=False)
    print("Combined record count", all_data.count())
    all_data.write.partitionBy('partition').mode('overwrite').parquet('output_dir')

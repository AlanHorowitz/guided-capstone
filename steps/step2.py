#!/usr/bin/env python
"""
Guided Capstone Step 2

Use Apache Spark RDD and dataframe APIs to read trade and quote data from csv and json sources, conform them
to a common schema, and write the output to parquet.
"""

from datetime import date, datetime
from decimal import Decimal
import json
import os
from typing import List, Dict, Union

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

input_files = [
    'part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.csv',
    'part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.json',
    'part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.csv',
    'part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.json'
]


def get_input_files() -> str:
    """Yield a URL for each file in the input directory"""
    cloud_input_path = "wasbs://data@guidedcapstonesa.blob.core.windows.net/"
    for filename in input_files:
        yield cloud_input_path + filename


def map_column(col: str) -> str:
    """Map column names used by quote and trade schemas to common schema names"""
    return col if col not in COLUMN_MAPPINGS else COLUMN_MAPPINGS[col]


def convert_value_to_common_type(value: Union[str, int, float], target_type: str) -> Union[str, int, Decimal, date, datetime]:
    """
    Convert a value to its required type. From csv, str is expected.  From json: str, int and float.

    Args:
       value: Value to be converted to the common type: string, int, and float are what the data currently present
       target_type: target type as shown by dataFrame.printSchema() for COMMON_SCHEMA

    Returns:
       converted_value: value of target_type

    Raises:
       ValueError if any of the conversion functions fail.
    """
    if target_type == 'date':
        converted_value = date.fromisoformat(value.strip())
    elif target_type == 'timestamp':
        converted_value = dateutil.parser.parse(value.strip())  # date.datetime.fromisoformat() too rigid
    elif target_type == 'integer':
        if isinstance(value, int):  # json
            converted_value = int(value)
        else:
            converted_value = int(value.strip())
    elif target_type.startswith('decimal'):
        if isinstance(value, float):  # json
            converted_value = Decimal.from_float(value)
        else:
            converted_value = Decimal(value.strip())
    elif target_type == 'string':
        converted_value = value.strip()

    return converted_value


def common_event(record_dict: Dict[str, Union[str, int, float]], schema: StructType, partition: str) -> List[Union[str, int, Decimal, date, datetime]]:
    """
    Create a format-validated record matching the supplied schema.

    Args:
       record_dict: A dictionary of key/value pairs representing a quote or trade record
       schema: the StructType of the target format
       partition: A partition field to be added to the output record

    Returns:
       common_event_row: List representing record in in schema format.

    Raises:
       ValueError if any of conversion function fails or a non-nullable field is missing from the record.
    """

    common_event_row = []
    try:
        # visit the target schema fields in order
        for struct_field in schema:
            field_name = struct_field.jsonValue()['name']
            field_type = struct_field.jsonValue()['type']
            field_nullable = struct_field.jsonValue()['nullable']

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


def error_event(schema: StructType, partition: str, line: str) -> List[Union[str, int, Decimal, date, datetime]]:
    """
    Create a format-validated error record matching the supplied schema.

    Args:
       schema: the StructType of the target format
       partition: A partition field to be added to the output record
       line: The raw text of the record that wasn't successfully parsed

    Returns:
       error_event_row: List representing error record in schema format. Hardcoded error values are used for data fields.
    """
    error_event_row = []
    for struct_field in schema:
        field_name = struct_field.jsonValue()['name']
        field_type = struct_field.jsonValue()['type']

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
    """
    Parse a line of text in csv format, setting partition column as record type.

    :param line: A line of comma delimited text
    :return: Record in format of COMMON_SCHEMA. May be successfully parsed data or Error record.
    """
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
    """
    Parse a line of text in json format, setting partition column as record type.

    :param line: A line of one json record
    :return: Record in format of COMMON_SCHEMA. May be successfully parsed data or Error record.
    """
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
    """
    Guided Capstone data ingestion.  Read json and csv input files, transform data into a common schema
    and persist in parquet format. 
    """
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    sc = spark.sparkContext
    all_data = spark.createDataFrame([], COMMON_SCHEMA)  # empty dataframe to accumulate all files

    for file_location in get_input_files():
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

    cloud_output_path = "wasbs://output@guidedcapstonesa.blob.core.windows.net/"
    all_data.write.partitionBy('partition').mode('overwrite').parquet(cloud_output_path + 'stage')

import json
from datetime import datetime

from pyspark.sql import functions as func
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, FloatType, DecimalType, LongType

from util.constants import GENERIC_VALIDATE_RAW_VALUE_INDEX, GENERIC_VALIDATE_COLUMN_NAME_INDEX, \
    GENERIC_VALIDATE_SCENARIO_JSON_INDEX, GENERIC_VALIDATE_ROW_ID_VALUE_INDEX, GENERIC_SOFT_DELETE_ROW_VALUE_INDEX, \
    GENERIC_SOFT_DELETE_VALUE_T0_DELETE_INDEX, GENERIC_SOFT_DELETED_VALUE_INDEX
from util.reflection_util import class_for_name
from util.scenario_util import find_field_type, find_field_type_map_as, find_mapped_as_value, get_id_field_name
from util.type_checkers import is_int, is_timestamp, is_float, is_decimal, is_blank, is_not_blank

generic_validate_udf = func.udf(
    lambda x: generic_validate(x[GENERIC_VALIDATE_RAW_VALUE_INDEX], x[GENERIC_VALIDATE_SCENARIO_JSON_INDEX],
                               x[GENERIC_VALIDATE_COLUMN_NAME_INDEX], x[GENERIC_VALIDATE_ROW_ID_VALUE_INDEX]))

generic_soft_delete_udf = func.udf(
    lambda x: generic_soft_delete(x[GENERIC_SOFT_DELETE_ROW_VALUE_INDEX], x[GENERIC_SOFT_DELETE_VALUE_T0_DELETE_INDEX],
                                  x[GENERIC_SOFT_DELETED_VALUE_INDEX]))

REFINER_MODULE_NAME = 'etl.refiner'
REFINER_LOGGER_NAME = 'logger'


def get_boolean(s: str) -> bool:
    return s.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'yea', 'certainly', 'uh-huh']


def get_day_partition_name_and_year(date_str):
    dt = datetime.strptime(str(date_str), '%Y-%m-%d')
    partition_name = dt.strftime('%Y%m%d')
    year = dt.year

    return partition_name, year


def valid_default(string):
    return string


def is_valid(primitive_type):
    return {
        'integer': is_int,
        'double': is_float,
        'long': is_int,
        'timestamp': is_timestamp,
        'float': is_float,
        'decimal': is_decimal,
    }.get(primitive_type, valid_default)


def to_spark_type(primitive_type: str):
    if primitive_type == 'integer':
        return IntegerType()
    elif primitive_type == 'long':
        return LongType()
    elif primitive_type == 'double':
        return DoubleType()
    elif primitive_type.startswith('timestamp'):
        return TimestampType()
    elif primitive_type == 'float':
        return FloatType()
    elif primitive_type == 'decimal':
        return DecimalType()
    else:
        return StringType()


def generic_cast(scenario_json, column_name: str):
    field_type = find_field_type(scenario_json, column_name)
    return to_spark_type(field_type)


def generic_cast_map_as(scenario_json, column_name: str):
    field_type = find_field_type_map_as(scenario_json, column_name)
    return to_spark_type(field_type)


def generic_map_as(scenario_json, column_name: str):
    map_as = find_mapped_as_value(scenario_json, column_name)
    return map_as


def generic_validate(raw, scenario_json, column_name: str, row_id_value):
    if raw is None:
        return None

    if is_blank(str(raw)):
        return None

    scenario_json = json.loads(scenario_json)
    field_type = find_field_type(scenario_json, column_name)

    ts_source_type_format = None
    ts_destination_type_format = None
    raw = str(raw).strip()

    if str(field_type).startswith('timestamp'):
        type_and_format = str(field_type).split('|')
        field_type = type_and_format[0]
        ts_source_type_format = type_and_format[1]
        ts_destination_type_format = type_and_format[2]

    if is_valid(field_type)(raw):
        if field_type == 'timestamp':
            del scenario_json
            return datetime.strptime(raw, ts_source_type_format).strftime(ts_destination_type_format)
        del scenario_json
        return raw

    logger = class_for_name(REFINER_MODULE_NAME, REFINER_LOGGER_NAME)
    logger.warning(f'On column {column_name} with {get_id_field_name(scenario_json)}: {row_id_value} '
                   f'has corrupted value. {raw} cannot be parsable to Spark {field_type}. '
                   f'Null value is assigned for that cell.')
    del scenario_json
    return None


def generic_soft_delete(row_value, id_value_to_delete, soft_deleted_value):
    if str(row_value) == str(id_value_to_delete) and soft_deleted_value is None:
        logger = class_for_name(REFINER_MODULE_NAME, REFINER_LOGGER_NAME)

        soft_deleted = datetime.today().strftime('%Y-%m-%d')
        logger.info(f'Record found. Soft-delete applied as {soft_deleted}')
        return soft_deleted

        # x_years_old = 3
        # x_years_ago = datetime.now() - relativedelta(years=x_years_old)
        # logger.info(f'Record found. Soft-delete applied as {x_years_ago.strftime("%Y-%m-%d")}')
        # return x_years_ago.strftime('%Y-%m-%d')

    if soft_deleted_value is not None and is_not_blank(soft_deleted_value):
        return datetime.strptime(str(soft_deleted_value), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    return None

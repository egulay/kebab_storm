from datetime import datetime

# Retention demo only: uncomment together with the backdated block in generic_soft_delete.
# from dateutil.relativedelta import relativedelta

from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, \
    LongType, ShortType, StringType, TimestampType

from util.constants import GENERIC_SOFT_DELETE_ROW_VALUE_INDEX, \
    GENERIC_SOFT_DELETE_VALUE_T0_DELETE_INDEX, GENERIC_SOFT_DELETED_VALUE_INDEX
from util.data_type_util import AtomicTypeSpec, NUMERIC_TYPES, TEMPORAL_TYPES, option_values, parse_data_type, \
    to_spark_datetime_format
from util.reflection_util import class_for_name
from util.scenario_util import find_field_type_map_as
from util.type_checkers import is_not_blank

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


def to_spark_atomic_type(data_type: AtomicTypeSpec):
    if data_type.name == 'byte':
        return ByteType()
    elif data_type.name == 'short':
        return ShortType()
    elif data_type.name == 'integer':
        return IntegerType()
    elif data_type.name == 'long':
        return LongType()
    elif data_type.name == 'double':
        return DoubleType()
    elif data_type.name == 'timestamp':
        return TimestampType()
    elif data_type.name == 'date':
        return DateType()
    elif data_type.name == 'boolean':
        return BooleanType()
    elif data_type.name == 'float':
        return FloatType()
    elif data_type.name == 'decimal':
        return DecimalType(data_type.precision or 10, data_type.scale or 0)
    else:
        return StringType()


def to_spark_type(type_definition: str):
    return to_spark_atomic_type(parse_data_type(type_definition).target)


def native_convert(column, type_definition: str):
    conversion = parse_data_type(type_definition)
    normalized = func.trim(column.cast(StringType()))
    normalized = func.when(normalized == '', func.lit(None)).otherwise(normalized)
    source_value = _parse_source_value(normalized, conversion)
    return _convert_source_to_target(source_value, normalized, conversion)


def conversion_error_condition(column, type_definition: str):
    normalized = func.trim(column.cast(StringType()))
    converted = native_convert(column, type_definition)
    return normalized.isNotNull() & (normalized != '') & converted.isNull()


def _parse_source_value(normalized, conversion):
    source = conversion.source
    if source.name == 'string':
        return normalized
    if source.name == 'boolean':
        return _parse_boolean(normalized, conversion)
    if source.name in TEMPORAL_TYPES:
        parsed = _parse_timestamp(normalized, conversion.source_format)
        return parsed.cast(to_spark_atomic_type(source))
    return normalized.try_cast(to_spark_atomic_type(source))


def _convert_source_to_target(source_value, normalized, conversion):
    source_name = conversion.source.name
    target_name = conversion.target.name
    target_type = to_spark_atomic_type(conversion.target)

    if target_name == 'string':
        if source_name in TEMPORAL_TYPES and conversion.destination_format:
            return func.date_format(source_value, to_spark_datetime_format(conversion.destination_format))
        if source_name == 'boolean':
            return func.when(source_value, func.lit(conversion.options.get('true_value', 'true'))) \
                .when(~source_value, func.lit(conversion.options.get('false_value', 'false'))) \
                .otherwise(func.lit(None).cast(StringType()))
        if conversion.destination_format and source_name in NUMERIC_TYPES:
            return func.when(source_value.isNull(), func.lit(None).cast(StringType())) \
                .otherwise(func.format_string(conversion.destination_format, source_value))
        return source_value.cast(StringType())

    if target_name in NUMERIC_TYPES:
        if source_name in TEMPORAL_TYPES:
            return _timestamp_to_epoch(source_value, conversion.options['epoch_unit']).try_cast(target_type)
        if source_name == 'boolean':
            numeric_boolean = func.when(source_value, func.lit(1)).when(~source_value, func.lit(0))
            return numeric_boolean.try_cast(target_type)
        return source_value.try_cast(target_type)

    if target_name in TEMPORAL_TYPES:
        if source_name in NUMERIC_TYPES:
            timestamp = _epoch_to_timestamp(source_value, conversion.options['epoch_unit'])
        elif source_name == 'string':
            timestamp = _parse_timestamp(normalized, conversion.source_format)
        else:
            timestamp = source_value.cast(TimestampType())
        return timestamp.cast(target_type)

    if target_name == 'boolean':
        if source_name == 'string':
            return _parse_boolean(normalized, conversion)
        if source_name in NUMERIC_TYPES:
            return func.when(source_value == 1, func.lit(True)) \
                .when(source_value == 0, func.lit(False)) \
                .otherwise(func.lit(None).cast(BooleanType()))
        return source_value.cast(BooleanType())

    return source_value.try_cast(target_type)


def _parse_boolean(value, conversion):
    true_values = option_values(conversion, 'true_values', ('true', '1', 't', 'y', 'yes'))
    false_values = option_values(conversion, 'false_values', ('false', '0', 'f', 'n', 'no'))
    lowered = func.lower(value)
    return func.when(lowered.isin(*true_values), func.lit(True)) \
        .when(lowered.isin(*false_values), func.lit(False)) \
        .otherwise(func.lit(None).cast(BooleanType()))


def _parse_timestamp(value, source_format):
    spark_format = to_spark_datetime_format(source_format, flexible_width=True)
    return func.try_to_timestamp(value, func.lit(spark_format)) if spark_format \
        else value.try_cast(TimestampType())


def _timestamp_to_epoch(value, epoch_unit):
    timestamp = value.cast(TimestampType())
    return {
        'seconds': func.unix_seconds,
        'milliseconds': func.unix_millis,
        'microseconds': func.unix_micros,
    }[epoch_unit](timestamp)


def _epoch_to_timestamp(value, epoch_unit):
    return {
        'seconds': func.timestamp_seconds,
        'milliseconds': func.timestamp_millis,
        'microseconds': func.timestamp_micros,
    }[epoch_unit](value)


def generic_cast_map_as(scenario_json, column_name: str):
    field_type = find_field_type_map_as(scenario_json, column_name)
    return to_spark_type(field_type)


def generic_soft_delete(row_value, id_value_to_delete, soft_deleted_value):
    if str(row_value) == str(id_value_to_delete) and soft_deleted_value is None:
        logger = class_for_name(REFINER_MODULE_NAME, REFINER_LOGGER_NAME)

        soft_deleted = datetime.today().strftime('%Y-%m-%d')
        logger.info(f'Record with id {id_value_to_delete} found. Soft-delete applied as {soft_deleted}')
        return soft_deleted

        # Retention demo only: uncomment these lines and comment the current-date block above.
        # x_years_old = 3
        # x_years_ago = datetime.now() - relativedelta(years=x_years_old)
        # logger.info(f'Record found. Soft-delete applied as {x_years_ago.strftime("%Y-%m-%d")}')
        # return x_years_ago.strftime('%Y-%m-%d')

    if soft_deleted_value is not None and is_not_blank(soft_deleted_value):
        return datetime.strptime(str(soft_deleted_value), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    return None

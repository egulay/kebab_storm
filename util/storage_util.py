import logging
import re

from dateutil.parser import parse as dt_parse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, StructField, StructType

from conf import settings
from util.constants import DATE_IMPORTED_FIELD_NAME
from util.parquet_save_mode import ParquetSaveMode
from util.parquet_util import read_spark_parquet, read_spark_parquets_by_dates, write_spark_parquet
from util.type_checkers import is_timestamp

logger = logging.getLogger(__name__)

ICEBERG = 'iceberg'


def is_iceberg_storage() -> bool:
    return settings.storage_provider == ICEBERG


def storage_description() -> str:
    return 'Iceberg table backed by Parquet' if is_iceberg_storage() else 'raw Parquet'


def iceberg_table_name(logical_location: str) -> str:
    relative_location = str(logical_location)
    data_root = str(settings.default_data_location).rstrip('/')
    if relative_location.startswith(data_root):
        relative_location = relative_location[len(data_root):]
    table_name = re.sub(r'[^a-zA-Z0-9_]+', '_', relative_location.strip('/')).strip('_').lower()
    if not table_name:
        raise ValueError(f'Cannot derive an Iceberg table name from location: {logical_location!r}')
    return f'{settings.iceberg_catalog}.{settings.iceberg_namespace}.{table_name}'


def read_storage(session: SparkSession, logical_location: str) -> DataFrame:
    if is_iceberg_storage():
        table_name = iceberg_table_name(logical_location)
        if session.catalog.tableExists(table_name):
            return session.table(table_name)
        return _empty_data_frame(session)
    return read_spark_parquet(session, logical_location)


def read_storage_by_dates(session: SparkSession, start_date, end_date, logical_location: str) -> DataFrame:
    if not is_iceberg_storage():
        return read_spark_parquets_by_dates(session, start_date, end_date, logical_location)
    if not is_timestamp(start_date) or not is_timestamp(end_date):
        logger.error(f'{start_date} and/or {end_date} cannot be parsed as a timestamp')
        return _empty_data_frame(session)
    data = read_storage(session, logical_location)
    if data.isEmpty():
        return data
    start_day = dt_parse(start_date).strftime('%Y%m%d')
    end_day = dt_parse(end_date).strftime('%Y%m%d')
    return data.filter(func.col(DATE_IMPORTED_FIELD_NAME).between(start_day, end_day))


def write_storage(source: DataFrame, logical_location: str, save_mode: ParquetSaveMode, *partition_by):
    if not is_iceberg_storage():
        write_spark_parquet(source, logical_location, save_mode, *partition_by)
        return

    table_name = iceberg_table_name(logical_location)
    session = source.sparkSession
    exists = session.catalog.tableExists(table_name)
    if not exists:
        _create_iceberg_namespace(session)
        writer = _iceberg_writer(source, table_name, partition_by)
        writer.create()
        return

    writer = source.writeTo(table_name)
    if save_mode is ParquetSaveMode.append:
        writer.append()
    elif save_mode is ParquetSaveMode.overwrite:
        writer.overwritePartitions()
    elif save_mode is ParquetSaveMode.ignore:
        logger.info(f'Iceberg table {table_name} already exists; write ignored')
    else:
        raise ValueError(f'Iceberg table already exists: {table_name}')


def replace_storage(source: DataFrame, logical_location: str, *partition_by):
    if not is_iceberg_storage():
        write_spark_parquet(source, logical_location, ParquetSaveMode.overwrite, *partition_by)
        return

    table_name = iceberg_table_name(logical_location)
    session = source.sparkSession
    if not session.catalog.tableExists(table_name):
        _create_iceberg_namespace(session)
        _iceberg_writer(source, table_name, partition_by).create()
        return
    source.writeTo(table_name).overwrite(func.lit(True))


def _iceberg_writer(source: DataFrame, table_name: str, partition_by):
    writer = source.writeTo(table_name) \
        .using('iceberg') \
        .tableProperty('format-version', '2') \
        .tableProperty('write.format.default', 'parquet')
    if partition_by:
        writer = writer.partitionedBy(*(func.col(column_name) for column_name in partition_by))
    return writer


def _create_iceberg_namespace(session: SparkSession):
    session.sql(f'CREATE NAMESPACE IF NOT EXISTS {settings.iceberg_catalog}.{settings.iceberg_namespace}')


def _empty_data_frame(session: SparkSession) -> DataFrame:
    return session.createDataFrame([], StructType([StructField('no data', StringType(), True)]))

import logging
from datetime import timedelta

from dateutil.parser import parse as dt_parse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from util.constants import DAY_PARTITION_FIELD_NAME, EXCEPTION_TEMPLATE
from util.parquet_save_mode import ParquetSaveMode
from util.type_checkers import is_timestamp

logger = logging.getLogger(__name__)


def _path_and_file_system(session: SparkSession, path: str):
    hadoop_path = session.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    file_system = hadoop_path.getFileSystem(session.sparkContext._jsc.hadoopConfiguration())
    return hadoop_path, file_system


def write_spark_parquet(source: DataFrame, save_path: str, save_mode: ParquetSaveMode, *partition_by):
    if partition_by:
        source.write \
            .mode(save_mode.value) \
            .partitionBy(partition_by) \
            .parquet(save_path)
    else:
        source.write \
            .mode(save_mode.value) \
            .parquet(save_path)


def read_spark_parquet(session: SparkSession, parquet_path: str):
    try:
        if filesystem_path_exists(session, parquet_path):
            return session.read.parquet(parquet_path)

        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))
    except Exception as e:
        logger.error(EXCEPTION_TEMPLATE.format(type(e).__name__, e.args))
        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))


def read_spark_parquets_by_dates(session: SparkSession, start_date, end_date, parquet_root_path: str):
    try:
        if not is_timestamp(start_date) or not is_timestamp(end_date):
            logger.error(f'{start_date} and/or {end_date} cannot be parsable to timestamp')
            return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))

        start = dt_parse(start_date)
        end = dt_parse(end_date)
        delta = end - start

        paths = []
        for i in range(delta.days + 1):
            day = (start + timedelta(days=i)).strftime("%Y%m%d")
            paths.append(str(f'{parquet_root_path}/{DAY_PARTITION_FIELD_NAME}={day}'))

        available_parquets = [path for path in paths if filesystem_path_exists(session, path)]

        if available_parquets:
            return session.read.parquet(*available_parquets)

        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))
    except Exception as e:
        logger.error(EXCEPTION_TEMPLATE.format(type(e).__name__, e.args))
        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))


def filesystem_path_exists(session: SparkSession, path: str) -> bool:
    hadoop_path, fs = _path_and_file_system(session, path)
    return fs.exists(hadoop_path)


def filesystem_delete(session: SparkSession, path: str):
    hadoop_path, fs = _path_and_file_system(session, path)
    if fs.exists(hadoop_path) and not fs.delete(hadoop_path, True):
        raise OSError(f'Filesystem refused to delete path: {path}')


def delete_input_file(session: SparkSession, input_path: str):
    """Delete one exact source file through its Hadoop filesystem URI."""
    hadoop_path, fs = _path_and_file_system(session, input_path)
    if not fs.exists(hadoop_path):
        raise FileNotFoundError(f'Input file no longer exists: {input_path}')
    if not fs.getFileStatus(hadoop_path).isFile():
        raise IsADirectoryError(f'Input path must identify one file: {input_path}')
    if not fs.delete(hadoop_path, False):
        raise OSError(f'Filesystem refused to delete input file: {input_path}')


def filesystem_get_partitioned_folder_paths(session: SparkSession, scenario_save_location):
    scenario_path, fs = _path_and_file_system(session, scenario_save_location)
    statuses = fs.listStatus(scenario_path.getParent())
    scenario_name = str(scenario_path.getName())
    return [str(item.getPath()) for item in statuses
            if item.isDirectory() and _partitioned_folder_year(item.getPath().getName(), scenario_name) is not None]


def filesystem_get_folder_years_less_than(session: SparkSession, scenario_save_location, less_than_year: int):
    scenario_path, fs = _path_and_file_system(session, scenario_save_location)
    statuses = fs.listStatus(scenario_path.getParent())
    scenario_name = str(scenario_path.getName())
    years = [_partitioned_folder_year(item.getPath().getName(), scenario_name)
             for item in statuses if item.isDirectory()]
    return [str(year) for year in years if year is not None and year <= less_than_year]


def _partitioned_folder_year(folder_name, scenario_name):
    prefix = f'{scenario_name}_'
    name = str(folder_name)
    suffix = name[len(prefix):] if name.startswith(prefix) else ''
    return int(suffix) if len(suffix) == 4 and suffix.isdigit() else None

from datetime import datetime
from datetime import timedelta

from dateutil.parser import parse as dt_parse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from conf import settings
from util.constants import DAY_PARTITION_FIELD_NAME, EXCEPTION_TEMPLATE
from util.logger import get_logger
from util.parquet_save_mode import ParquetSaveMode
from util.type_checkers import is_timestamp

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)


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


def write_hive_table(table_name: str, source: DataFrame, save_path: str, save_mode: ParquetSaveMode, *partition_by):
    if partition_by:
        source.write.saveAsTable(table_name, format='parquet', mode=save_mode.value, partitionBy=partition_by,
                                 path=save_path)
    else:
        source.write.saveAsTable(table_name, format='parquet', mode=save_mode.value, path=save_path)


def read_spark_parquet(session: SparkSession, parquet_path: str):
    try:
        if hdfs_path_exist(session, parquet_path):
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

        available_parquets = [path for path in paths if hdfs_path_exist(session, path)]

        if available_parquets:
            return session.read.parquet(*available_parquets)

        return session.createDataFrame(session.sparkContext.emptyRDD,
                                       StructType([StructField("no data", StringType(), True)]))
    except Exception as e:
        logger.error(EXCEPTION_TEMPLATE.format(type(e).__name__, e.args))
        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))


def read_all_spark_parquet(session: SparkSession, parquet_root_path: str):
    try:
        fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            session.sparkContext._jsc.hadoopConfiguration())
        statuses = fs.listStatus(session.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(parquet_root_path))
        parquets = [str(f'{parquet_root_path}/{item.getPath().getName()}') for item in statuses if item.isDirectory()]

        if parquets:
            return session.read.parquet(*parquets)

        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))
    except Exception as e:
        logger.error(EXCEPTION_TEMPLATE.format(type(e).__name__, e.args))
        return session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))


def hdfs_path_exist(session: SparkSession, path: str) -> bool:
    fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        session.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(session.sparkContext._jvm.org.apache.hadoop.fs.Path(path))


def hdfs_delete(session: SparkSession, path: str):
    if hdfs_path_exist(session, path):
        fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            session.sparkContext._jsc.hadoopConfiguration())
        fs.delete(session.sparkContext._jvm.org.apache.hadoop.fs.Path(path))


def hdfs_rename_or_move(session: SparkSession, source_path: str, destination_path: str):
    fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        session.sparkContext._jsc.hadoopConfiguration())
    fs.rename(session.sparkContext._jvm.org.apache.hadoop.fs.Path(source_path),
              session.sparkContext._jvm.org.apache.hadoop.fs.Path(destination_path))


def hdfs_get_folder_names(session: SparkSession, scenario_save_location):
    fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        session.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.listStatus(session.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(settings.default_data_location))

    result = [item.getPath().getName() for item in statuses if
              item.isDirectory() and str(item.getPath().getName()).strip().startswith(
                  str(scenario_save_location.replace(settings.default_data_location, '').replace('/', '')).strip())]
    return result


def hdfs_get_folder_years(session: SparkSession, scenario_save_location):
    fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        session.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.listStatus(session.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path())

    result = [str(item.getPath().getName()).split('_')[-1] for item in statuses if
              item.isDirectory() and str(item.getPath().getName()).strip().startswith(
                  str(scenario_save_location.replace(settings.default_data_location, '').replace('/', '')).strip())]
    return result


def hdfs_get_folder_years_less_than(session: SparkSession, scenario_save_location, less_than_year: int):
    fs = session.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        session.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.listStatus(
        session.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(settings.default_data_location))

    result = [str(item.getPath().getName()).split('_')[-1] for item in statuses if
              item.isDirectory() and str(item.getPath().getName()).strip().startswith(
                  str(scenario_save_location.replace(settings.default_data_location, '').replace('/',
                                                                                                 '')).strip()) and int(
                  str(item.getPath().getName()).split('_')[-1]) <= less_than_year]
    return result

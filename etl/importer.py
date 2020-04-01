import itertools
import json
from datetime import datetime

import numpy as np
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as func, DataFrame, SparkSession
from pyspark.sql.types import BinaryType, TimestampType, StructType, StructField, StringType

from conf import settings
from etl.crypter import execute_crypto_action
from etl.refiner import validate_and_refine
from util.constants import DAY_PARTITION_FIELD_NAME, DATE_IMPORTED_FIELD_NAME, SOFT_DELETED_FIELD_NAME, \
    YEAR_IMPORTED_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.crypto.crypto_util import generic_decrypt_udf, generic_encrypt_udf
from util.etl_util import generic_cast_map_as, generic_soft_delete_udf, get_day_partition_name_and_year
from util.logger import get_logger
from util.parquet_save_mode import ParquetSaveMode
from util.parquet_util import write_spark_parquet, write_hive_table, read_spark_parquet, \
    hdfs_delete, hdfs_get_folder_names, hdfs_get_folder_years_less_than
from util.scenario_util import get_scenario_defaults, get_missing_fields, find_mapped_as_value, \
    load_scenario, find_field_is_encrypted

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)


async def get_reporting_data(spark_session: SparkSession, scenario_json_path: str,
                             crypto_action: CryptoAction,
                             incl_soft_deleted: bool, day) -> DataFrame:
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Get reporting data for {name} entity defined in {scenario_json_path} '
                f'on day={partition_name}')

    if is_apply_year_to_save_location:
        save_location = f'{save_location}_{year}'

    parquet_path = f'{save_location}/{DAY_PARTITION_FIELD_NAME}={partition_name}'

    spark_session.sparkContext.setJobDescription(f'Get reporting data for {name}')
    data = read_spark_parquet(spark_session, parquet_path)

    if data.rdd.isEmpty():
        logger.error('No data found')
        return spark_session.createDataFrame(spark_session.sparkContext.emptyRDD,
                                             StructType([StructField("no data", StringType(), True)]))

    if not incl_soft_deleted:
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NULL')

    return data if crypto_action == CryptoAction.encrypt else execute_crypto_action(data, scenario_json_path,
                                                                                    CryptoAction.decrypt)


async def hard_delete(spark_session: SparkSession, scenario_json_path):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    logger.info(f'Active action: Hard delete for {name} entity defined in {scenario_json_path}')
    spark_session.sparkContext.setJobDescription(f'Hard delete for {name}')

    x_years_ago = datetime.now() - relativedelta(years=hard_delete_in_years)
    folder_years_less_than_x = hdfs_get_folder_years_less_than(spark_session, save_location, x_years_ago.year)
    all_data = None
    if not is_apply_year_to_save_location:
        all_data = read_spark_parquet(spark_session, save_location)
    else:
        for year in folder_years_less_than_x:
            if all_data is not None and not all_data.rdd.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{save_location}_{year}')
                if not u_data.rdd.isEmpty():
                    all_data = all_data.union(u_data)
            else:
                all_data = read_spark_parquet(spark_session, f'{save_location}_{year}')

    if all_data is None or all_data.rdd.isEmpty():
        logger.error(f'No data found')
        exit(1)

    all_data = all_data.withColumn(
        DAY_PARTITION_FIELD_NAME, func.col(DATE_IMPORTED_FIELD_NAME))

    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - find soft deleted')
    soft_deleted_data = all_data.filter(
        func.col(SOFT_DELETED_FIELD_NAME) <= func.unix_timestamp(
            func.lit(x_years_ago.strftime('%Y-%m-%d %H:%M:%S'))).cast(TimestampType()))

    if soft_deleted_data.rdd.isEmpty():
        logger.warning(f'No data found for earlier than {x_years_ago.strftime("%Y-%m-%d")}')
        return

    logger.info(f'Total count for soft deleted data earlier than {x_years_ago.strftime("%Y-%m-%d")} is '
                f'{str(soft_deleted_data.count())}')
    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - exclude soft deleted')
    new_data = all_data.exceptAll(soft_deleted_data)
    logger.info(f'Overwriting partitions')

    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - overwrite partitions')
    if str(save_type).startswith('parquet'):
        write_spark_parquet(new_data, temp_save_location,
                            ParquetSaveMode.overwrite,
                            DAY_PARTITION_FIELD_NAME)
        logger.info(f'Save as {save_type} finished into {temp_save_location} for hard delete')

        if not is_apply_year_to_save_location:
            write_spark_parquet(read_spark_parquet(spark_session, temp_save_location), save_location,
                                ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(f'Save as {save_type} finished into {save_location} '
                        f'with overwrite for hard delete')
        else:
            for year in folder_years_less_than_x:
                write_spark_parquet(read_spark_parquet(spark_session, temp_save_location).where(
                    f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{save_location}_{year}',
                    ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
                logger.info(f'Save as {save_type} finished into {save_location}_{year} '
                            f'with overwrite for hard delete')

        hdfs_delete(spark_session, temp_save_location)
        logger.info(f'{temp_save_location} is cleaned up')
    elif str(save_type).startswith('table'):
        table_name = str(save_type).split('|')[1]
        temp_table_name = f"{str(save_type).split('|')[1]}_temp"
        s_type = str(save_type).split('|')[0]
        write_hive_table(temp_table_name, new_data, temp_save_location, ParquetSaveMode.overwrite,
                         DAY_PARTITION_FIELD_NAME)
        logger.info(
            f'Save as {s_type} finished into {table_name} table on {temp_save_location} for hard delete')

        if not is_apply_year_to_save_location:
            write_hive_table(table_name, read_spark_parquet(spark_session, temp_save_location), save_location,
                             ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(
                f'Save as {s_type} finished into {table_name} table on {save_location} with overwrite for hard delete')
        else:
            for year in folder_years_less_than_x:
                write_hive_table(f'{table_name}_{year}',
                                 read_spark_parquet(spark_session, temp_save_location).where(
                                     f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{save_location}_{year}',
                                 ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
                logger.info(
                    f'Save as {s_type} finished into {table_name} table on {save_location}_{year} with overwrite '
                    f'for hard delete')

        hdfs_delete(spark_session, temp_save_location)
        logger.info(f'{temp_save_location} is cleaned up')
    else:
        logger.error(f'Save mode {save_type} is not identified for {name} '
                     f'entity. Please check scenario JSON located in {scenario_json_path}.')
        exit(1)


async def soft_delete(spark_session: SparkSession, scenario_json_path, id_value_to_soft_delete):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    scenario = load_scenario(scenario_json_path)
    mapped_id_field_name = find_mapped_as_value(scenario, id_field_name)
    is_id_field_encrypted = find_field_is_encrypted(scenario, id_field_name)
    logger.info(
        f'Active action: Soft delete on {name} entity defined in {scenario_json_path} for '
        f'{mapped_id_field_name}={id_value_to_soft_delete}')
    spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - '
                                                 f'{mapped_id_field_name}={id_value_to_soft_delete}')

    found_in_parquets = None
    if not is_apply_year_to_save_location:
        found_in_parquets = read_spark_parquet(spark_session, save_location)
    else:
        for directory in hdfs_get_folder_names(spark_session, save_location):
            if found_in_parquets is not None and not found_in_parquets.rdd.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{settings.default_data_location}/{directory}')
                if not u_data.rdd.isEmpty():
                    found_in_parquets = found_in_parquets.union(u_data)
            else:
                found_in_parquets = read_spark_parquet(spark_session,
                                                       f'{settings.default_data_location}/{directory}')

    if found_in_parquets.rdd.isEmpty():
        logger.error(f'No data found in {save_location}')
        return

    if is_id_field_encrypted:
        spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - '
                                                     f'read partition with cryptography')
        data = found_in_parquets \
            .withColumn(mapped_id_field_name, generic_decrypt_udf(
                func.struct(mapped_id_field_name, func.lit(mapped_id_field_name),
                        func.lit(json.dumps(scenario)))).cast(
                generic_cast_map_as(scenario, mapped_id_field_name))) \
            .withColumn(SOFT_DELETED_FIELD_NAME, generic_soft_delete_udf(
                func.struct(mapped_id_field_name, func.lit(id_value_to_soft_delete), SOFT_DELETED_FIELD_NAME))
                        .cast(TimestampType())) \
            .withColumn(DAY_PARTITION_FIELD_NAME, func.col(DATE_IMPORTED_FIELD_NAME)) \
            .withColumn(mapped_id_field_name, generic_encrypt_udf(
                func.struct(mapped_id_field_name, func.lit(mapped_id_field_name),
                        func.lit(json.dumps(scenario)))).cast(BinaryType()))
    else:
        spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - read partition')
        data = found_in_parquets \
            .withColumn(SOFT_DELETED_FIELD_NAME, generic_soft_delete_udf(
                func.struct(mapped_id_field_name, func.lit(id_value_to_soft_delete), SOFT_DELETED_FIELD_NAME))
                        .cast(TimestampType())) \
            .withColumn(DAY_PARTITION_FIELD_NAME, func.col(DATE_IMPORTED_FIELD_NAME))

    spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - write partition')
    if str(save_type).startswith('parquet'):
        write_spark_parquet(data, temp_save_location,
                            ParquetSaveMode.overwrite,
                            DAY_PARTITION_FIELD_NAME)
        logger.info(f'Save as {save_type} finished into {temp_save_location}')

        if not is_apply_year_to_save_location:
            write_spark_parquet(read_spark_parquet(spark_session, temp_save_location), save_location,
                                ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(f'Save as {save_type} finished into {save_location} '
                        f'with overwrite')
        else:
            today = datetime.today().strftime('%Y-%m-%d')

            years = np.array(read_spark_parquet(spark_session, temp_save_location).filter(
                func.col(SOFT_DELETED_FIELD_NAME) == today)
                             .select(YEAR_IMPORTED_FIELD_NAME)
                             .distinct()
                             .collect())

            for year in list(itertools.chain(*years)):
                write_spark_parquet(read_spark_parquet(spark_session, temp_save_location).where(
                    f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{save_location}_{year}',
                    ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
                logger.info(f'Save as {save_type} finished into {save_location}_{year} '
                            f'with overwrite')

        hdfs_delete(spark_session, temp_save_location)
        logger.info(f'{temp_save_location} is cleaned up')
    elif str(save_type).startswith('table'):
        table_name = str(save_type).split('|')[1]
        temp_table_name = f"{str(save_type).split('|')[1]}_temp"
        s_type = str(save_type).split('|')[0]
        write_hive_table(temp_table_name, data, temp_save_location, ParquetSaveMode.overwrite,
                         DAY_PARTITION_FIELD_NAME)
        logger.info(
            f'Save as {s_type} finished into {table_name} table on {temp_save_location}')

        if not is_apply_year_to_save_location:
            write_hive_table(table_name, read_spark_parquet(spark_session, temp_save_location), save_location,
                             ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(
                f'Save as {s_type} finished into {table_name} table on {save_location} with overwrite')
        else:
            # x_years_old = 3
            # x_years_ago = datetime.now() - relativedelta(years=x_years_old)
            # today = x_years_ago.strftime('%Y-%m-%d')
            today = datetime.today().strftime('%Y-%m-%d')

            years = np.array(read_spark_parquet(spark_session, temp_save_location).filter(
                func.col(SOFT_DELETED_FIELD_NAME) == today)
                             .select(YEAR_IMPORTED_FIELD_NAME)
                             .distinct()
                             .collect())

            for year in list(itertools.chain(*years)):
                write_hive_table(f'{table_name}_{year}',
                                 read_spark_parquet(spark_session, temp_save_location).where(
                                     f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{save_location}_{year}',
                                 ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
                logger.info(
                    f'Save as {s_type} finished into {table_name} table on {save_location}_{year} with overwrite')

        hdfs_delete(spark_session, temp_save_location)
        logger.info(f'{temp_save_location} is cleaned up')
    else:
        logger.error(f'Save mode {save_type} is not identified for {name} '
                     f'entity. Please check scenario JSON located in {scenario_json_path}.')
        exit(1)


async def encrypt_and_import(spark_session: SparkSession,
                             scenario_json_path: str, input_file_path: str,
                             day: str):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Encrypt & Import for {name} entity defined in {scenario_json_path} on '
                f'day={partition_name}')
    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - load raw data')

    raw_data = spark_session.read.option('delimiter', delimiter).csv(input_file_path, inferSchema=True, header=True)
    if raw_data.rdd.isEmpty():
        logger.error(f'No data found in {input_file_path}')
        exit(1)

    if enforce_data_model:
        missing_fields = get_missing_fields(scenario_json_path, raw_data)
        if missing_fields:
            logger.error(f'Following field(s) are missing in the scenario file: {missing_fields}')
            exit(1)

    logger.info(f'Raw data loaded from {input_file_path} for {name} entity')

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - data refinement')
    refined_df = await validate_and_refine(scenario_json_path, raw_data, partition_name)
    logger.info(f'Data refinement and validation applied for {name} entity')

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - apply encryption')
    encrypted_df = await execute_crypto_action(refined_df, scenario_json_path, CryptoAction.encrypt)
    logger.info(f'Cryptography applied for {name} entity')

    logger.info(f'Save as {save_type} started. Import mode: {import_mode}')

    if is_apply_year_to_save_location:
        encrypted_df = encrypted_df.withColumn(YEAR_IMPORTED_FIELD_NAME, func.lit(year))

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - write partition')
    if str(save_type).startswith('parquet'):
        write_spark_parquet(encrypted_df,
                            save_location if not is_apply_year_to_save_location else f'{save_location}_{year}',
                            ParquetSaveMode(import_mode),
                            DAY_PARTITION_FIELD_NAME)
        logger.info(f'Save as {save_type} finished with partition day={partition_name}. Import mode: {import_mode}')
        return
    elif str(save_type).startswith('table'):
        table_name = str(save_type).split('|')[1]
        s_type = str(save_type).split('|')[0]
        write_hive_table(table_name if not is_apply_year_to_save_location else f'{table_name}_{year}', encrypted_df,
                         save_location if not is_apply_year_to_save_location else f'{save_location}_{year}',
                         ParquetSaveMode(import_mode),
                         DAY_PARTITION_FIELD_NAME)
        logger.info(
            f'Save as {s_type} finished into {table_name} table with partition day={partition_name}. '
            f'Import mode: {import_mode}')
        return
    else:
        logger.error(f'Save mode {save_type} is not identified for {name} '
                     f'entity. Please check scenario JSON located in {scenario_json_path}.')
        exit(1)

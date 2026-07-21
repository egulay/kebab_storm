import json
import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as func, SparkSession
from pyspark.sql.types import BinaryType, TimestampType, StructType, StructField, StringType

from conf import settings
from etl.crypter import execute_crypto_action
from etl.refiner import validate_and_refine
from util.constants import DAY_PARTITION_FIELD_NAME, DATE_IMPORTED_FIELD_NAME, SOFT_DELETED_FIELD_NAME, \
    YEAR_IMPORTED_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.crypto.crypto_util import generic_decrypt_udf, generic_encrypt_udf
from util.etl_util import generic_cast_map_as, generic_soft_delete_udf, get_day_partition_name_and_year
from util.parquet_save_mode import ParquetSaveMode
from util.parquet_util import write_spark_parquet, read_spark_parquet, \
    hdfs_delete, hdfs_get_folder_names, hdfs_get_folder_years_less_than, read_spark_parquets_by_dates, hdfs_path_exist
from util.scenario_util import get_scenario_defaults, get_missing_fields, get_unexpected_fields, find_mapped_as_value, \
    load_scenario, find_field_is_encrypted
from util.storage_util import is_iceberg_storage, read_storage, read_storage_by_dates, replace_storage, \
    storage_description, write_storage

logger = logging.getLogger(__name__)


def get_reporting_data_between_dates(spark_session: SparkSession, scenario_json_path: str,
                                     crypto_action: CryptoAction,
                                     include_soft_deleted: bool, start_date, end_date):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location = \
        get_scenario_defaults(scenario_json_path)

    partition_name_start, year_start = get_day_partition_name_and_year(start_date)
    partition_name_end, year_end = get_day_partition_name_and_year(end_date)

    logger.info(f'Active action: Get reporting data for {name} entity defined in {scenario_json_path} '
                f'between {start_date} and {end_date}')
    spark_session.sparkContext.setJobDescription(f'Get reporting data for {name}')

    data = None
    if is_iceberg_storage():
        data = read_storage_by_dates(spark_session, start_date, end_date, import_save_location)
    elif is_apply_year_to_save_location:
        if year_start != year_end:
            locations = [f'{import_save_location}_{year}' for year in range(year_start, year_end + 1)
                         if hdfs_path_exist(spark_session, f'{import_save_location}_{year}')]
            if not locations:
                empty_data = spark_session.createDataFrame(
                    [], StructType([StructField("no data", StringType(), True)]))
                return empty_data, report_save_location, is_apply_year_to_save_location
            data = read_spark_parquet(spark_session, locations[0])
            for location in locations[1:]:
                data = data.unionByName(read_spark_parquet(spark_session, location))
            data = data.withColumn(DATE_IMPORTED_FIELD_NAME, func.to_timestamp(DATE_IMPORTED_FIELD_NAME, "yyyyMMdd")) \
                .filter(func.col(DATE_IMPORTED_FIELD_NAME).between(start_date, end_date))
        else:
            data = read_spark_parquets_by_dates(spark_session, start_date, end_date,
                                                f'{import_save_location}_{year_start}')
    else:
        data = read_spark_parquets_by_dates(spark_session, start_date, end_date, import_save_location)

    if data.isEmpty():
        logger.error('No data found')
        empty_data = spark_session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))
        return empty_data, report_save_location, is_apply_year_to_save_location

    if not include_soft_deleted:
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NULL')

    return data if crypto_action == CryptoAction.encrypt else \
        execute_crypto_action(data, scenario_json_path, CryptoAction.decrypt), \
           report_save_location, is_apply_year_to_save_location


def get_reporting_data(spark_session: SparkSession, scenario_json_path: str,
                       crypto_action: CryptoAction,
                       include_soft_deleted: bool, day):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location = \
        get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Get reporting data for {name} entity defined in {scenario_json_path} '
                f'on day={partition_name}')

    if is_apply_year_to_save_location and not is_iceberg_storage():
        import_save_location = f'{import_save_location}_{year}'

    spark_session.sparkContext.setJobDescription(f'Get reporting data for {name}')
    data = read_storage(spark_session, import_save_location) if is_iceberg_storage() else \
        read_spark_parquet(spark_session, f'{import_save_location}/{DAY_PARTITION_FIELD_NAME}={partition_name}')
    if is_iceberg_storage() and not data.isEmpty():
        data = data.filter(func.col(DATE_IMPORTED_FIELD_NAME) == partition_name)

    if data.isEmpty():
        logger.error('No data found')
        empty_data = spark_session.createDataFrame([], StructType([StructField("no data", StringType(), True)]))
        return empty_data, report_save_location, is_apply_year_to_save_location

    if not include_soft_deleted:
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NULL')

    return data if crypto_action == CryptoAction.encrypt else \
        execute_crypto_action(data, scenario_json_path, CryptoAction.decrypt), \
           report_save_location, is_apply_year_to_save_location


def hard_delete(spark_session: SparkSession, scenario_json_path):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location = \
        get_scenario_defaults(scenario_json_path)

    logger.info(f'Active action: Hard delete for {name} entity defined in {scenario_json_path}')
    spark_session.sparkContext.setJobDescription(f'Hard delete for {name}')

    x_years_ago = datetime.now() - relativedelta(years=hard_delete_in_years)
    folder_years_less_than_x = [] if is_iceberg_storage() else \
        hdfs_get_folder_years_less_than(spark_session, import_save_location, x_years_ago.year)
    all_data = None
    if is_iceberg_storage():
        all_data = read_storage(spark_session, import_save_location)
    elif not is_apply_year_to_save_location:
        all_data = read_spark_parquet(spark_session, import_save_location)
    else:
        for year in folder_years_less_than_x:
            if all_data is not None and not all_data.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{import_save_location}_{year}')
                if not u_data.isEmpty():
                    all_data = all_data.union(u_data)
            else:
                all_data = read_spark_parquet(spark_session, f'{import_save_location}_{year}')

    if all_data is None or all_data.isEmpty():
        logger.error(f'No data found')
        exit(1)

    all_data = all_data.withColumn(
        DAY_PARTITION_FIELD_NAME, func.col(DATE_IMPORTED_FIELD_NAME))

    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - find soft deleted')
    soft_deleted_data = all_data.filter(
        func.col(SOFT_DELETED_FIELD_NAME) <= func.unix_timestamp(
            func.lit(x_years_ago.strftime('%Y-%m-%d %H:%M:%S'))).cast(TimestampType()))

    if soft_deleted_data.isEmpty():
        logger.warning(f'No data found for earlier than {x_years_ago.strftime("%Y-%m-%d")}')
        return

    logger.info(f'Total count for soft deleted data earlier than {x_years_ago.strftime("%Y-%m-%d")} is '
                f'{str(soft_deleted_data.count())}')
    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - exclude soft deleted')
    new_data = all_data.exceptAll(soft_deleted_data)
    logger.info(f'Overwriting partitions')

    spark_session.sparkContext.setJobDescription(f'Hard delete for {name} - overwrite partitions')
    if is_iceberg_storage():
        replace_storage(new_data, import_save_location, DAY_PARTITION_FIELD_NAME)
        logger.info(f'Hard delete committed to {storage_description()} storage')
        logger.warning('Iceberg snapshot expiration and orphan-file cleanup must run under the platform retention job '
                       'to physically remove data retained by older snapshots')
        return

    write_spark_parquet(new_data, temp_save_location, ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
    logger.info(f'Raw Parquet saved into {temp_save_location} for hard delete')

    if not is_apply_year_to_save_location:
        write_spark_parquet(read_spark_parquet(spark_session, temp_save_location), import_save_location,
                            ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
        logger.info(f'Raw Parquet saved into {import_save_location} with overwrite for hard delete')
    else:
        for year in folder_years_less_than_x:
            write_spark_parquet(read_spark_parquet(spark_session, temp_save_location).where(
                f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{import_save_location}_{year}',
                ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(f'Raw Parquet saved into {import_save_location}_{year} with overwrite for hard delete')

    hdfs_delete(spark_session, temp_save_location)
    logger.info(f'{temp_save_location} is cleaned up')


def soft_delete(spark_session: SparkSession, scenario_json_path, id_values_to_soft_delete):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location = \
        get_scenario_defaults(scenario_json_path)

    scenario = load_scenario(scenario_json_path)
    mapped_id_field_name = find_mapped_as_value(scenario, id_field_name)
    is_id_field_encrypted = find_field_is_encrypted(scenario, id_field_name)
    logger.info(
        f'Active action: Soft delete on {name} entity defined in {scenario_json_path} for '
        f'{mapped_id_field_name}={id_values_to_soft_delete}')
    spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - '
                                                 f'{mapped_id_field_name}={id_values_to_soft_delete}')

    found_in_parquets = None
    if is_iceberg_storage():
        found_in_parquets = read_storage(spark_session, import_save_location)
    elif not is_apply_year_to_save_location:
        found_in_parquets = read_spark_parquet(spark_session, import_save_location)
    else:
        for directory in hdfs_get_folder_names(spark_session, import_save_location):
            if found_in_parquets is not None and not found_in_parquets.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{settings.default_data_location}/{directory}')
                if not u_data.isEmpty():
                    found_in_parquets = found_in_parquets.union(u_data)
            else:
                found_in_parquets = read_spark_parquet(spark_session,
                                                       f'{settings.default_data_location}/{directory}')

    if found_in_parquets is None or found_in_parquets.isEmpty():
        logger.error(f'No data found in {import_save_location}')
        return

    if is_id_field_encrypted:
        spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - '
                                                     f'read partition with cryptography')
        data = found_in_parquets \
            .withColumn(mapped_id_field_name, generic_decrypt_udf(
                func.struct(mapped_id_field_name, func.lit(mapped_id_field_name),
                        func.lit(json.dumps(scenario)))).cast(generic_cast_map_as(scenario, mapped_id_field_name)))

        for row_value in str(id_values_to_soft_delete).strip().split(','):
            data = data.withColumn(SOFT_DELETED_FIELD_NAME, generic_soft_delete_udf(
                func.struct(mapped_id_field_name, func.lit(row_value), SOFT_DELETED_FIELD_NAME))
                                   .cast(TimestampType()))

        data = data.withColumn(mapped_id_field_name, generic_encrypt_udf(
            func.struct(mapped_id_field_name, func.lit(mapped_id_field_name),
                        func.lit(json.dumps(scenario)))).cast(BinaryType()))

    else:
        spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - read partition')

        data = found_in_parquets.withColumn(DAY_PARTITION_FIELD_NAME, func.col(DATE_IMPORTED_FIELD_NAME))

        for row_value in str(id_values_to_soft_delete).strip().split(','):
            data = data.withColumn(SOFT_DELETED_FIELD_NAME, generic_soft_delete_udf(
                func.struct(mapped_id_field_name, func.lit(row_value), SOFT_DELETED_FIELD_NAME))
                                   .cast(TimestampType()))

    spark_session.sparkContext.setJobDescription(f'Soft delete for {name} - write partition')
    if is_iceberg_storage():
        replace_storage(data, import_save_location, DAY_PARTITION_FIELD_NAME)
        logger.info(f'Soft delete committed to {storage_description()} storage')
        return

    write_spark_parquet(data, temp_save_location, ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
    logger.info(f'Raw Parquet saved into {temp_save_location}')

    if not is_apply_year_to_save_location:
        write_spark_parquet(read_spark_parquet(spark_session, temp_save_location), import_save_location,
                            ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
        logger.info(f'Raw Parquet saved into {import_save_location} with overwrite')
    else:
        # Retention demo only: uncomment these lines and comment the current-date line below.
        # x_years_old = 3
        # x_years_ago = datetime.now() - relativedelta(years=x_years_old)
        # today = x_years_ago.strftime('%Y-%m-%d')
        today = datetime.today().strftime('%Y-%m-%d')

        years = (read_spark_parquet(spark_session, temp_save_location)
                 .filter(func.col(SOFT_DELETED_FIELD_NAME) == today)
                 .select(YEAR_IMPORTED_FIELD_NAME)
                 .distinct()
                 .collect())

        for row in years:
            year = row[YEAR_IMPORTED_FIELD_NAME]
            write_spark_parquet(read_spark_parquet(spark_session, temp_save_location).where(
                f'{YEAR_IMPORTED_FIELD_NAME}={year}'), f'{import_save_location}_{year}',
                ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
            logger.info(f'Raw Parquet saved into {import_save_location}_{year} with overwrite')

    hdfs_delete(spark_session, temp_save_location)
    logger.info(f'{temp_save_location} is cleaned up')


def encrypt_and_import(spark_session: SparkSession,
                       scenario_json_path: str, input_file_path: str,
                       day: str):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location = \
        get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Encrypt & Import for {name} entity defined in {scenario_json_path} on '
                f'day={partition_name}')
    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - load raw data')

    # Scenario definitions are the schema authority. Reading CSV cells as strings
    # makes conversion deterministic even when corrupt values occur in a column.
    raw_data = spark_session.read.option('delimiter', delimiter).csv(input_file_path, inferSchema=False, header=True)
    if raw_data.isEmpty():
        logger.error(f'No data found in {input_file_path}')
        exit(1)

    if enforce_data_model:
        missing_fields = get_missing_fields(scenario_json_path, raw_data)
        unexpected_fields = get_unexpected_fields(scenario_json_path, raw_data)
        if missing_fields:
            logger.error(f'Following scenario field(s) are missing from the input file: '
                         f'{", ".join(map(str, missing_fields)).strip()}')
            exit(1)
        if unexpected_fields:
            logger.error(f'Following input field(s) are not defined in the scenario: '
                         f'{", ".join(map(str, unexpected_fields)).strip()}')
            exit(1)

    logger.info(f'Raw data loaded from {input_file_path} for {name} entity')

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - data refinement')
    refined_df = validate_and_refine(scenario_json_path, raw_data, partition_name)
    logger.info(f'Data refinement and validation applied for {name} entity')

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - apply encryption')
    encrypted_df = execute_crypto_action(refined_df, scenario_json_path, CryptoAction.encrypt)
    logger.info(f'Cryptography applied for {name} entity')

    logger.info(f'Save as {storage_description()} started. Import mode: {import_mode}')

    if is_apply_year_to_save_location:
        encrypted_df = encrypted_df.withColumn(YEAR_IMPORTED_FIELD_NAME, func.lit(year))

    spark_session.sparkContext.setJobDescription(f'Encrypt & import for {name} - write partition')
    storage_location = import_save_location if is_iceberg_storage() or not is_apply_year_to_save_location else \
        f'{import_save_location}_{year}'
    write_storage(encrypted_df, storage_location, ParquetSaveMode(import_mode), DAY_PARTITION_FIELD_NAME)
    logger.info(f'Save as {storage_description()} finished with partition day={partition_name} '
                f'Import mode: {import_mode}')

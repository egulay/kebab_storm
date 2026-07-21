import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as func

from etl.crypter import execute_crypto_action
from etl.refiner import validate_and_refine
from util.constants import DATE_IMPORTED_FIELD_NAME, DAY_PARTITION_FIELD_NAME, SOFT_DELETED_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.etl_util import get_day_partition_name_and_year
from util.parquet_util import read_spark_parquet, filesystem_get_partitioned_folder_paths
from util.scenario_util import get_scenario_defaults, get_missing_fields, get_unexpected_fields
from util.storage_util import is_iceberg_storage, read_storage
from util.type_checkers import is_not_blank

logger = logging.getLogger(__name__)


def print_report_data(spark_session: SparkSession, scenario_json_path: str, report_name: str, day):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location, _ = \
        get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Print report data for {report_name} based on {scenario_json_path} '
                f'on day={partition_name}')

    report_location = f'{report_save_location}/{report_name}' if is_iceberg_storage() or \
        not is_apply_year_to_save_location else f'{report_save_location}_{year}/{report_name}'
    if is_iceberg_storage():
        data = read_storage(spark_session, report_location)
        if not data.isEmpty():
            data = data.filter(func.col(DAY_PARTITION_FIELD_NAME) == partition_name)
    else:
        parquet_path = f'{report_location}/{DAY_PARTITION_FIELD_NAME}={partition_name}'
        logger.info(f'Identified raw Parquet path: {parquet_path}')
        data = read_spark_parquet(spark_session, parquet_path)

    if data.isEmpty():
        logger.error('No data found')
        return

    print_data_frame_with_schema(data)


def print_imported_data(spark_session: SparkSession, scenario_json_path: str, crypto_action: str, day):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location, _ = \
        get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Print imported data for {name} entity defined in {scenario_json_path} '
                f'on day={partition_name}')

    storage_location = import_save_location if is_iceberg_storage() or not is_apply_year_to_save_location else \
        f'{import_save_location}_{year}'
    if is_iceberg_storage():
        data = read_storage(spark_session, storage_location)
        if not data.isEmpty():
            data = data.filter(func.col(DATE_IMPORTED_FIELD_NAME) == partition_name)
    else:
        data = read_spark_parquet(
            spark_session, f'{storage_location}/{DAY_PARTITION_FIELD_NAME}={partition_name}')

    if data.isEmpty():
        logger.error('No data found')
        return

    print_data_frame_with_schema(
        data if crypto_action == 'encrypted' else execute_crypto_action(data, scenario_json_path,
                                                                        CryptoAction.decrypt))


def print_soft_deleted(spark_session: SparkSession, scenario_json_path: str, crypto_action: str):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location, _ = \
        get_scenario_defaults(scenario_json_path)

    logger.info(f'Active action: Print soft-deleted data for {name} entity defined in {scenario_json_path}')

    data = None
    if is_iceberg_storage():
        data = read_storage(spark_session, import_save_location)
    elif not is_apply_year_to_save_location:
        data = read_spark_parquet(spark_session, import_save_location)
    else:
        for folder_path in filesystem_get_partitioned_folder_paths(spark_session, import_save_location):
            if data is not None and not data.isEmpty():
                u_data = read_spark_parquet(spark_session, folder_path)
                if not u_data.isEmpty():
                    data = data.union(u_data)
            else:
                data = read_spark_parquet(spark_session, folder_path)

    if data is not None and not data.isEmpty():
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NOT NULL')
        if data.isEmpty():
            logger.error('No data found')
            return
        print_data_frame_with_schema(
            data if crypto_action == 'encrypted' else execute_crypto_action(data, scenario_json_path,
                                                                            CryptoAction.decrypt))
        return

    logger.error('No data found')


def print_data_frame_with_schema(source: DataFrame):
    source.persist()
    try:
        total_count = source.count()
        if not total_count:
            logger.error('No data found')
            return
        source.printSchema()
        source.show()
        print(f'Total Count: {total_count}')
    finally:
        source.unpersist()


def print_refined_data_with_schema(spark_session: SparkSession, scenario_json_path, source_file):
    name, import_save_location, temp_save_location, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location, report_save_location, _ = \
        get_scenario_defaults(scenario_json_path)

    if is_not_blank(source_file) and source_file.endswith('.csv'):
        logger.info(f'Printing refined data with schema information for {name} entity defined in {scenario_json_path}')
        data = spark_session.read.option("delimiter", delimiter) \
            .csv(source_file, inferSchema=False, header=True)

        if data.isEmpty():
            logger.error('No data found')
            return

        if enforce_data_model:
            missing_fields = get_missing_fields(scenario_json_path, data)
            unexpected_fields = get_unexpected_fields(scenario_json_path, data)
            if missing_fields:
                logger.error(f'Following scenario field(s) are missing from the input file: '
                             f'{", ".join(map(str, missing_fields)).strip()}')
                exit(1)
            if unexpected_fields:
                logger.error(f'Following input field(s) are not defined in the scenario: '
                             f'{", ".join(map(str, unexpected_fields)).strip()}')
                exit(1)

        logger.info(f'Data loaded from {source_file}')
        result = validate_and_refine(scenario_json_path, data, datetime.today().strftime('%Y%m%d'))
        print_data_frame_with_schema(result)
        return

    logger.error('input data file must be csv')
    exit(1)


def print_sample_data_with_schema(spark_session: SparkSession, source_file, delimiter):
    if is_not_blank(source_file) and source_file.endswith('.csv'):
        logger.info('Printing data with schema information')
        data = spark_session.read.option('delimiter', delimiter) \
            .csv(source_file, inferSchema=True, header=True)
        logger.info(f'Data loaded from {source_file}')

        if data.isEmpty():
            logger.error('No data found')
            return

        print_data_frame_with_schema(data)
        return

    logger.error('input data file must be csv')

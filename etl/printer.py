from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from conf import settings
from etl.crypter import execute_crypto_action
from etl.refiner import validate_and_refine
from util.constants import DAY_PARTITION_FIELD_NAME, SOFT_DELETED_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.etl_util import get_day_partition_name_and_year
from util.logger import get_logger
from util.parquet_util import read_spark_parquet, hdfs_get_folder_names
from util.scenario_util import get_scenario_defaults, get_missing_fields
from util.type_checkers import is_not_blank

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)


async def print_imported_data(spark_session: SparkSession, scenario_json_path: str, crypto_action: str,
                              day):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    partition_name, year = get_day_partition_name_and_year(day)

    logger.info(f'Active action: Print imported data for {name} entity defined in {scenario_json_path} '
                f'on day={partition_name}')

    parquet_path = f'{save_location if not is_apply_year_to_save_location else f"{save_location}_{year}"}' \
                   f'/{DAY_PARTITION_FIELD_NAME}={partition_name}'

    data = read_spark_parquet(spark_session, parquet_path)

    if data.rdd.isEmpty():
        logger.error('No data found')
        return

    await print_data_frame_with_schema(
        data if crypto_action == 'encrypted' else await execute_crypto_action(data, scenario_json_path,
                                                                              CryptoAction.decrypt))


async def print_soft_deleted(spark_session: SparkSession, scenario_json_path: str, crypto_action: str):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    logger.info(f'Active action: Print soft-deleted data for {name} entity defined in {scenario_json_path}')

    data = None
    if not is_apply_year_to_save_location:
        data = read_spark_parquet(spark_session, save_location)
    else:
        for directory in hdfs_get_folder_names(spark_session, save_location):
            if data is not None and not data.rdd.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{settings.default_data_location}/{directory}')
                if not u_data.rdd.isEmpty():
                    data = data.union(u_data)
            else:
                data = read_spark_parquet(spark_session, f'{settings.default_data_location}/{directory}')

    if not data.rdd.isEmpty():
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NOT NULL')
        if data.rdd.isEmpty():
            logger.error('No data found')
            return
        await print_data_frame_with_schema(
            data if crypto_action == 'encrypted' else await execute_crypto_action(data, scenario_json_path,
                                                                                  CryptoAction.decrypt))
        return

    logger.error('No data found')


async def print_data_frame_with_schema(source: DataFrame):
    if not source.rdd.isEmpty():
        source.printSchema()
        source.show()
        print(f'Total Count: {str(source.count())}')
        return
    logger.error('No data found')


async def print_refined_data_with_schema(spark_session: SparkSession, scenario_json_path, source_file):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    if is_not_blank(source_file) and source_file.endswith('.csv'):
        logger.info(f'Printing refined data with schema information for {name} entity defined in {scenario_json_path}')
        data = spark_session.read.option("delimiter", delimiter) \
            .csv(source_file, inferSchema=True, header=True)

        if data.rdd.isEmpty():
            logger.error('No data found')
            return

        if enforce_data_model:
            missing_fields = get_missing_fields(scenario_json_path, data)
            if missing_fields:
                logger.error(f'Following field(s) are missing in the scenario file: {missing_fields}')
                return

        logger.info(f'Data loaded from {source_file}')
        result = await validate_and_refine(scenario_json_path, data, datetime.today().strftime('%Y%m%d'))
        await print_data_frame_with_schema(result)
        return

    logger.error('input data file must be csv')


async def print_sample_data_with_schema(spark_session: SparkSession, source_file, delimiter):
    if is_not_blank(source_file) and source_file.endswith('.csv'):
        logger.info('Printing data with schema information')
        data = spark_session.read.option('delimiter', delimiter) \
            .csv(source_file, inferSchema=True, header=True)
        logger.info(f'Data loaded from {source_file}')

        if data.rdd.isEmpty():
            logger.error('No data found')
            return

        await print_data_frame_with_schema(data)
        return

    logger.error('input data file must be csv')

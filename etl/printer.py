from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import DataFrame

# import pandas
from conf.settings import SPARK_MASTER, SPARK_CONFIG, KEBAB_STORM_LOGGING_LOCATION, DEFAULT_DATA_LOCATION
from etl.crypter import execute_crypto_action
from etl.refiner import validate_and_refine
from util.constants import DAY_PARTITION_FIELD_NAME, SOFT_DELETED_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
# from util.dataframe_util import pandas_to_spark
from util.etl_util import get_day_partition_name_and_year
from util.logger import get_logger
from util.parquet_util import read_spark_parquet, hdfs_get_folder_names
from util.scenario_util import get_scenario_defaults, get_missing_fields
from util.spark_util import SparkProvider
from util.type_checkers import is_not_blank

logger = get_logger(__name__, KEBAB_STORM_LOGGING_LOCATION,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log')

spark_session = SparkProvider.setup_spark('Project: Kebab Storm', SPARK_MASTER,
                                          extra_dependencies=[], conf=SparkConf().setAll(SPARK_CONFIG))


# def compare_data_frames(source: DataFrame, target: DataFrame, column_name: str):
#     df = pandas_to_spark(pandas.merge(source.toPandas(), target.toPandas(), on=[column_name], how='inner'),
#                          spark_session)
#
#     print(f'merged count {df.count()} || source count {source.count()}')


def print_imported_data(scenario_json_path: str, crypto_action: str, day):
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

    print_data_frame_with_schema(
        data if crypto_action == 'encrypted' else execute_crypto_action(data, scenario_json_path, CryptoAction.decrypt))


def print_soft_deleted(scenario_json_path: str, crypto_action: str):
    name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
        enforce_data_model, is_apply_year_to_save_location = get_scenario_defaults(scenario_json_path)

    logger.info(f'Active action: Print soft-deleted data for {name} entity defined in {scenario_json_path}')

    data = None
    if not is_apply_year_to_save_location:
        data = read_spark_parquet(spark_session, save_location)
    else:
        for directory in hdfs_get_folder_names(spark_session, save_location):
            if data is not None and not data.rdd.isEmpty():
                u_data = read_spark_parquet(spark_session, f'{DEFAULT_DATA_LOCATION}/{directory}')
                if not u_data.rdd.isEmpty():
                    data = data.union(u_data)
            else:
                data = read_spark_parquet(spark_session, f'{DEFAULT_DATA_LOCATION}/{directory}')

    if not data.rdd.isEmpty():
        data = data.where(f'{SOFT_DELETED_FIELD_NAME} IS NOT NULL')
        if data.rdd.isEmpty():
            logger.error('No data found')
            return
        print_data_frame_with_schema(
            data if crypto_action == 'encrypted' else execute_crypto_action(data, scenario_json_path,
                                                                            CryptoAction.decrypt))
        return

    logger.error('No data found')


def print_data_frame_with_schema(source: DataFrame):
    if not source.rdd.isEmpty():
        source.printSchema()
        source.show()
        print(f'Total Count: {str(source.count())}')
        return
    logger.error('No data found')


# def hebeh():
#     name, save_location, temp_save_location, save_type, import_mode, id_field_name, delimiter, hard_delete_in_years, \
#         enforce_data_model = get_scenario_defaults('../scenario/customer_contract_scenario.json')
#     source = spark_session.read.option("delimiter", ";") \
#         .csv('../data/20200226_report.csv', inferSchema=True, header=True)
#
#     parquet_path = f'{save_location}/{DAY_PARTITION_FIELD_NAME}=20200223'
#
#     target = execute_crypto_action(read_spark_parquet(spark_session, parquet_path),
#                                    '../scenario/customer_contract_scenario.json', CryptoAction.decrypt)
#
#     compare_dataframes(source, target, 'ID')


def print_refined_data_with_schema(scenario_json_path, source_file):
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
        result = validate_and_refine(scenario_json_path, data, datetime.today().strftime('%Y%m%d'))
        print_data_frame_with_schema(result)
        return

    logger.error('input data file must be csv')


def print_sample_data_with_schema(source_file, delimiter):
    if is_not_blank(source_file) and source_file.endswith('.csv'):
        logger.info('Printing data with schema information')
        data = spark_session.read.option('delimiter', delimiter) \
            .csv(source_file, inferSchema=True, header=True)
        logger.info(f'Data loaded from {source_file}')

        if data.rdd.isEmpty():
            logger.error('No data found')
            return

        print_data_frame_with_schema(data)
        return

    logger.error('input data file must be csv')

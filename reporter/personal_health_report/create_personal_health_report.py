import argparse
import logging
import time
from datetime import datetime

import pyspark.sql.functions as func

from conf import settings
from etl.importer import get_reporting_data, get_reporting_data_between_dates
from reporter.personal_health_report import START_DATE_INDEX, END_DATE_INDEX, REPORT_NAME
from util.application_util import spark_application
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_DATE, CLI_INCLUDE_SOFT_DELETED, DAY_PARTITION_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.etl_util import get_boolean, get_day_partition_name_and_year
from util.parquet_save_mode import ParquetSaveMode
from util.storage_util import is_iceberg_storage, storage_description, write_storage


# ex. --scenario ../../scenario/personal_health_records_scenario.json --include-soft-deleted nope --date 2026-07-21
# ex. --scenario ../../scenario/personal_health_records_scenario.json --include-soft-deleted nope --date 2026-01-01->2026-07-21
logger = logging.getLogger(__name__)


def main():
    set_args()
    date_range_arg = settings.active_config[CLI_DATE].get(str)
    date_range = date_range_arg.split('->')

    with spark_application(__name__) as (spark_session, application_logger):
        application_logger.info(
            f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
            f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')

        if len(date_range) == 1:
            data, report_save_location, is_apply_year_to_save_location = get_reporting_data(
                spark_session,
                settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                CryptoAction.decrypt,
                get_boolean(settings.active_config[CLI_INCLUDE_SOFT_DELETED].get()),
                date_range_arg,
            )
        else:
            data, report_save_location, is_apply_year_to_save_location = \
                get_reporting_data_between_dates(
                    spark_session,
                    settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                    CryptoAction.decrypt,
                    False,
                    str(date_range[START_DATE_INDEX]),
                    str(date_range[END_DATE_INDEX]),
                )
        if data.isEmpty():
            raise SystemExit(1)

        application_logger.info(f'Total row(s) will be processed for {REPORT_NAME} is {data.count()}')
        partition_name, year = get_day_partition_name_and_year(datetime.today().strftime('%Y-%m-%d'))

        age_band = func.when(data.AGE < 18, '0-17') \
            .when(data.AGE < 35, '18-34') \
            .when(data.AGE < 50, '35-49') \
            .when(data.AGE < 65, '50-64') \
            .when(data.AGE < 80, '65-79') \
            .otherwise('80+')

        sample_report = data.withColumn('AGE_BAND', age_band) \
            .groupBy(data.DIAGNOSIS_CODE, data.BIOLOGICAL_SEX, 'AGE_BAND') \
            .agg(func.countDistinct(data.PATIENT_RECORD_ID).alias('PATIENT_COUNT'),
                 func.round(func.avg(data.AGE), 1).alias('AVERAGE_AGE'),
                 func.round(func.avg(data.BMI), 2).alias('AVERAGE_BMI'),
                 func.round(func.avg(data.GLUCOSE_LEVEL), 1).alias('AVERAGE_GLUCOSE_LEVEL'),
                 func.round(func.avg(data.SYSTOLIC_BP), 1).alias('AVERAGE_SYSTOLIC_BP'),
                 func.round(func.avg(data.DAILY_STEPS), 0).cast('long').alias('AVERAGE_DAILY_STEPS'),
                 func.sum(func.when(data.IS_SMOKER, 1).otherwise(0)).alias('SMOKER_COUNT'),
                 func.sum(func.when(data.HAS_CONSENT, 1).otherwise(0)).alias('CONSENTED_PATIENT_COUNT'),
                 func.sum(func.when(data.RISK_SCORE_TEXT.cast('double') >= 0.65, 1).otherwise(0))
                 .alias('HIGH_RISK_PATIENT_COUNT')) \
            .withColumn(DAY_PARTITION_FIELD_NAME, func.lit(partition_name))

        application_logger.info(f'Save as {storage_description()} started')
        report_location = f'{report_save_location}/{REPORT_NAME}' if is_iceberg_storage() or \
            not is_apply_year_to_save_location else f'{report_save_location}_{year}/{REPORT_NAME}'
        write_storage(sample_report, report_location, ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
        application_logger.info(
            f'Save as {storage_description()} finished with partition day={partition_name}')


def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                                    help='Scenario JSON file path', required=True)

    required_arguments.add_argument('--include-soft-deleted', '-isd', dest=CLI_INCLUDE_SOFT_DELETED, metavar='yes | no',
                                    help='Include soft deleted records into the report', required=True)

    required_arguments.add_argument('--date', '-d', dest=CLI_DATE, metavar='2019-01-01->2020-01-01 | 2019-01-01',
                                    help='Import date range in YYYY-mm-dd format. If date range is '
                                         'required the separator must be -> without white spaces', required=True)

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_include_soft_deleted, args.cli_date)
    if not is_args_provided:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    main()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

import argparse
import asyncio
import time
from datetime import datetime

import pyspark.sql.functions as func
from pyspark.sql.types import DecimalType

from conf import settings
from etl.importer import get_reporting_data, get_reporting_data_between_dates
from reporter import spark_session, logger
from reporter.simple_sales_report import START_DATE_INDEX, END_DATE_INDEX, REPORT_NAME
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_DATE, CLI_INCLUDE_SOFT_DELETED, DAY_PARTITION_FIELD_NAME
from util.crypto.crypto_action import CryptoAction
from util.etl_util import get_boolean, get_day_partition_name_and_year
from util.parquet_save_mode import ParquetSaveMode
from util.parquet_util import write_spark_parquet, write_hive_table


# ex. --scenario ../../scenario/sales_records_scenario.json --include-soft-deleted nope --date 2020-02-23
# ex. --scenario ../../scenario/sales_records_scenario.json --include-soft-deleted nope --date 2019-02-27->2020-12-01
async def main():
    await set_args()
    date_range_arg = settings.active_config[CLI_DATE].get(str)
    date_range = date_range_arg.split('->')

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')

    if len(date_range) == 1:
        data, report_save_type, report_save_location, is_apply_year_to_save_location = \
            await get_reporting_data(spark_session,
                                     settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                                     CryptoAction.decrypt,
                                     get_boolean(settings.active_config[CLI_INCLUDE_SOFT_DELETED].get()),
                                     date_range_arg)
    else:
        data, report_save_type, report_save_location, is_apply_year_to_save_location = \
            await get_reporting_data_between_dates(spark_session,
                                                   settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                                                   CryptoAction.decrypt,
                                                   False, str(date_range[START_DATE_INDEX]),
                                                   str(date_range[END_DATE_INDEX]))
    if data.rdd.isEmpty():
        exit(1)

    logger.info(f'Total row(s) will be processed for {REPORT_NAME} is {str(data.count())}')
    partition_name, year = get_day_partition_name_and_year(datetime.today().strftime('%Y-%m-%d'))

    sample_report = data.select(data.COUNTRY, data.TOTALREVENUE) \
        .groupBy(data.COUNTRY) \
        .agg(func.count(data.COUNTRY).alias('TOTAL_SALES_ENTRY'),
             func.sum(data.TOTALREVENUE).cast(DecimalType()).alias('TOTAL_SALES_REVENUE')) \
        .withColumn(DAY_PARTITION_FIELD_NAME, func.lit(partition_name))

    logger.info(f'Save as {report_save_type} started')
    if str(report_save_type).startswith('parquet'):
        write_spark_parquet(sample_report,
                            f'{report_save_location}/{REPORT_NAME}' if not is_apply_year_to_save_location else
                            f'{report_save_location}_{year}/{REPORT_NAME}',
                            ParquetSaveMode.overwrite,
                            DAY_PARTITION_FIELD_NAME)
        logger.info(f'Save as {report_save_type} finished with partition day={partition_name}')
        return
    elif str(report_save_type).startswith('table'):
        table_name = str(report_save_type).split('|')[1]
        s_type = str(report_save_type).split('|')[0]
        write_hive_table(table_name if not is_apply_year_to_save_location else f'{table_name}_{year}', sample_report,
                         f'{report_save_location}/{REPORT_NAME}' if not is_apply_year_to_save_location
                         else f'{report_save_location}_{year}/{REPORT_NAME}',
                         ParquetSaveMode.overwrite, DAY_PARTITION_FIELD_NAME)
        logger.info(
            f'Save as {s_type} finished into {table_name} table with partition day={partition_name}')
        return
    else:
        logger.error(f'Save mode {report_save_type} is not identified. Please check scenario JSON located in '
                     f'{settings.active_config[CLI_SCENARIO_JSON_PATH].get()}')
        exit(1)


async def set_args():
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
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

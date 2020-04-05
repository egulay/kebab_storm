import argparse
import asyncio
import time

import pyspark.sql.functions as func

from conf import settings
from etl.importer import get_reporting_data
from etl.reporter import spark_session, logger
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_DATE, CLI_INCLUDE_SOFT_DELETED
from util.crypto.crypto_action import CryptoAction
from util.etl_util import get_boolean


# TODO: Separate create report & print report

# ex. --scenario ../../scenario/sales_records_scenario.json --include-soft-deleted nope --date 2020-02-23
async def main():
    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    # TODO: Add multiple day partition selection also update CLI

    data, report_save_type, report_save_location = \
        await get_reporting_data(spark_session,
                                 settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                                 CryptoAction.decrypt,
                                 get_boolean(settings.active_config[CLI_INCLUDE_SOFT_DELETED].get()),
                                 settings.active_config[CLI_DATE].get())

    if data.rdd.isEmpty():
        exit(1)

    sample_report = data.select(data.COUNTRY, data.TOTALPROFIT) \
        .groupBy(data.COUNTRY) \
        .agg(func.count(data.COUNTRY).alias('TOTAL_SALES_ENTRY')) \
        .sort(func.desc('TOTAL_SALES_ENTRY'))

    sample_report.show(truncate=False)

    # TODO: save will be implemented here
    if str(report_save_type).startswith('parquet'):
        print('save as parquet')
    elif str(report_save_type).startswith('table'):
        print('save as table')


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                        help='Scenario JSON file path')

    parser.add_argument('--include-soft-deleted', '-isd', dest=CLI_INCLUDE_SOFT_DELETED, metavar='yes | no',
                        help='Include soft deleted records into the report')

    parser.add_argument('--date', '-d', dest=CLI_DATE, metavar='2020-01-01',
                        help='Import date in YYYY-mm-dd format')

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

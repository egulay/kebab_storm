import argparse
import asyncio
import time
from datetime import datetime

from pyspark import SparkConf

from conf import settings
from etl.printer import print_imported_data
from util.constants import CLI_CRYPTO_ACTION, CLI_DATE, CLI_SCENARIO_JSON_PATH
from util.logger import get_logger
from util.spark_util import SparkProvider

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)

spark_session = SparkProvider.setup_spark('Project: Kebab Storm', settings.spark_master,
                                          extra_dependencies=[], conf=SparkConf().setAll(settings.spark_config))


# ex. --scenario ../scenario/sales_records_scenario.json --crypto-action decrypted --date 2020-02-23
async def main():
    print(settings.banner)
    time.sleep(0.005)
    logger.info(
        f'###### KEBAB STORM STARTED | Active Profile: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')
    time.sleep(0.005)

    await set_args()

    await print_imported_data(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                              settings.active_config[CLI_CRYPTO_ACTION].get(), settings.active_config[CLI_DATE].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                        help='Scenario JSON file path')

    parser.add_argument('--crypto-action', '-ca', dest=CLI_CRYPTO_ACTION, metavar='decrypted',
                        help='Print data decrypted or encrypted')

    parser.add_argument('--date', '-d', dest=CLI_DATE, metavar='2020-01-01',
                        help='Imported date in YYYY-mm-dd format')

    args = parser.parse_args()
    is_params_provided = None not in (args.cli_scenario_json_path, args.cli_crypto_action, args.cli_date)
    if not is_params_provided:
        logger.error('Missing parameter value(s). For information execute with --help')
        exit(1)
    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    # Python 3.6:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")
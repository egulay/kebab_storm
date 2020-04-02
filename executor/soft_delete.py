import argparse
import asyncio
import time
from datetime import datetime

from pyspark import SparkConf

from conf import settings
from etl.importer import soft_delete
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_ID_VALUE
from util.logger import get_logger
from util.spark_util import SparkProvider

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)

spark_session = SparkProvider.setup_spark('Project: Kebab Storm', settings.spark_master,
                                          extra_dependencies=[], conf=SparkConf().setAll(settings.spark_config))


# ex. --scenario ../scenario/sales_records_scenario.json --id_value 897751939
# ex. --scenario ../scenario/sales_records_scenario.json --id_value 281291043
async def main():
    print(settings.banner)
    time.sleep(0.005)

    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active Profile: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    await soft_delete(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                      settings.active_config[CLI_ID_VALUE].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                        help='Scenario JSON file path')

    parser.add_argument('--id_value', '-id', dest=CLI_ID_VALUE, metavar='0123456789',
                        help='Row ID value to soft-delete')

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_id_value)
    if not is_args_provided:
        parser.error('Missing parameter value(s). For information execute with --help')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    # Python 3.6:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

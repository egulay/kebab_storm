import argparse
import asyncio
import time

from conf import settings
from etl.executor import spark_session, logger
from etl.importer import hard_delete
from util.constants import CLI_SCENARIO_JSON_PATH


# ex. --scenario ../../scenario/sales_records_scenario.json
async def main():
    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    await hard_delete(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                                    help='Scenario JSON file path', required=True)

    args = parser.parse_args()
    if args.cli_scenario_json_path is None:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

import argparse
import asyncio
import time

from conf import settings
from etl.importer import encrypt_and_import
from executor import spark_session, logger
from util.constants import CLI_INPUT_FILE_PATH, CLI_DATE, CLI_SCENARIO_JSON_PATH


# ex. --scenario ../scenario/sales_records_scenario.json --input-file ../data/50k_sales_records_corrupted.csv --date 2020-02-23
async def main():
    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    await encrypt_and_import(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                             settings.active_config[CLI_INPUT_FILE_PATH].get(), settings.active_config[CLI_DATE].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                        help='Scenario JSON file path')

    parser.add_argument('--input-file', '-if', dest=CLI_INPUT_FILE_PATH, metavar='/path/to/input_file.csv',
                        help='File to import')

    parser.add_argument('--date', '-d', dest=CLI_DATE, metavar='2020-01-01',
                        help='Import date in YYYY-mm-dd format')

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_input_file_path, args.cli_date)
    if not is_args_provided:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

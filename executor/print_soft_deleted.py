import argparse
import asyncio
import time

from conf import settings
from etl.printer import print_soft_deleted
from executor import spark_session, logger
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_CRYPTO_ACTION


# ex. --scenario ../scenario/sales_records_scenario.json --crypto-action decrypted
async def main():
    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    await print_soft_deleted(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                             settings.active_config[CLI_CRYPTO_ACTION].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                        help='Scenario JSON file path')

    parser.add_argument('--crypto-action', '-ca', dest=CLI_CRYPTO_ACTION, metavar='decrypted | encrypted',
                        help='Print soft-deleted rows decrypted or encrypted')

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_crypto_action)
    if not is_args_provided:
        parser.error(f'Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    # Python 3.6:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

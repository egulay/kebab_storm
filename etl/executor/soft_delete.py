import argparse
import asyncio
import time

from conf import settings
from etl.executor import spark_session, logger
from etl.importer import soft_delete
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_ID_VALUE


async def main():
    """
    Execution Sample:
        soft_delete.py --scenario ../../scenario/sales_records_scenario.json --id_value 897751939
        soft_delete.py --scenario ../../scenario/sales_records_scenario.json --id_value 281291043
    """

    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')

    await soft_delete(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                      settings.active_config[CLI_ID_VALUE].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH, metavar='/path/to/scenario.json',
                                    help='Scenario JSON file path', required=True)

    required_arguments.add_argument('--id_value', '-id', dest=CLI_ID_VALUE, metavar='0123456789',
                                    help='Row ID value to soft-delete', required=True)

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_id_value)
    if not is_args_provided:
        parser.error('Missing parameter value(s). For information execute with --help')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

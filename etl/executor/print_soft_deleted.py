import argparse
import logging
import time

from conf import settings
from etl.printer import print_soft_deleted
from util.application_util import spark_application
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_CRYPTO_ACTION


logger = logging.getLogger(__name__)


def main():
    """
    Execution Sample:
        print_soft_deleted.py --scenario ../../scenario/sales_records_scenario.json --crypto-action decrypted
    """

    set_args()
    with spark_application(__name__) as (spark_session, application_logger):
        application_logger.info(
            f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
            f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')
        print_soft_deleted(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                           settings.active_config[CLI_CRYPTO_ACTION].get())


def set_args():
    parser = argparse.ArgumentParser(description='Kebab Storm: A Spark driver demonstrating how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH,
                                    metavar='/path/to/scenario.json OR https://domain.com/scenario-id',
                                    help='Scenario JSON file path or URL', required=True)

    required_arguments.add_argument('--crypto-action', '-ca', dest=CLI_CRYPTO_ACTION, metavar='decrypted | encrypted',
                                    help='Print soft-deleted rows decrypted or encrypted', required=True)

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_crypto_action)
    if not is_args_provided:
        parser.error(f'Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    main()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

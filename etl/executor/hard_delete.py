import argparse
import logging
import time

from conf import settings
from etl.importer import hard_delete
from util.application_util import spark_application
from util.constants import CLI_SCENARIO_JSON_PATH


logger = logging.getLogger(__name__)


def main():
    """
    Execution Sample:
        hard_delete.py --scenario ../../scenario/sales_records_scenario.json
    """

    set_args()
    with spark_application(__name__) as (spark_session, application_logger):
        application_logger.info(
            f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
            f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')
        hard_delete(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get())


def set_args():
    parser = argparse.ArgumentParser(description='Kebab Storm: A Spark driver demonstrating native Spark '
                                                 'AES-256-GCM cryptography with data quality checks based on ETL '
                                                 'scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH,
                                    metavar='/path/to/scenario.json OR https://domain.com/scenario-id',
                                    help='Scenario JSON file path or URL', required=True)

    args = parser.parse_args()
    if args.cli_scenario_json_path is None:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    main()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

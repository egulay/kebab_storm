import argparse
import logging
import time

from conf import settings
from etl.importer import soft_delete
from util.application_util import spark_application
from util.constants import CLI_SCENARIO_JSON_PATH, CLI_ID_VALUES


logger = logging.getLogger(__name__)


def main():
    """
    Execution Sample:
        soft_delete.py --scenario ../../scenario/sales_records_scenario.json --id_values 897892701,988647067,926870000,118860328,291229379,540900981
    """

    set_args()
    with spark_application(__name__) as (spark_session, application_logger):
        application_logger.info(
            f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
            f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')
        soft_delete(spark_session, settings.active_config[CLI_SCENARIO_JSON_PATH].get(),
                    settings.active_config[CLI_ID_VALUES].get())


def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--scenario', '-scn', dest=CLI_SCENARIO_JSON_PATH,
                                    metavar='/path/to/scenario.json OR https://domain.com/scenario-id',
                                    help='Scenario JSON file path or URL', required=True)

    required_arguments.add_argument('--id_values', '-ids', dest=CLI_ID_VALUES, metavar='897892701,988647067',
                                    help='Comma seperated row ID values to soft-delete', required=True)

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_scenario_json_path, args.cli_id_values)
    if not is_args_provided:
        parser.error('Missing parameter value(s). For information execute with --help')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    main()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

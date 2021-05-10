import argparse
import asyncio
import time

from conf import settings
from etl.executor import spark_session, logger
from etl.printer import print_sample_data_with_schema
from util.constants import CLI_INPUT_FILE_PATH, CLI_INPUT_FILE_DELIMITER


async def main():
    """
    Execution Sample:
        print_sample_data_with_schema.py --input-file ../../data/50k_sales_records_corrupted.csv --delimiter ,
    """

    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} with application ID {spark_session.sparkContext.applicationId} ######')

    await print_sample_data_with_schema(spark_session, settings.active_config[CLI_INPUT_FILE_PATH].get(),
                                        settings.active_config[CLI_INPUT_FILE_DELIMITER].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')
    required_arguments = parser.add_argument_group('required arguments')

    required_arguments.add_argument('--input-file', '-if', dest=CLI_INPUT_FILE_PATH, metavar='/path/to/input_file.csv',
                                    help='File to print', required=True)

    required_arguments.add_argument('--delimiter', '-d', dest=CLI_INPUT_FILE_DELIMITER, metavar=';',
                                    help='Column delimiter for CSV file', required=True)

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_input_file_path, args.cli_input_file_delimiter)
    if not is_args_provided:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

import argparse
import asyncio
import time

from conf import settings
from etl.printer import print_sample_data_with_schema
from executor import spark_session, logger
from util.constants import CLI_INPUT_FILE_PATH, CLI_INPUT_FILE_DELIMITER


# ex. --input-file ../data/50k_sales_records_corrupted.csv --delimiter ,
async def main():
    await set_args()

    logger.info(
        f'###### KEBAB STORM STARTED | Active YAML Configuration: {settings.active_profile} '
        f'on Spark {spark_session.version} ######')

    await print_sample_data_with_schema(spark_session, settings.active_config[CLI_INPUT_FILE_PATH].get(),
                                        settings.active_config[CLI_INPUT_FILE_DELIMITER].get())


async def set_args():
    parser = argparse.ArgumentParser(description='KebabStorm: A Spark driver for to demonstrate how to apply '
                                                 'cryptography (with AES) on UDF level with data quality checks based '
                                                 'on ETL scenarios in JSON format')

    parser.add_argument('--input-file', '-if', dest=CLI_INPUT_FILE_PATH, metavar='/path/to/input_file.csv',
                        help='File to print')

    parser.add_argument('--delimiter', '-d', dest=CLI_INPUT_FILE_DELIMITER, metavar=';',
                        help='Column delimiter for CSV file')

    args = parser.parse_args()
    is_args_provided = None not in (args.cli_input_file_path, args.cli_input_file_delimiter)
    if not is_args_provided:
        parser.error('Missing argument(s)')

    settings.active_config.set_args(args, dots=False)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main())
    # Python 3.6:
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds")

import sys
from datetime import datetime
from time import sleep

from conf.settings import KEBAB_STORM_LOGGING_LOCATION, ACTIVE_PROFILE, DRIVER_BANNER
from etl.importer import encrypt_and_import, soft_delete, hard_delete
from etl.printer import print_imported_data, print_sample_data_with_schema, print_refined_data_with_schema, \
    print_soft_deleted
from util.logger import get_logger

logger = get_logger(__name__, KEBAB_STORM_LOGGING_LOCATION,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log')


def main():
    print(DRIVER_BANNER)
    sleep(0.05)
    logger.info(f'###### KEBAB STORM STARTED | Active Profile: {ACTIVE_PROFILE} ######')
    if len(sys.argv) < 2:
        logger.error('Please provide necessary parameters')
        return

    scenario_json_file_path = str(str(sys.argv[1]).split("->")[1])

    # ex. apply-hard-delete->../scenario/sales_records_scenario.json
    if str(sys.argv[1]).startswith('apply-hard-delete->'):
        hard_delete(scenario_json_file_path)
        return

    # ex. print-sample-data-with-schema->../data/50k_sales_records.csv->,
    if str(sys.argv[1]).startswith('print-sample-data-with-schema->'):
        file_path = str(str(sys.argv[1]).split("->")[1])
        delimiter = str(str(sys.argv[1]).split("->")[2])
        print_sample_data_with_schema(file_path, delimiter)
        return

    # ex. refine-and-print->../scenario/sales_records_scenario.json->../data/50k_sales_records.csv
    if str(sys.argv[1]).startswith('refine-and-print->'):
        input_file = str(str(sys.argv[1]).split("->")[2])
        print_refined_data_with_schema(scenario_json_file_path, input_file)
        return

    # ex. soft-delete->../scenario/sales_records_scenario.json->897751939
    # ex. soft-delete->../scenario/sales_records_scenario.json->281291043
    if str(sys.argv[1]).startswith('soft-delete->'):
        id_to_soft_delete = str(str(sys.argv[1]).split("->")[2])
        soft_delete(scenario_json_file_path, id_to_soft_delete)
        return

    # ex. print-soft-deleted->../scenario/sales_records_scenario.json->decrypted
    if str(sys.argv[1]).startswith('print-soft-deleted->'):
        encryption_action = str(str(sys.argv[1]).split("->")[1])
        print_soft_deleted(scenario_json_file_path, encryption_action)
        return

    # ex. print-imported-data->../scenario/sales_records_scenario.json->decrypted->2020-02-23
    # ex. print-imported-data->../scenario/sales_records_scenario.json->encrypted->2020-02-23
    if str(sys.argv[1]).startswith('print-imported-data->'):
        encryption_action = str(str(sys.argv[1]).split("->")[2])
        day = str(str(sys.argv[1]).split("->")[3])
        print_imported_data(scenario_json_file_path, encryption_action, day)
        return

    # ex. encrypt-and-import->../scenario/sales_records_scenario.json->../data/50k_sales_records.csv->2020-02-23
    # ex. encrypt-and-import->../scenario/sales_records_scenario.json->../data/50k_sales_records_corrupted.csv->2020-02-23
    if str(sys.argv[1]).startswith('encrypt-and-import->'):
        input_file = str(str(sys.argv[1]).split("->")[2])
        day = str(str(sys.argv[1]).split("->")[3])
        encrypt_and_import(scenario_json_file_path, input_file, day)
        return


if __name__ == "__main__":
    main()

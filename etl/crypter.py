import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as func

from util.crypto.crypto_action import CryptoAction
from util.crypto.crypto_util import decrypt_column, encrypt_column
from util.etl_util import generic_cast_map_as
from util.scenario_util import load_scenario, find_encrypted_fields_map_as, find_encryption_key

logger = logging.getLogger(__name__)


def execute_crypto_action(source: DataFrame, scenario_json_path, action: CryptoAction) -> DataFrame:
    scenario = load_scenario(scenario_json_path)

    columns_to_encrypt = find_encrypted_fields_map_as(scenario)
    logger.info(
        f'{action.value} will be applied to following mapped column(s): '
        f'{", ".join(map(str, columns_to_encrypt)).strip()}')

    encrypted_fields = set(columns_to_encrypt)
    projected_columns = []
    for column_name in source.columns:
        if column_name not in encrypted_fields:
            projected_columns.append(func.col(column_name))
            continue

        encryption_key = find_encryption_key(scenario, column_name)
        if action is CryptoAction.encrypt:
            transformed = encrypt_column(func.col(column_name), encryption_key, column_name)
        else:
            transformed = decrypt_column(func.col(column_name), encryption_key, column_name).cast(
                generic_cast_map_as(scenario, column_name))
        projected_columns.append(transformed.alias(column_name))

    return source.select(*projected_columns)

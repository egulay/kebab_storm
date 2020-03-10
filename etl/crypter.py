import json
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import BinaryType

from conf.settings import KEBAB_STORM_LOGGING_LOCATION
from util.crypto.crypto_action import CryptoAction
from util.crypto.crypto_util import generic_encrypt_udf, generic_decrypt_udf
from util.etl_util import generic_cast_map_as
from util.logger import get_logger
from util.scenario_util import load_scenario, find_encrypted_fields_map_as

logger = get_logger(__name__, KEBAB_STORM_LOGGING_LOCATION,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log')


def execute_crypto_action(source: DataFrame, scenario_json_path, action: CryptoAction) -> DataFrame:
    scenario = load_scenario(scenario_json_path)

    columns_to_encrypt = find_encrypted_fields_map_as(scenario)
    logger.info(f'{action.value} will be applied to following mapped column(s): {columns_to_encrypt}')

    if action is CryptoAction.encrypt:
        result = reduce(lambda x_df, col_name: x_df.withColumn(col_name, generic_encrypt_udf(
            func.struct(col_name, func.lit(col_name), func.lit(json.dumps(scenario)))).cast(BinaryType())),
                        columns_to_encrypt, source)
        del scenario
        return result

    result = reduce(lambda x_df, col_name: x_df.withColumn(col_name, generic_decrypt_udf(
        func.struct(col_name, func.lit(col_name), func.lit(json.dumps(scenario)))).cast(
        generic_cast_map_as(scenario, col_name))), columns_to_encrypt, source)

    del scenario
    return result

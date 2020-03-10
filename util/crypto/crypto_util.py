from pyspark.sql import functions as func
from pyspark.sql.types import BinaryType
import json

from util.constants import GENERIC_CRYPTO_COLUMN_INDEX, GENERIC_CRYPTO_SCENARIO_INDEX, GENERIC_CRYPTO_VALUE_INDEX
from util.crypto.aes_cipher_util import encrypt, decrypt
from util.scenario_util import find_encryption_key

generic_encrypt_udf = func.udf(lambda x: generic_encrypt(x[GENERIC_CRYPTO_VALUE_INDEX], x[GENERIC_CRYPTO_COLUMN_INDEX],
                                                         x[GENERIC_CRYPTO_SCENARIO_INDEX]), BinaryType())

generic_decrypt_udf = func.udf(lambda x: generic_decrypt(x[GENERIC_CRYPTO_VALUE_INDEX], x[GENERIC_CRYPTO_COLUMN_INDEX],
                                                         x[GENERIC_CRYPTO_SCENARIO_INDEX]))


def generic_encrypt(raw: str, column_name: str, scenario_json):
    if raw is None:
        return None

    return encrypt(find_encryption_key(json.loads(scenario_json), column_name), raw)


def generic_decrypt(enc: str, column_name: str, scenario_json):
    if enc is None:
        return None

    return str(decrypt(find_encryption_key(json.loads(scenario_json), column_name), enc), 'utf-8')

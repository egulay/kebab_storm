from pyspark.sql import Column
from pyspark.sql import functions as func
from pyspark.sql.types import StringType

AES_MODE = 'GCM'
AES_PADDING = 'NONE'
AAD_PREFIX = 'kebab-storm:v1:'


def encrypt_column(column: Column, hexadecimal_key: str, column_name: str) -> Column:
    """Encrypt a Spark value as raw AES-256-GCM binary bound to its mapped column."""
    return func.aes_encrypt(
        func.encode(column.cast(StringType()), 'UTF-8'),
        _binary_key(hexadecimal_key),
        func.lit(AES_MODE),
        func.lit(AES_PADDING),
        aad=_additional_authenticated_data(column_name),
    )


def decrypt_column(column: Column, hexadecimal_key: str, column_name: str) -> Column:
    """Authenticate and decrypt a Spark AES-256-GCM binary value as UTF-8 text."""
    return func.decode(
        func.aes_decrypt(
            column,
            _binary_key(hexadecimal_key),
            func.lit(AES_MODE),
            func.lit(AES_PADDING),
            aad=_additional_authenticated_data(column_name),
        ),
        'UTF-8',
    )


def _binary_key(hexadecimal_key: str) -> Column:
    return func.unhex(func.lit(hexadecimal_key))


def _additional_authenticated_data(column_name: str) -> Column:
    return func.lit(f'{AAD_PREFIX}{column_name}')

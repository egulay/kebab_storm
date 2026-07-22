from pyspark.sql import Column
from pyspark.sql import functions as func

from kebab_storm_analytics.types import to_spark_type

AES_MODE = 'GCM'
AES_PADDING = 'NONE'
AAD_PREFIX = 'kebab-storm:v1:'


def decrypt_column(column: Column, hexadecimal_key: str, column_name: str, type_definition: str) -> Column:
    """Strictly authenticate and decrypt one Kebab Storm binary field using native Spark AES."""
    plaintext = func.decode(
        func.aes_decrypt(
            column,
            func.unhex(func.lit(hexadecimal_key)),
            func.lit(AES_MODE),
            func.lit(AES_PADDING),
            aad=func.lit(f'{AAD_PREFIX}{column_name}'),
        ),
        'UTF-8',
    )
    return plaintext.cast(to_spark_type(type_definition))

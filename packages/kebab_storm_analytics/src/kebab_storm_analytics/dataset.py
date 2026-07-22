import os
import re
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import BinaryType, IntegerType, StringType, StructField, StructType, TimestampType

from kebab_storm_analytics.crypto import decrypt_column
from kebab_storm_analytics.scenario import encrypted_mapped_fields, field_by_mapped_name
from kebab_storm_analytics.types import to_spark_type
from kebab_storm_analytics.vault import load_scenario_from_vault

DATE_IMPORTED = 'DATE_IMPORTED'
SOFT_DELETED = 'SOFT_DELETED'
YEAR_IMPORTED = 'YEAR_IMPORTED'
DAY_PARTITION = 'day'
_SUPPORTED_STORAGE = {'parquet', 'iceberg'}


def read_dataset(
        spark: SparkSession,
        vault_secret=None,
        crypto_action='encrypted',
        decrypt_columns=None,
        start_date=None,
        end_date=None,
        include_soft_deleted=False,
        data_location=None,
        storage_provider=None,
        iceberg_catalog=None,
        iceberg_namespace=None,
        vault_addr=None,
        vault_token=None,
        vault_namespace=None,
        allow_insecure_vault_http=None) -> DataFrame:
    """Return a lazy, read-only Spark DataFrame described by a Vault scenario."""
    scenario = load_scenario_from_vault(
        secret_path=vault_secret,
        vault_addr=vault_addr,
        vault_token=vault_token,
        vault_namespace=vault_namespace,
        allow_insecure_http=allow_insecure_vault_http,
    )
    return _read_validated_scenario(
        spark=spark,
        scenario=scenario,
        crypto_action=crypto_action,
        decrypt_columns=decrypt_columns,
        start_date=start_date,
        end_date=end_date,
        include_soft_deleted=include_soft_deleted,
        data_location=data_location,
        storage_provider=storage_provider,
        iceberg_catalog=iceberg_catalog,
        iceberg_namespace=iceberg_namespace,
    )


def _read_validated_scenario(
        spark: SparkSession,
        scenario,
        crypto_action='encrypted',
        decrypt_columns=None,
        start_date=None,
        end_date=None,
        include_soft_deleted=False,
        data_location=None,
        storage_provider=None,
        iceberg_catalog=None,
        iceberg_namespace=None) -> DataFrame:
    action = str(crypto_action).strip().lower()
    if action not in {'encrypted', 'decrypted'}:
        raise ValueError('crypto_action must be encrypted or decrypted')
    if action == 'encrypted' and decrypt_columns is not None:
        raise ValueError('decrypt_columns can be used only when crypto_action is decrypted')

    root = str(data_location or os.getenv('KEBAB_STORM_DATA_LOCATION', '')).strip().rstrip('/')
    if not root:
        raise ValueError('A data_location or KEBAB_STORM_DATA_LOCATION is required')
    provider = str(storage_provider or os.getenv('KEBAB_STORM_STORAGE_PROVIDER', 'parquet')).strip().lower()
    if provider not in _SUPPORTED_STORAGE:
        raise ValueError(f'Unsupported storage provider: {provider}')

    start_day = _day_value(start_date, 'start_date')
    end_day = _day_value(end_date, 'end_date')
    if start_day and end_day and start_day > end_day:
        raise ValueError('start_date must not be later than end_date')

    logical_location = f'{root}/{scenario["import_save_location"].lstrip("/")}'
    if provider == 'iceberg':
        data = _read_iceberg(
            spark,
            logical_location,
            root,
            scenario,
            iceberg_catalog or os.getenv('KEBAB_STORM_ICEBERG_CATALOG', 'kebab'),
            iceberg_namespace or os.getenv('KEBAB_STORM_ICEBERG_NAMESPACE', 'default'),
        )
    else:
        data = _read_parquet(spark, logical_location, scenario, start_day, end_day)

    if DATE_IMPORTED in data.columns:
        if start_day:
            data = data.filter(func.col(DATE_IMPORTED) >= start_day)
        if end_day:
            data = data.filter(func.col(DATE_IMPORTED) <= end_day)
    if not include_soft_deleted and SOFT_DELETED in data.columns:
        data = data.filter(func.col(SOFT_DELETED).isNull())

    if action == 'encrypted':
        return data
    return _decrypt_fields(data, scenario, decrypt_columns)


def _decrypt_fields(data: DataFrame, scenario, requested_columns) -> DataFrame:
    encrypted_fields = encrypted_mapped_fields(scenario)
    if requested_columns is None:
        selected = set(encrypted_fields)
    else:
        selected = {str(column).strip() for column in requested_columns}
        blank = '' in selected
        unknown = sorted(selected.difference(encrypted_fields))
        if blank or unknown:
            invalid = unknown + (['<blank>'] if blank else [])
            raise ValueError(f'decrypt_columns contains non-encrypted or unknown fields: {", ".join(invalid)}')

    projected = []
    for column_name in data.columns:
        if column_name not in selected:
            projected.append(func.col(column_name))
            continue
        field = field_by_mapped_name(scenario, column_name)
        projected.append(decrypt_column(
            func.col(column_name), field['encryption_key'], column_name, field['type']).alias(column_name))
    return data.select(*projected)


def _read_parquet(spark, logical_location, scenario, start_day, end_day):
    if scenario['is_apply_year_to_save_location']:
        if start_day and end_day:
            years = range(int(start_day[:4]), int(end_day[:4]) + 1)
            candidates = [f'{logical_location}_{year}' for year in years]
            paths = [path for path in candidates if _filesystem_path_exists(spark, path)]
        else:
            paths = _year_partition_paths(spark, logical_location)
    else:
        paths = [logical_location] if _filesystem_path_exists(spark, logical_location) else []

    if not paths:
        return spark.createDataFrame([], _encrypted_storage_schema(scenario))
    return spark.read.parquet(*paths)


def _read_iceberg(spark, logical_location, data_root, scenario, catalog, namespace):
    table_name = _iceberg_table_name(logical_location, data_root, catalog, namespace)
    if not spark.catalog.tableExists(table_name):
        return spark.createDataFrame([], _encrypted_storage_schema(scenario))
    return spark.table(table_name)


def _iceberg_table_name(logical_location, data_root, catalog, namespace):
    relative_location = logical_location
    if relative_location.startswith(data_root):
        relative_location = relative_location[len(data_root):]
    table = re.sub(r'[^a-zA-Z0-9_]+', '_', relative_location.strip('/')).strip('_').lower()
    if not table:
        raise ValueError(f'Cannot derive an Iceberg table name from location: {logical_location!r}')
    return f'{catalog}.{namespace}.{table}'


def _filesystem_path_exists(spark, location):
    path = spark._jvm.org.apache.hadoop.fs.Path(location)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return filesystem.exists(path)


def _year_partition_paths(spark, logical_location):
    path = spark._jvm.org.apache.hadoop.fs.Path(logical_location)
    parent = path.getParent()
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if parent is None or not filesystem.exists(parent):
        return []
    prefix = f'{path.getName()}_'
    matches = []
    for status in filesystem.listStatus(parent):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith(prefix) and name[len(prefix):].isdigit() \
                and len(name[len(prefix):]) == 4:
            matches.append(status.getPath().toString())
    return sorted(matches)


def _encrypted_storage_schema(scenario):
    fields = []
    for field in scenario['fields']:
        data_type = BinaryType() if field['is_encrypted'] else to_spark_type(field['type'])
        fields.append(StructField(field['map_as'], data_type, True))
    fields.extend([
        StructField(DATE_IMPORTED, StringType(), True),
        StructField(DAY_PARTITION, StringType(), True),
        StructField(SOFT_DELETED, TimestampType(), True),
    ])
    if scenario['is_apply_year_to_save_location']:
        fields.append(StructField(YEAR_IMPORTED, IntegerType(), True))
    return StructType(fields)


def _day_value(value, argument_name):
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        try:
            parsed = datetime.strptime(str(value), '%Y-%m-%d').date()
        except ValueError as error:
            raise ValueError(f'{argument_name} must use YYYY-MM-DD format') from error
    return parsed.strftime('%Y%m%d')

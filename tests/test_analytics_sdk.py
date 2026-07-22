import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

ANALYTICS_SOURCE = Path(__file__).resolve().parents[1] / 'packages' / 'kebab_storm_analytics' / 'src'
sys.path.insert(0, str(ANALYTICS_SOURCE))

from kebab_storm_analytics.dataset import _iceberg_table_name, read_dataset
from kebab_storm_analytics.scenario import validate_scenario
from kebab_storm_analytics.vault import VaultScenarioError, load_scenario_from_vault
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import BinaryType

from util.crypto.crypto_util import encrypt_column
from util.crypto.crypto_util import AAD_PREFIX as FRAMEWORK_AAD_PREFIX
from kebab_storm_analytics.crypto import AAD_PREFIX as ANALYTICS_AAD_PREFIX


class _VaultResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return json.dumps(self.payload).encode('utf-8')


def _scenario():
    return {
        'entity_name': 'Health',
        'import_save_location': '/Health',
        'report_save_location': '/Health_Reports',
        'temp_save_location': '/tmp/Health',
        'import_mode': 'append',
        'delete_input_file_after_import': False,
        'id_field': 'Secret',
        'delimiter': ',',
        'hard_delete_condition_in_years': 3,
        'enforce_data_model': True,
        'is_apply_year_to_save_location': False,
        'fields': [
            {
                'name': 'Secret',
                'map_as': 'SECRET',
                'type': 'string|integer',
                'encryption_key':
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                'is_encrypted': True,
            },
            {
                'name': 'Category',
                'map_as': 'CATEGORY',
                'type': 'string|string',
                'is_encrypted': False,
            },
        ],
    }


class AnalyticsVaultTest(unittest.TestCase):
    def test_reads_and_validates_standard_vault_kv_v2_response(self):
        scenario = _scenario()
        response = _VaultResponse({'data': {'data': {'scenario': json.dumps(scenario)}}})
        with patch('kebab_storm_analytics.vault.urlopen', return_value=response) as request:
            loaded = load_scenario_from_vault(
                secret_path='secret/kebab-storm/health',
                vault_addr='http://vault:8200',
                vault_token='demo-token',
                allow_insecure_http=True,
            )

        self.assertEqual(scenario, loaded)
        self.assertEqual(
            'http://vault:8200/v1/secret/data/kebab-storm/health',
            request.call_args.args[0].full_url,
        )

    def test_rejects_insecure_vault_without_explicit_demo_permission(self):
        with self.assertRaisesRegex(VaultScenarioError, 'must use HTTPS'):
            load_scenario_from_vault(
                secret_path='secret/kebab-storm/health',
                vault_addr='http://vault:8200',
                vault_token='demo-token',
                allow_insecure_http=False,
            )

    def test_vault_network_error_identifies_address_and_configuration(self):
        with patch('kebab_storm_analytics.vault.urlopen', side_effect=URLError('name lookup failed')):
            with self.assertRaisesRegex(
                    VaultScenarioError,
                    r'Unable to reach Vault at https://vault\.internal:8200.*Verify VAULT_ADDR'):
                load_scenario_from_vault(
                    secret_path='secret/kebab-storm/health',
                    vault_addr='https://vault.internal:8200',
                    vault_token='demo-token',
                )

    def test_packaged_schema_matches_framework_schema(self):
        project_root = Path(__file__).resolve().parents[1]
        framework_schema = json.loads(
            (project_root / 'scenario/schema/kebab_storm_scenario.schema.json').read_text(encoding='utf-8'))
        sdk_schema = json.loads(
            (ANALYTICS_SOURCE / 'kebab_storm_analytics/schema/kebab_storm_scenario.schema.json')
            .read_text(encoding='utf-8'))
        self.assertEqual(framework_schema, sdk_schema)

    def test_sdk_uses_the_framework_ciphertext_aad_contract(self):
        self.assertEqual(FRAMEWORK_AAD_PREFIX, ANALYTICS_AAD_PREFIX)

    def test_real_project_scenarios_satisfy_the_sdk_contract(self):
        project_root = Path(__file__).resolve().parents[1]
        for scenario_name in ('sales_records_scenario.json', 'personal_health_records_scenario.json'):
            with self.subTest(scenario=scenario_name):
                scenario = json.loads((project_root / 'scenario' / scenario_name).read_text(encoding='utf-8'))
                self.assertIs(scenario, validate_scenario(scenario))

    def test_iceberg_table_name_matches_framework_location_mapping(self):
        self.assertEqual(
            'kebab.default.personal_health_records',
            _iceberg_table_name(
                '/data/kebab_storm/Personal_Health_Records',
                '/data/kebab_storm',
                'kebab',
                'default',
            ),
        )


class AnalyticsDatasetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        cls.spark = SparkSession.builder.master('local[1]').appName('kebab-storm-analytics-tests').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_returns_encrypted_or_selectively_decrypted_read_only_dataframe(self):
        scenario = validate_scenario(_scenario())
        key = scenario['fields'][0]['encryption_key']
        source = self.spark.createDataFrame([
            (42, 'active', '20260721', None),
            (99, 'deleted', '20260721', datetime(2026, 7, 22)),
        ], ['SECRET', 'CATEGORY', 'DATE_IMPORTED', 'SOFT_DELETED'])
        encrypted_source = source.withColumn('SECRET', encrypt_column(func.col('SECRET'), key, 'SECRET'))

        with tempfile.TemporaryDirectory() as temp_directory:
            encrypted_source.write.mode('overwrite').parquet(f'{temp_directory}/Health')
            with patch('kebab_storm_analytics.dataset.load_scenario_from_vault', return_value=scenario):
                encrypted = read_dataset(
                    self.spark,
                    crypto_action='encrypted',
                    start_date='2026-07-21',
                    end_date='2026-07-21',
                    data_location=temp_directory,
                )
                decrypted = read_dataset(
                    self.spark,
                    crypto_action='decrypted',
                    decrypt_columns=['SECRET'],
                    data_location=temp_directory,
                )
                self.assertIsInstance(encrypted.schema['SECRET'].dataType, BinaryType)
                self.assertEqual(1, encrypted.count())
                row = decrypted.first()
                self.assertEqual(42, row['SECRET'])
                self.assertEqual('active', row['CATEGORY'])
                self.assertNotIn('PythonUDF', decrypted._jdf.queryExecution().optimizedPlan().toString())

    def test_defaults_to_encrypted_and_rejects_decrypt_columns_in_encrypted_mode(self):
        scenario = validate_scenario(_scenario())
        with patch('kebab_storm_analytics.dataset.load_scenario_from_vault', return_value=scenario):
            with self.assertRaisesRegex(ValueError, 'only when crypto_action is decrypted'):
                read_dataset(
                    self.spark,
                    decrypt_columns=['SECRET'],
                    data_location='/does/not-matter',
                )


if __name__ == '__main__':
    unittest.main()

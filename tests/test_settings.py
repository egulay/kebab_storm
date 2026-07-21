import os
import unittest
from unittest.mock import patch

from conf.setup import Settings


class SettingsTest(unittest.TestCase):
    def test_defaults_to_local_profile(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()

        self.assertEqual('local', settings.active_profile)
        self.assertEqual('local[*]', settings.spark_master)
        self.assertFalse(settings.enable_hive_support)

    def test_selects_profile_from_environment(self):
        with patch.dict(os.environ, {'KEBAB_STORM_ENV': ' DEV '}, clear=True):
            settings = Settings()

        self.assertEqual('dev', settings.active_profile)
        self.assertEqual('yarn', settings.spark_master)
        self.assertTrue(settings.enable_hive_support)

    def test_rejects_unknown_profile(self):
        with patch.dict(os.environ, {'KEBAB_STORM_ENV': '../unknown'}, clear=True):
            with self.assertRaisesRegex(ValueError, 'Unsupported KEBAB_STORM_ENV value'):
                Settings()

    def test_merges_shared_and_profile_spark_configuration(self):
        settings = Settings('local')
        spark_config = dict(settings.spark_config)

        self.assertEqual('org.apache.spark.serializer.KryoSerializer', spark_config['spark.serializer'])
        self.assertEqual('2g', spark_config['spark.driver.maxResultSize'])
        self.assertEqual(8, spark_config['spark.sql.shuffle.partitions'])
        self.assertGreater(len(spark_config), 10)

    def test_environment_values_override_yaml(self):
        overrides = {
            'KEBAB_STORM_SPARK_MASTER': 'local[2]',
            'KEBAB_STORM_LOGGING_LOCATION': '/tmp/kebab-storm-test-logs',
            'KEBAB_STORM_DATA_LOCATION': '/tmp/kebab-storm-test-data',
        }
        with patch.dict(os.environ, overrides, clear=True):
            settings = Settings('prod')

        self.assertEqual('local[2]', settings.spark_master)
        self.assertEqual('/tmp/kebab-storm-test-logs', settings.logging_location)
        self.assertEqual('/tmp/kebab-storm-test-data', settings.default_data_location)


if __name__ == '__main__':
    unittest.main()

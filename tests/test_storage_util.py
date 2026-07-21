import unittest
from types import SimpleNamespace
from unittest.mock import patch

from util import storage_util


class StorageUtilTest(unittest.TestCase):
    def test_derives_stable_iceberg_table_name_from_logical_location(self):
        test_settings = SimpleNamespace(
            storage_provider='iceberg',
            default_data_location='/data/kebab_storm',
            iceberg_catalog='kebab',
            iceberg_namespace='analytics',
        )

        with patch.object(storage_util, 'settings', test_settings):
            table_name = storage_util.iceberg_table_name(
                '/data/kebab_storm/Sales_Reports/Simple_Report')

        self.assertEqual('kebab.analytics.sales_reports_simple_report', table_name)

    def test_reports_the_selected_physical_storage_model(self):
        raw_settings = SimpleNamespace(storage_provider='parquet')
        iceberg_settings = SimpleNamespace(storage_provider='iceberg')

        with patch.object(storage_util, 'settings', raw_settings):
            self.assertEqual('raw Parquet', storage_util.storage_description())
        with patch.object(storage_util, 'settings', iceberg_settings):
            self.assertEqual('Iceberg table backed by Parquet', storage_util.storage_description())


if __name__ == '__main__':
    unittest.main()

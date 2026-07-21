import unittest
from unittest.mock import Mock, patch

from etl.importer import get_reporting_data_between_dates
from util.crypto.crypto_action import CryptoAction


class ReportingDataTest(unittest.TestCase):
    @patch('etl.importer.hdfs_path_exist', return_value=False)
    @patch('etl.importer.get_scenario_defaults')
    def test_cross_year_range_without_storage_returns_consistent_empty_result(
            self, get_scenario_defaults, _hdfs_path_exist):
        get_scenario_defaults.return_value = (
            'PersonalHealthRecords', '/health', '/tmp/health', 'parquet', 'append',
            'Patient_Record_ID', ',', 3, True, True, '/health_reports', 'parquet',
        )
        empty_data = Mock()
        spark = Mock()
        spark.createDataFrame.return_value = empty_data

        result = get_reporting_data_between_dates(
            spark,
            'scenario.json',
            CryptoAction.decrypt,
            False,
            '2025-12-31',
            '2026-01-01',
        )

        self.assertEqual((empty_data, 'parquet', '/health_reports', True), result)


if __name__ == '__main__':
    unittest.main()

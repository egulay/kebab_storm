import unittest
from unittest.mock import Mock, patch

from etl.importer import encrypt_and_import


class InputFileCleanupTest(unittest.TestCase):
    def setUp(self):
        self.spark = Mock()
        self.raw_data = Mock()
        self.raw_data.isEmpty.return_value = False
        self.spark.read.option.return_value.csv.return_value = self.raw_data
        self.refined_data = Mock()
        self.encrypted_data = Mock()
        self.scenario_defaults = (
            'Sales', '/data/Sales', '/tmp/Sales', 'append', 'Order_ID', ',', 3,
            False, False, '/data/Sales_Reports', True,
        )

    @patch('etl.importer.storage_description', return_value='raw Parquet')
    @patch('etl.importer.delete_input_file')
    @patch('etl.importer.write_storage')
    @patch('etl.importer.execute_crypto_action')
    @patch('etl.importer.validate_and_refine')
    @patch('etl.importer.get_scenario_defaults')
    def test_deletes_input_after_successful_storage_write(
            self, get_defaults, refine, encrypt, write, delete, _description):
        get_defaults.return_value = self.scenario_defaults
        refine.return_value = self.refined_data
        encrypt.return_value = self.encrypted_data

        encrypt_and_import(self.spark, 'scenario.json', 's3a://landing/sales/sales.csv', '2026-07-21')

        write.assert_called_once()
        delete.assert_called_once_with(self.spark, 's3a://landing/sales/sales.csv')

    @patch('etl.importer.storage_description', return_value='raw Parquet')
    @patch('etl.importer.delete_input_file')
    @patch('etl.importer.write_storage', side_effect=RuntimeError('write failed'))
    @patch('etl.importer.execute_crypto_action')
    @patch('etl.importer.validate_and_refine')
    @patch('etl.importer.get_scenario_defaults')
    def test_preserves_input_when_storage_write_fails(
            self, get_defaults, refine, encrypt, _write, delete, _description):
        get_defaults.return_value = self.scenario_defaults
        refine.return_value = self.refined_data
        encrypt.return_value = self.encrypted_data

        with self.assertRaisesRegex(RuntimeError, 'write failed'):
            encrypt_and_import(self.spark, 'scenario.json', 'ofs://ozone/vol/bucket/sales.csv', '2026-07-21')

        delete.assert_not_called()


if __name__ == '__main__':
    unittest.main()

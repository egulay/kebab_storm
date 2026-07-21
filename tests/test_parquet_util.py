import unittest

from util.parquet_util import _partitioned_folder_year


class ParquetUtilTest(unittest.TestCase):
    def test_accepts_only_exact_year_partition_folder_names(self):
        self.assertEqual(2026, _partitioned_folder_year('Sales_2026', 'Sales'))
        self.assertIsNone(_partitioned_folder_year('Sales_Reports', 'Sales'))
        self.assertIsNone(_partitioned_folder_year('Sales_backup_2026', 'Sales'))
        self.assertIsNone(_partitioned_folder_year('Other_2026', 'Sales'))
        self.assertIsNone(_partitioned_folder_year('Sales_26', 'Sales'))


if __name__ == '__main__':
    unittest.main()

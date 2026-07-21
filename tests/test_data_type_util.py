import unittest

from util.data_type_util import parse_data_type, to_spark_datetime_format


class DataTypeUtilTest(unittest.TestCase):
    def test_normalizes_integer_aliases(self):
        self.assertEqual('integer', parse_data_type('int').name)
        self.assertEqual('long', parse_data_type('BIGINT').name)
        self.assertEqual('short', parse_data_type('smallint').name)

    def test_parses_parameterized_decimal(self):
        spec = parse_data_type('decimal(18, 2)')
        self.assertEqual(('decimal', 18, 2), (spec.name, spec.precision, spec.scale))
        self.assertEqual('string', spec.source.name)

    def test_parses_explicit_source_target_and_options(self):
        spec = parse_data_type('timestamp|long|source_format=%Y-%m-%d %H:%M:%S|epoch_unit=seconds')
        self.assertEqual('timestamp', spec.source.name)
        self.assertEqual('long', spec.target.name)
        self.assertEqual('%Y-%m-%d %H:%M:%S', spec.options['source_format'])
        self.assertEqual('seconds', spec.options['epoch_unit'])

    def test_rejects_invalid_decimal_parameters(self):
        with self.assertRaises(ValueError):
            parse_data_type('decimal(39,2)')
        with self.assertRaises(ValueError):
            parse_data_type('decimal(2,3)')

    def test_rejects_unsupported_or_ambiguous_conversions(self):
        with self.assertRaises(ValueError):
            parse_data_type('unknown')
        with self.assertRaises(ValueError):
            parse_data_type('timestamp|boolean')
        with self.assertRaises(ValueError):
            parse_data_type('long|timestamp')
        with self.assertRaises(ValueError):
            parse_data_type('string|integer|broken-option')

    def test_preserves_flexible_timestamp_shorthand(self):
        spec = parse_data_type('timestamp|%m/%d/%Y|%Y-%m-%d')
        self.assertEqual('timestamp', spec.name)
        self.assertEqual('%m/%d/%Y', spec.source_format)
        self.assertEqual('%Y-%m-%d', spec.destination_format)

    def test_translates_python_datetime_format_to_spark(self):
        self.assertEqual('MM/dd/yyyy HH:mm:ss', to_spark_datetime_format('%m/%d/%Y %H:%M:%S'))
        self.assertEqual(
            'M/d/yyyy H:m:s',
            to_spark_datetime_format('%m/%d/%Y %H:%M:%S', flexible_width=True),
        )


if __name__ == '__main__':
    unittest.main()

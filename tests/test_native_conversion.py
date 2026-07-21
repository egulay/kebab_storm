import unittest
import json
import os
import sys
import time
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import BinaryType

from util.crypto.crypto_util import generic_decrypt_udf, generic_encrypt_udf
from util.etl_util import conversion_error_condition, native_convert, to_spark_type


class NativeConversionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['TZ'] = 'UTC'
        if hasattr(time, 'tzset'):
            time.tzset()
        cls.spark = SparkSession.builder.master('local[1]').appName('kebab-storm-tests') \
            .config('spark.sql.session.timeZone', 'UTC').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def convert(self, values, type_definition):
        source = self.spark.createDataFrame([(value,) for value in values], ['raw'])
        return [row['converted'] for row in source.select(
            native_convert(func.col('raw'), type_definition).alias('converted')
        ).collect()]

    def test_integer_alias_uses_native_safe_cast(self):
        self.assertEqual([42, None, None], self.convert([' 42 ', 'broken', ''], 'int'))

    def test_conversion_plan_contains_no_python_udf(self):
        source = self.spark.createDataFrame([('42',)], ['raw'])
        converted = source.select(native_convert(func.col('raw'), 'string|integer').alias('converted'))
        plan = converted._jdf.queryExecution().optimizedPlan().toString()
        self.assertNotIn('PythonUDF', plan)

    def test_conversion_error_condition_counts_only_nonblank_failures(self):
        source = self.spark.createDataFrame([('42',), ('broken',), ('',), (None,)], ['raw'])
        error_count = source.select(func.sum(func.when(
            conversion_error_condition(func.col('raw'), 'string|integer'), 1
        ).otherwise(0)).alias('errors')).first()['errors']
        self.assertEqual(1, error_count)

    def test_numeric_and_string_conversions(self):
        self.assertEqual(['42', None], self.convert(['42', 'broken'], 'integer|string'))
        self.assertEqual(['123.40', None], self.convert(['123.40', 'broken'], 'decimal(6,2)|string'))
        self.assertEqual(['0.125', None], self.convert(['0.12519', 'broken'], 'double|string|target_format=%.3f'))

    def test_parameterized_decimal_preserves_scale(self):
        self.assertEqual(
            [Decimal('123.45'), None],
            self.convert(['123.45', '12345678901234567.89'], 'decimal(6,2)'),
        )

    def test_timestamp_shorthand_is_parsed_natively(self):
        self.assertEqual(
            [datetime(2012, 3, 21), None],
            self.convert(['3/21/2012', '3/2j/2012'], 'timestamp|%m/%d/%Y|%Y-%m-%d'),
        )

    def test_date_and_boolean_conversion(self):
        self.assertEqual([date(2026, 7, 21), None], self.convert(['2026-07-21', 'bad'], 'date|%Y-%m-%d'))
        self.assertEqual([True, False, None], self.convert(['yes', '0', 'unknown'], 'boolean'))

    def test_configurable_boolean_conversions(self):
        self.assertEqual(
            [True, False, None],
            self.convert(['positive', 'negative', 'unknown'],
                         'string|boolean|true_values=positive,p|false_values=negative,n'),
        )
        self.assertEqual(
            ['ACTIVE', 'INACTIVE', None],
            self.convert(['yes', 'no', 'unknown'], 'boolean|string|true_value=ACTIVE|false_value=INACTIVE'),
        )
        self.assertEqual([True, False, None], self.convert(['1', '0', '2'], 'integer|boolean'))

    def test_epoch_and_formatted_timestamp_conversions(self):
        self.assertEqual(
            [datetime(1970, 1, 1), None],
            self.convert(['0', 'bad'], 'long|timestamp|epoch_unit=seconds'),
        )
        self.assertEqual(
            [0, None],
            self.convert(['1970-01-01 00:00:00', 'bad'],
                         'timestamp|long|source_format=%Y-%m-%d %H:%M:%S|epoch_unit=seconds'),
        )
        self.assertEqual(
            ['21/07/2026', None],
            self.convert(['2026-07-21', 'bad'],
                         'date|string|source_format=%Y-%m-%d|target_format=%d/%m/%Y'),
        )

    def test_custom_aes_udf_keeps_binary_storage(self):
        scenario = {
            'fields': [{
                'name': 'Secret',
                'map_as': 'SECRET',
                'type': 'string|integer',
                'encryption_key': '0123456789abcdef0123456789abcdef',
                'is_encrypted': True,
            }]
        }
        source = self.spark.createDataFrame([(42,)], ['SECRET'])
        encrypted = source.select(generic_encrypt_udf(func.struct(
            func.col('SECRET'), func.lit('SECRET'), func.lit(json.dumps(scenario))
        )).alias('SECRET'))

        self.assertIsInstance(encrypted.schema['SECRET'].dataType, BinaryType)

        decrypted = encrypted.select(generic_decrypt_udf(func.struct(
            func.col('SECRET'), func.lit('SECRET'), func.lit(json.dumps(scenario))
        )).cast(to_spark_type(scenario['fields'][0]['type'])).alias('SECRET')).first()['SECRET']
        self.assertEqual(42, decrypted)


if __name__ == '__main__':
    unittest.main()

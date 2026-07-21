import os
import sys
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from reporter.personal_health_report import create_personal_health_report as report_module
from util.constants import CLI_DATE, CLI_INCLUDE_SOFT_DELETED, CLI_SCENARIO_JSON_PATH
from util.crypto.crypto_action import CryptoAction


class _ConfigValue:
    def __init__(self, value):
        self.value = value

    def get(self, *_args):
        return self.value


class PersonalHealthReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        cls.spark = SparkSession.builder.master('local[1]').appName('kebab-storm-health-report-tests') \
            .config('spark.ui.enabled', 'false').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_report_decrypts_then_groups_health_values(self):
        source = self.spark.createDataFrame([
            {
                'PATIENT_RECORD_ID': 'PHR1', 'DIAGNOSIS_CODE': 'I10', 'BIOLOGICAL_SEX': 'Female',
                'AGE': 42, 'BMI': 28.0, 'GLUCOSE_LEVEL': 100.0, 'SYSTOLIC_BP': 135,
                'DAILY_STEPS': 8000, 'IS_SMOKER': False, 'HAS_CONSENT': True, 'RISK_SCORE_TEXT': '0.500',
            },
            {
                'PATIENT_RECORD_ID': 'PHR2', 'DIAGNOSIS_CODE': 'I10', 'BIOLOGICAL_SEX': 'Female',
                'AGE': 48, 'BMI': 32.0, 'GLUCOSE_LEVEL': 140.0, 'SYSTOLIC_BP': 155,
                'DAILY_STEPS': 6000, 'IS_SMOKER': True, 'HAS_CONSENT': False, 'RISK_SCORE_TEXT': '0.800',
            },
            {
                'PATIENT_RECORD_ID': 'PHR3', 'DIAGNOSIS_CODE': 'Z00.129', 'BIOLOGICAL_SEX': 'Male',
                'AGE': 12, 'BMI': 19.0, 'GLUCOSE_LEVEL': 88.0, 'SYSTOLIC_BP': 105,
                'DAILY_STEPS': 12000, 'IS_SMOKER': False, 'HAS_CONSENT': True, 'RISK_SCORE_TEXT': '0.100',
            },
        ])
        settings = SimpleNamespace(
            active_profile='local',
            active_config={
                CLI_DATE: _ConfigValue('2026-07-21'),
                CLI_INCLUDE_SOFT_DELETED: _ConfigValue('no'),
                CLI_SCENARIO_JSON_PATH: _ConfigValue('scenario.json'),
            },
        )
        report_rows = []

        @contextmanager
        def application_context(*_args, **_kwargs):
            yield self.spark, Mock()

        def capture_report(data, *_args):
            report_rows.extend(row.asDict() for row in data.collect())

        with patch.object(report_module, 'settings', settings), \
                patch.object(report_module, 'set_args'), \
                patch.object(report_module, 'spark_application', application_context), \
                patch.object(report_module, 'get_day_partition_name_and_year', return_value=('20260721', 2026)), \
                patch.object(report_module, 'get_reporting_data',
                             return_value=(source, 'parquet', '/reports', False)) as get_reporting_data, \
                patch.object(report_module, 'write_spark_parquet', side_effect=capture_report):
            report_module.main()

        get_reporting_data.assert_called_once_with(
            self.spark, 'scenario.json', CryptoAction.decrypt, False, '2026-07-21')
        groups = {
            (row['DIAGNOSIS_CODE'], row['BIOLOGICAL_SEX'], row['AGE_BAND']): row
            for row in report_rows
        }
        hypertension = groups[('I10', 'Female', '35-49')]
        self.assertEqual(2, hypertension['PATIENT_COUNT'])
        self.assertEqual(45.0, hypertension['AVERAGE_AGE'])
        self.assertEqual(30.0, hypertension['AVERAGE_BMI'])
        self.assertEqual(120.0, hypertension['AVERAGE_GLUCOSE_LEVEL'])
        self.assertEqual(145.0, hypertension['AVERAGE_SYSTOLIC_BP'])
        self.assertEqual(7000, hypertension['AVERAGE_DAILY_STEPS'])
        self.assertEqual(1, hypertension['SMOKER_COUNT'])
        self.assertEqual(1, hypertension['CONSENTED_PATIENT_COUNT'])
        self.assertEqual(1, hypertension['HIGH_RISK_PATIENT_COUNT'])
        self.assertEqual('20260721', hypertension['day'])
        self.assertIn(('Z00.129', 'Male', '0-17'), groups)


if __name__ == '__main__':
    unittest.main()

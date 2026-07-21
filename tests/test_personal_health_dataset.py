import json
import os
import sys
import time
import unittest
from pathlib import Path

from pyspark.sql import SparkSession, functions as func

from util.etl_util import native_convert


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / 'data' / '50k_personal_health_records.csv'
SCENARIO_PATH = PROJECT_ROOT / 'scenario' / 'personal_health_records_scenario.json'


class PersonalHealthDatasetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['TZ'] = 'UTC'
        if hasattr(time, 'tzset'):
            time.tzset()

        cls.spark = SparkSession.builder.master('local[2]').appName('kebab-storm-health-data-tests') \
            .config('spark.sql.session.timeZone', 'UTC').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

        with SCENARIO_PATH.open() as scenario_file:
            cls.scenario = json.load(scenario_file)
        cls.source = cls.spark.read.option('delimiter', cls.scenario['delimiter']) \
            .csv(str(DATA_PATH), inferSchema=False, header=True)
        cls.converted = cls.source.select(*[
            native_convert(func.col(field['name']), field['type']).alias(field['map_as'])
            for field in cls.scenario['fields']
        ]).cache()
        cls.converted.count()

    @classmethod
    def tearDownClass(cls):
        cls.converted.unpersist()
        cls.spark.stop()

    def test_record_count_and_headers_match_scenario(self):
        self.assertEqual(50_000, self.source.count())
        self.assertEqual([field['name'] for field in self.scenario['fields']], self.source.columns)

    def test_patient_population_is_varied_and_plausible(self):
        profile = self.converted.agg(
            func.countDistinct('PATIENT_NAME').alias('distinct_names'),
            func.min('AGE').alias('minimum_age'),
            func.max('AGE').alias('maximum_age'),
            func.avg('BMI').alias('average_bmi'),
        ).first()

        self.assertGreater(profile['distinct_names'], 5_000)
        self.assertEqual(0, profile['minimum_age'])
        self.assertEqual(100, profile['maximum_age'])
        self.assertGreater(profile['average_bmi'], 20)
        self.assertLess(profile['average_bmi'], 30)
        self.assertEqual(0, self.converted.filter(func.col('PATIENT_NAME').startswith('Synthetic Person')).count())

    def test_representative_target_types(self):
        target_types = {field.name: field.dataType.simpleString() for field in self.converted.schema.fields}
        self.assertEqual('int', target_types['AGE'])
        self.assertEqual('string', target_types['VISIT_COUNT_TEXT'])
        self.assertEqual('tinyint', target_types['MEDICATION_COUNT'])
        self.assertEqual('decimal(5,2)', target_types['HEIGHT_CM'])
        self.assertEqual('float', target_types['GLUCOSE_LEVEL'])
        self.assertEqual('date', target_types['BIRTH_DATE'])
        self.assertEqual('timestamp', target_types['APPOINTMENT_AT'])
        self.assertEqual('bigint', target_types['DISCHARGE_EPOCH_SECONDS'])
        self.assertEqual('boolean', target_types['HAS_CONSENT'])
        self.assertEqual('boolean', target_types['IS_SMOKER'])

    def test_controlled_corruptions_become_expected_nulls(self):
        expected_nulls = {
            'AGE': 50,
            'HEIGHT_CM': 50,
            'HEART_RATE': 50,
            'RISK_SCORE_TEXT': 51,
            'DAILY_STEPS': 51,
            'BIRTH_DATE': 51,
            'APPOINTMENT_AT': 52,
            'DISCHARGE_EPOCH_SECONDS': 52,
            'HAS_CONSENT': 53,
            'ACTIVE_FLAG': 53,
            'IS_SMOKER': 53,
        }
        counts = self.converted.agg(*[
            func.sum(func.when(func.col(column).isNull(), 1).otherwise(0)).alias(column)
            for column in expected_nulls
        ]).first().asDict()
        self.assertEqual(expected_nulls, counts)

    def test_conversion_plan_is_native(self):
        plan = self.converted._jdf.queryExecution().optimizedPlan().toString()
        self.assertNotIn('PythonUDF', plan)


if __name__ == '__main__':
    unittest.main()

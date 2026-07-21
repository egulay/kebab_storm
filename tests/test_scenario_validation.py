import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator

from util.scenario_util import SCENARIO_SCHEMA_PATH, ScenarioSchemaValidationError, validate_scenario


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ScenarioValidationTest(unittest.TestCase):
    def setUp(self):
        with (PROJECT_ROOT / 'scenario' / 'sales_records_scenario.json').open() as scenario_file:
            self.scenario = json.load(scenario_file)

    def test_sales_scenario_uses_valid_explicit_conversions(self):
        validate_scenario(self.scenario)

    def test_personal_health_scenario_is_valid(self):
        with (PROJECT_ROOT / 'scenario' / 'personal_health_records_scenario.json').open() as scenario_file:
            validate_scenario(json.load(scenario_file))

    def test_json_schema_definition_is_valid(self):
        with SCENARIO_SCHEMA_PATH.open(encoding='utf-8') as schema_file:
            Draft202012Validator.check_schema(json.load(schema_file))

    def test_rejects_missing_top_level_setting_with_json_path(self):
        scenario = copy.deepcopy(self.scenario)
        del scenario['delimiter']

        with self.assertRaisesRegex(
                ScenarioSchemaValidationError,
                r"at \$: 'delimiter' is a required property"):
            validate_scenario(scenario)

    def test_rejects_wrong_nested_type_with_json_path(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][0]['is_encrypted'] = 'yes'

        with self.assertRaisesRegex(
                ScenarioSchemaValidationError,
                r"at \$\.fields\[0\]\.is_encrypted: 'yes' is not of type 'boolean'"):
            validate_scenario(scenario)

    def test_rejects_unknown_scenario_setting(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['data_generation'] = {'records': 50000}

        with self.assertRaisesRegex(ScenarioSchemaValidationError, 'Additional properties are not allowed'):
            validate_scenario(scenario)

    def test_rejects_removed_scenario_storage_type(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['import_save_type'] = 'parquet'

        with self.assertRaisesRegex(ScenarioSchemaValidationError, 'import_save_type'):
            validate_scenario(scenario)

    def test_accepts_explicit_conversion(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][0]['type'] = 'string|boolean|true_values=yes|false_values=no'
        validate_scenario(scenario)

    def test_rejects_duplicate_mapped_fields(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][1]['map_as'] = scenario['fields'][0]['map_as']
        with self.assertRaisesRegex(ValueError, 'Duplicate mapped field'):
            validate_scenario(scenario)

    def test_rejects_missing_encryption_key(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][0]['encryption_key'] = ''
        with self.assertRaisesRegex(ScenarioSchemaValidationError, 'is too short'):
            validate_scenario(scenario)

    def test_rejects_short_or_reused_encryption_keys(self):
        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][0]['encryption_key'] = 'too-short'
        with self.assertRaisesRegex(ScenarioSchemaValidationError, 'is too short'):
            validate_scenario(scenario)

        scenario = copy.deepcopy(self.scenario)
        scenario['fields'][1]['encryption_key'] = scenario['fields'][0]['encryption_key']
        with self.assertRaisesRegex(ValueError, 'must not share encryption keys'):
            validate_scenario(scenario)

if __name__ == '__main__':
    unittest.main()

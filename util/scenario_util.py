import json
from functools import lru_cache
from pathlib import Path
from urllib.request import urlopen

import validators
from jsonschema import Draft202012Validator
from pyspark.sql import DataFrame

from conf import settings
from util.data_type_util import parse_data_type
from util.constants import DATE_IMPORTED_FIELD_NAME, DAY_PARTITION_FIELD_NAME, FIELD_NAME, FIELD_IS_ENCRYPTED, \
    FIELD_ENCRYPTION_KEY, FIELD_TYPE, FIELDS, FIELD_MAP_AS, SOFT_DELETED_FIELD_NAME, YEAR_IMPORTED_FIELD_NAME, \
    SCENARIO_IMPORT_SAVE_LOCATION, SCENARIO_DELIMITER, \
    SCENARIO_ENTITY_NAME, SCENARIO_ID_FIELD, SCENARIO_TEMP_SAVE_LOCATION, SCENARIO_IMPORT_MODE, \
    SCENARIO_HARD_DELETE_CONDITION_IN_YEARS, SCENARIO_ENFORCE_DATA_MODEL, SCENARIO_IS_APPLY_YEAR_TO_SAVE_LOCATION, \
    SCENARIO_REPORT_SAVE_LOCATION, SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT


SCENARIO_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / 'scenario'
    / 'schema'
    / 'kebab_storm_scenario.schema.json'
)


class ScenarioSchemaValidationError(ValueError):
    """Raised when a scenario does not satisfy its JSON Schema contract."""


def get_missing_fields(scenario_json_path, target: DataFrame):
    scenario = load_scenario(scenario_json_path)
    target_fields = target.schema.names
    scenario_fields = find_fields(scenario)

    return [field for field in scenario_fields if field not in set(target_fields)]


def get_unexpected_fields(scenario_json_path, target: DataFrame):
    scenario = load_scenario(scenario_json_path)
    scenario_fields = set(find_fields(scenario))
    return [field for field in target.schema.names if field not in scenario_fields]


def get_scenario_defaults(scenario_json_path):
    scenario = load_scenario(scenario_json_path)

    name = get_entity_name(scenario)
    import_save_location = get_import_save_location(scenario)
    temp_save_location = get_temp_save_location(scenario)
    import_mode = get_import_mode(scenario)
    id_field_name = get_id_field_name(scenario)
    delimiter = get_delimiter(scenario)
    hard_delete_in_years = get_hard_delete_condition_in_years(scenario)
    enforce_data_model = get_enforce_data_model(scenario)
    is_apply_year_to_save_location = get_is_apply_year_to_save_location(scenario)
    report_save_location = get_report_save_location(scenario)
    delete_input_file_after_import = get_delete_input_file_after_import(scenario)

    return name, \
           import_save_location, \
           temp_save_location, \
           import_mode, \
           id_field_name, \
           delimiter, \
           hard_delete_in_years, \
           enforce_data_model, \
           is_apply_year_to_save_location, \
           report_save_location, \
           delete_input_file_after_import


def load_scenario(path):
    if validators.url(path):
        scenario = json.loads(urlopen(path).read())
    else:
        with open(path, 'r') as active_scenario:
            scenario = json.load(active_scenario)

    validate_scenario(scenario)
    return scenario


@lru_cache(maxsize=1)
def _get_scenario_schema_validator():
    with SCENARIO_SCHEMA_PATH.open(encoding='utf-8') as schema_file:
        schema = json.load(schema_file)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_scenario_structure(scenario):
    errors = sorted(
        _get_scenario_schema_validator().iter_errors(scenario),
        key=lambda error: (tuple(str(part) for part in error.absolute_path), error.message),
    )
    if errors:
        error = errors[0]
        path = _format_json_path(error.absolute_path)
        raise ScenarioSchemaValidationError(
            f'Scenario JSON Schema validation failed at {path}: {error.message}'
        )


def _format_json_path(path_parts):
    path = '$'
    for part in path_parts:
        path += f'[{part}]' if isinstance(part, int) else f'.{part}'
    return path


def validate_scenario(scenario):
    validate_scenario_structure(scenario)

    required_scenario_keys = (
        SCENARIO_DELIMITER, SCENARIO_ENFORCE_DATA_MODEL, SCENARIO_ENTITY_NAME,
        SCENARIO_HARD_DELETE_CONDITION_IN_YEARS, SCENARIO_ID_FIELD, SCENARIO_IMPORT_MODE,
        SCENARIO_IMPORT_SAVE_LOCATION, SCENARIO_IS_APPLY_YEAR_TO_SAVE_LOCATION,
        SCENARIO_REPORT_SAVE_LOCATION, SCENARIO_TEMP_SAVE_LOCATION,
        SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT, FIELDS,
    )
    missing_keys = [key for key in required_scenario_keys if key not in scenario]
    if missing_keys:
        raise ValueError(f'Scenario is missing required setting(s): {", ".join(missing_keys)}')

    for boolean_setting in (SCENARIO_ENFORCE_DATA_MODEL, SCENARIO_IS_APPLY_YEAR_TO_SAVE_LOCATION,
                            SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT):
        if not isinstance(scenario[boolean_setting], bool):
            raise ValueError(f'Scenario setting {boolean_setting} must be a boolean')

    hard_delete_years = scenario[SCENARIO_HARD_DELETE_CONDITION_IN_YEARS]
    if not isinstance(hard_delete_years, int) or hard_delete_years < 0:
        raise ValueError(f'{SCENARIO_HARD_DELETE_CONDITION_IN_YEARS} must be a non-negative integer')

    if scenario[SCENARIO_IMPORT_MODE] not in ('append', 'overwrite', 'ignore', 'errorifexists'):
        raise ValueError(f'Unsupported import mode: {scenario[SCENARIO_IMPORT_MODE]}')
    if scenario[SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT] and scenario[SCENARIO_IMPORT_MODE] == 'ignore':
        raise ValueError(
            f'{SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT} cannot be true when import_mode is ignore because '
            'an existing destination may cause the input file to be skipped rather than imported'
        )

    fields = scenario.get(FIELDS)
    if not isinstance(fields, list) or not fields:
        raise ValueError('Scenario fields must be a non-empty list')

    source_names = []
    target_names = []
    encrypted_keys = []
    for index, field in enumerate(fields):
        for required_key in (FIELD_NAME, FIELD_MAP_AS, FIELD_TYPE, FIELD_IS_ENCRYPTED):
            if required_key not in field:
                raise ValueError(f'Field at index {index} is missing {required_key}')

        source_name = str(field[FIELD_NAME]).strip()
        target_name = str(field[FIELD_MAP_AS]).strip()
        if not source_name or not target_name:
            raise ValueError(f'Field at index {index} has a blank source or target name')
        if not isinstance(field[FIELD_IS_ENCRYPTED], bool):
            raise ValueError(f'Field {source_name} must define is_encrypted as a boolean')

        try:
            parse_data_type(field[FIELD_TYPE])
        except ValueError as error:
            raise ValueError(f'Invalid type for field {source_name}: {error}') from error

        if field[FIELD_IS_ENCRYPTED]:
            encryption_key = str(field.get(FIELD_ENCRYPTION_KEY, '')).strip()
            if not encryption_key:
                raise ValueError(f'Encrypted field {source_name} requires an encryption key')
            try:
                key_bytes = bytes.fromhex(encryption_key)
            except ValueError as error:
                raise ValueError(
                    f'Encryption key for {source_name} must contain exactly 64 hexadecimal characters') from error
            if len(encryption_key) != 64 or len(key_bytes) != 32:
                raise ValueError(f'Encryption key for {source_name} must contain exactly 64 hexadecimal characters')
            encrypted_keys.append(encryption_key)

        source_names.append(source_name)
        target_names.append(target_name)

    duplicate_sources = _duplicates(source_names)
    duplicate_targets = _duplicates(target_names)
    if duplicate_sources:
        raise ValueError(f'Duplicate source field(s): {", ".join(duplicate_sources)}')
    if duplicate_targets:
        raise ValueError(f'Duplicate mapped field(s): {", ".join(duplicate_targets)}')
    if _duplicates(encrypted_keys):
        raise ValueError('Encrypted fields must not share encryption keys')

    reserved_targets = {
        DATE_IMPORTED_FIELD_NAME, DAY_PARTITION_FIELD_NAME, SOFT_DELETED_FIELD_NAME, YEAR_IMPORTED_FIELD_NAME
    }
    conflicting_targets = sorted(set(target_names).intersection(reserved_targets))
    if conflicting_targets:
        raise ValueError(f'Mapped field(s) use reserved names: {", ".join(conflicting_targets)}')

    id_field = scenario.get(SCENARIO_ID_FIELD)
    if id_field not in source_names:
        raise ValueError(f'Scenario id field is not defined in fields: {id_field}')


def _duplicates(values):
    return sorted({value for value in values if values.count(value) > 1})


def get_enforce_data_model(scenario_json):
    return bool(scenario_json[SCENARIO_ENFORCE_DATA_MODEL])


def get_hard_delete_condition_in_years(scenario_json):
    return scenario_json[SCENARIO_HARD_DELETE_CONDITION_IN_YEARS]


def get_delimiter(scenario_json):
    return scenario_json[SCENARIO_DELIMITER]


def get_import_save_location(scenario_json):
    return f'{settings.default_data_location}{str(scenario_json[SCENARIO_IMPORT_SAVE_LOCATION])}'


def get_report_save_location(scenario_json):
    return f'{settings.default_data_location}{str(scenario_json[SCENARIO_REPORT_SAVE_LOCATION])}'


def get_is_apply_year_to_save_location(scenario_json):
    return bool(scenario_json[SCENARIO_IS_APPLY_YEAR_TO_SAVE_LOCATION])


def get_import_mode(scenario_json):
    return scenario_json[SCENARIO_IMPORT_MODE]


def get_delete_input_file_after_import(scenario_json):
    return bool(scenario_json[SCENARIO_DELETE_INPUT_FILE_AFTER_IMPORT])


def get_entity_name(scenario_json):
    return scenario_json[SCENARIO_ENTITY_NAME]


def get_id_field_name(scenario_json):
    return scenario_json[SCENARIO_ID_FIELD]


def get_temp_save_location(scenario_json):
    return scenario_json[SCENARIO_TEMP_SAVE_LOCATION]


def find_field(scenario_json, field_name):
    return [field for field in scenario_json if field[FIELD_NAME] == field_name][0]


def find_field_map_as(scenario_json, field_name_map_as):
    return [field for field in scenario_json if field[FIELD_MAP_AS] == field_name_map_as][0]


def find_field_is_encrypted(scenario_json, field_name):
    return bool(find_field(scenario_json[FIELDS], field_name)[FIELD_IS_ENCRYPTED])


def find_encryption_key(scenario_json, field_name):
    return find_field_map_as(scenario_json[FIELDS], field_name)[FIELD_ENCRYPTION_KEY]


def find_mapped_as_value(scenario_json, field_name):
    return find_field(scenario_json[FIELDS], field_name)[FIELD_MAP_AS]


def find_field_type_map_as(scenario_json, field_name):
    return find_field_map_as(scenario_json[FIELDS], field_name)[FIELD_TYPE]


def find_encrypted_fields_map_as(scenario_json):
    return [field[FIELD_MAP_AS] for field in scenario_json[FIELDS] if bool(field[FIELD_IS_ENCRYPTED])]


def find_fields(scenario_json):
    return [field[FIELD_NAME] for field in scenario_json[FIELDS]]

import json

from pyspark.sql import DataFrame

from conf import settings
from util.constants import FIELD_NAME, FIELD_IS_ENCRYPTED, FIELD_ENCRYPTION_KEY, FIELD_TYPE, FIELDS, FIELD_MAP_AS, \
    SCENARIO_IMPORT_SAVE_LOCATION, SCENARIO_IMPORT_SAVE_TYPE, SCENARIO_DELIMITER, \
    SCENARIO_ENTITY_NAME, SCENARIO_ID_FIELD, SCENARIO_TEMP_SAVE_LOCATION, SCENARIO_IMPORT_MODE, \
    SCENARIO_HARD_DELETE_CONDITION_IN_YEARS, SCENARIO_ENFORCE_DATA_MODEL, SCENARIO_IS_APPLY_YEAR_TO_SAVE_LOCATION, \
    SCENARIO_REPORT_SAVE_TYPE, SCENARIO_REPORT_SAVE_LOCATION


def get_missing_fields(scenario_json_path, target: DataFrame):
    scenario = load_scenario(scenario_json_path)
    target_fields = target.schema.names
    scenario_fields = find_fields(scenario)

    result = [x for x in target_fields if x not in set(scenario_fields)]

    del scenario
    return result


def get_scenario_defaults(scenario_json_path):
    scenario = load_scenario(scenario_json_path)

    name = get_entity_name(scenario)
    import_save_location = get_import_save_location(scenario)
    temp_save_location = get_temp_save_location(scenario)
    import_save_type = get_import_save_type(scenario)
    import_mode = get_import_mode(scenario)
    id_field_name = get_id_field_name(scenario)
    delimiter = get_delimiter(scenario)
    hard_delete_in_years = get_hard_delete_condition_in_years(scenario)
    enforce_data_model = get_enforce_data_model(scenario)
    is_apply_year_to_save_location = get_is_apply_year_to_save_location(scenario)
    report_save_location = get_report_save_location(scenario)
    report_save_type = get_report_save_type(scenario)

    del scenario
    return name, \
           import_save_location, \
           temp_save_location, \
           import_save_type, \
           import_mode, \
           id_field_name, \
           delimiter, \
           hard_delete_in_years, \
           enforce_data_model, \
           is_apply_year_to_save_location, \
           report_save_location, \
           report_save_type


def load_scenario(path):
    with open(path, 'r') as active_scenario:
        return json.load(active_scenario)


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


def get_import_save_type(scenario_json):
    return scenario_json[SCENARIO_IMPORT_SAVE_TYPE]


def get_report_save_type(scenario_json):
    return scenario_json[SCENARIO_REPORT_SAVE_TYPE]


def get_import_mode(scenario_json):
    return scenario_json[SCENARIO_IMPORT_MODE]


def get_entity_name(scenario_json):
    return scenario_json[SCENARIO_ENTITY_NAME]


def get_id_field_name(scenario_json):
    return scenario_json[SCENARIO_ID_FIELD]


def get_temp_save_location(scenario_json):
    return scenario_json[SCENARIO_TEMP_SAVE_LOCATION]


def find_field(scenario_json, field_name):
    result = [field for field in scenario_json if field[FIELD_NAME] == field_name][0]
    del scenario_json
    return result


def find_field_map_as(scenario_json, field_name_map_as):
    result = [field for field in scenario_json if field[FIELD_MAP_AS] == field_name_map_as][0]
    del scenario_json
    return result


def find_timestamp_fields(scenario_json):
    result = [field[FIELD_NAME] for field in scenario_json[FIELDS] if str(field[FIELD_TYPE]).startswith('timestamp')]
    del scenario_json
    return result


def find_field_is_encrypted(scenario_json, field_name):
    result = bool(find_field(scenario_json[FIELDS], field_name)[FIELD_IS_ENCRYPTED])
    del scenario_json
    return result


def find_encryption_key(scenario_json, field_name):
    result = find_field_map_as(scenario_json[FIELDS], field_name)[FIELD_ENCRYPTION_KEY]
    del scenario_json
    return result


def find_mapped_as_value(scenario_json, field_name):
    result = find_field(scenario_json[FIELDS], field_name)[FIELD_MAP_AS]
    del scenario_json
    return result


def find_field_type(scenario_json, field_name):
    result = find_field(scenario_json[FIELDS], field_name)[FIELD_TYPE]
    del scenario_json
    return result


def find_field_type_map_as(scenario_json, field_name):
    result = find_field_map_as(scenario_json[FIELDS], field_name)[FIELD_TYPE]
    del scenario_json
    return result


def find_encrypted_fields(scenario_json):
    result = [field[FIELD_NAME] for field in scenario_json[FIELDS] if bool(field[FIELD_IS_ENCRYPTED])]
    del scenario_json
    return result


def find_encrypted_fields_map_as(scenario_json):
    result = [field[FIELD_MAP_AS] for field in scenario_json[FIELDS] if bool(field[FIELD_IS_ENCRYPTED])]
    del scenario_json
    return result


def find_fields(scenario_json):
    result = [field[FIELD_NAME] for field in scenario_json[FIELDS]]
    del scenario_json
    return result


def find_fields_map_as(scenario_json):
    result = [field[FIELD_MAP_AS] for field in scenario_json[FIELDS]]
    del scenario_json
    return result

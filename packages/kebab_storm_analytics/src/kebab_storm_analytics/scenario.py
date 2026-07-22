import json
from functools import lru_cache
from importlib.resources import files

from jsonschema import Draft202012Validator

from kebab_storm_analytics.types import target_type_name

_RESERVED_FIELDS = {'DATE_IMPORTED', 'SOFT_DELETED', 'YEAR_IMPORTED', 'day'}


class ScenarioValidationError(ValueError):
    """Raised when a Vault scenario is not safe to use as an analytics contract."""


def validate_scenario(scenario):
    if not isinstance(scenario, dict):
        raise ScenarioValidationError('Scenario must be a JSON object')

    validator = _schema_validator()
    errors = sorted(
        validator.iter_errors(scenario),
        key=lambda error: (tuple(str(part) for part in error.absolute_path), error.message),
    )
    if errors:
        error = errors[0]
        raise ScenarioValidationError(
            f'Scenario JSON Schema validation failed at {_format_path(error.absolute_path)}: {error.message}'
        )

    source_names = []
    mapped_names = []
    encrypted_keys = []
    for field in scenario['fields']:
        source_name = field['name'].strip()
        mapped_name = field['map_as'].strip()
        try:
            target_type_name(field['type'])
        except ValueError as error:
            raise ScenarioValidationError(f'Invalid type for field {source_name}: {error}') from error

        if field['is_encrypted']:
            key = field['encryption_key'].lower()
            if len(bytes.fromhex(key)) != 32:
                raise ScenarioValidationError(f'Encryption key for {source_name} must represent exactly 32 bytes')
            encrypted_keys.append(key)
        source_names.append(source_name)
        mapped_names.append(mapped_name)

    _reject_duplicates(source_names, 'source')
    _reject_duplicates(mapped_names, 'mapped')
    _reject_duplicates(encrypted_keys, 'encryption key')

    reserved = sorted(set(mapped_names).intersection(_RESERVED_FIELDS))
    if reserved:
        raise ScenarioValidationError(f'Mapped fields use reserved names: {", ".join(reserved)}')
    if scenario['id_field'] not in source_names:
        raise ScenarioValidationError(f'Scenario id field is not defined in fields: {scenario["id_field"]}')
    return scenario


def field_by_mapped_name(scenario, mapped_name):
    for field in scenario['fields']:
        if field['map_as'] == mapped_name:
            return field
    raise ScenarioValidationError(f'Unknown mapped field: {mapped_name}')


def encrypted_mapped_fields(scenario):
    return [field['map_as'] for field in scenario['fields'] if field['is_encrypted']]


@lru_cache(maxsize=1)
def _schema_validator():
    schema_path = files('kebab_storm_analytics').joinpath('schema/kebab_storm_scenario.schema.json')
    schema = json.loads(schema_path.read_text(encoding='utf-8'))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _reject_duplicates(values, description):
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        raise ScenarioValidationError(f'Duplicate {description} value(s): {", ".join(duplicates)}')


def _format_path(path_parts):
    path = '$'
    for part in path_parts:
        path += f'[{part}]' if isinstance(part, int) else f'.{part}'
    return path

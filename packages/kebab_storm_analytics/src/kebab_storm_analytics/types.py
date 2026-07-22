import re
from dataclasses import dataclass, field
from typing import Dict, Optional

from pyspark.sql.types import BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, \
    LongType, ShortType, StringType, TimestampType

_DECIMAL_PATTERN = re.compile(r'^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$', re.IGNORECASE)
_ALIASES = {
    'bigint': 'long',
    'bool': 'boolean',
    'int': 'integer',
    'smallint': 'short',
    'tinyint': 'byte',
}
_SUPPORTED_TYPES = {
    'boolean', 'byte', 'date', 'decimal', 'double', 'float', 'integer', 'long', 'short', 'string', 'timestamp'
}
_NUMERIC_TYPES = {'byte', 'short', 'integer', 'long', 'float', 'double', 'decimal'}
_TEMPORAL_TYPES = {'date', 'timestamp'}
_SUPPORTED_OPTIONS = {
    'epoch_unit', 'false_value', 'false_values', 'source_format', 'target_format', 'true_value', 'true_values'
}
_EPOCH_UNITS = {'seconds', 'milliseconds', 'microseconds'}


@dataclass(frozen=True)
class AtomicTypeSpec:
    name: str
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass(frozen=True)
class DataTypeSpec:
    source: AtomicTypeSpec
    target: AtomicTypeSpec
    options: Dict[str, str] = field(default_factory=dict)
    shorthand: bool = False


def target_type_name(type_definition: str) -> str:
    target = parse_data_type(type_definition).target
    return f'decimal({target.precision},{target.scale})' if target.name == 'decimal' else target.name


def to_spark_type(type_definition: str):
    target = parse_data_type(type_definition).target
    if target.name == 'decimal':
        return DecimalType(target.precision, target.scale)
    return {
        'boolean': BooleanType,
        'byte': ByteType,
        'date': DateType,
        'double': DoubleType,
        'float': FloatType,
        'integer': IntegerType,
        'long': LongType,
        'short': ShortType,
        'string': StringType,
        'timestamp': TimestampType,
    }[target.name]()


def parse_data_type(type_definition: str) -> DataTypeSpec:
    parts = [part.strip() for part in str(type_definition).split('|')]
    if not parts or not parts[0]:
        raise ValueError('A data type definition cannot be blank')

    first_type = _parse_atomic_type(parts[0])
    if len(parts) == 1:
        spec = DataTypeSpec(AtomicTypeSpec('string'), first_type, shorthand=True)
        _validate_conversion(spec)
        return spec

    if first_type.name in _TEMPORAL_TYPES and not _is_atomic_type(parts[1]) and '=' not in parts[1]:
        options = {'source_format': parts[1]}
        if len(parts) > 2 and parts[2]:
            options['target_format'] = parts[2]
        if len(parts) > 3:
            raise ValueError(f'Too many shorthand temporal parameters: {type_definition}')
        spec = DataTypeSpec(AtomicTypeSpec('string'), first_type, options, shorthand=True)
        _validate_conversion(spec)
        return spec

    if len(parts) < 2:
        raise ValueError(f'Missing target data type: {type_definition}')
    target_type = _parse_atomic_type(parts[1])
    spec = DataTypeSpec(first_type, target_type, _parse_options(parts[2:]))
    _validate_conversion(spec)
    return spec


def _is_atomic_type(value: str) -> bool:
    try:
        _parse_atomic_type(value)
        return True
    except ValueError:
        return False


def _parse_atomic_type(value: str) -> AtomicTypeSpec:
    definition = str(value).strip().lower()
    decimal_match = _DECIMAL_PATTERN.fullmatch(definition)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        if not 1 <= precision <= 38:
            raise ValueError('Decimal precision must be between 1 and 38')
        if not 0 <= scale <= precision:
            raise ValueError('Decimal scale must be between 0 and precision')
        return AtomicTypeSpec('decimal', precision, scale)
    name = _ALIASES.get(definition, definition)
    if name not in _SUPPORTED_TYPES:
        raise ValueError(f'Unsupported data type: {value}')
    return AtomicTypeSpec(name)


def _parse_options(parts):
    options = {}
    for part in parts:
        if not part or '=' not in part:
            raise ValueError(f'Conversion options must use key=value syntax: {part}')
        key, value = (item.strip() for item in part.split('=', 1))
        key = key.lower()
        if not key or not value:
            raise ValueError(f'Conversion options require a non-blank key and value: {part}')
        if key in options:
            raise ValueError(f'Duplicate conversion option: {key}')
        options[key] = value
    return options


def _validate_conversion(spec: DataTypeSpec):
    unsupported = set(spec.options).difference(_SUPPORTED_OPTIONS)
    if unsupported:
        raise ValueError(f'Unsupported conversion option(s): {", ".join(sorted(unsupported))}')

    epoch_unit = spec.options.get('epoch_unit')
    if epoch_unit and epoch_unit not in _EPOCH_UNITS:
        raise ValueError(f'Unsupported epoch unit: {epoch_unit}')

    source_name = spec.source.name
    target_name = spec.target.name
    if source_name in _TEMPORAL_TYPES and target_name == 'boolean':
        raise ValueError(f'{source_name} cannot be converted to boolean')
    if source_name == 'boolean' and target_name in _TEMPORAL_TYPES:
        raise ValueError(f'boolean cannot be converted to {target_name}')

    temporal_to_numeric = source_name in _TEMPORAL_TYPES and target_name in _NUMERIC_TYPES
    numeric_to_temporal = source_name in _NUMERIC_TYPES and target_name in _TEMPORAL_TYPES
    if temporal_to_numeric or numeric_to_temporal:
        if not epoch_unit:
            raise ValueError('Numeric/timestamp conversions require epoch_unit')
    elif epoch_unit:
        raise ValueError('epoch_unit requires a numeric/timestamp conversion')

    source_format = spec.options.get('source_format')
    if source_format and source_name not in _TEMPORAL_TYPES and not (
            source_name == 'string' and target_name in _TEMPORAL_TYPES):
        raise ValueError('source_format requires a temporal source or target')

    target_format = spec.options.get('target_format')
    if target_format and not spec.shorthand and not (
            target_name == 'string' and source_name in _TEMPORAL_TYPES.union(_NUMERIC_TYPES)):
        raise ValueError('target_format requires a temporal/numeric source and string target')

    if {'false_values', 'true_values'}.intersection(spec.options) and not (
            source_name == 'boolean' or (source_name == 'string' and target_name == 'boolean')):
        raise ValueError('true_values/false_values require a boolean source or target')

    if {'false_value', 'true_value'}.intersection(spec.options) and not (
            source_name == 'boolean' and target_name == 'string'):
        raise ValueError('true_value/false_value require boolean-to-string conversion')

import re
from dataclasses import dataclass, field
from typing import Dict, Optional


_DECIMAL_PATTERN = re.compile(r'^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$', re.IGNORECASE)

TYPE_ALIASES = {
    'bool': 'boolean',
    'bigint': 'long',
    'int': 'integer',
    'smallint': 'short',
    'tinyint': 'byte',
}

SUPPORTED_TYPES = {
    'boolean', 'byte', 'date', 'decimal', 'double', 'float', 'integer', 'long', 'short', 'string', 'timestamp'
}
NUMERIC_TYPES = {'byte', 'short', 'integer', 'long', 'float', 'double', 'decimal'}
TEMPORAL_TYPES = {'date', 'timestamp'}

SUPPORTED_OPTIONS = {
    'epoch_unit', 'false_value', 'false_values', 'source_format', 'target_format', 'true_value', 'true_values'
}
EPOCH_UNITS = {'seconds', 'milliseconds', 'microseconds'}

PYTHON_TO_SPARK_DATETIME_PATTERNS = {
    '%Y': 'yyyy',
    '%y': 'yy',
    '%m': 'MM',
    '%d': 'dd',
    '%H': 'HH',
    '%I': 'hh',
    '%M': 'mm',
    '%S': 'ss',
    '%f': 'SSSSSS',
    '%p': 'a',
    '%z': 'XXX',
}


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

    # Compatibility properties for callers that previously treated the parsed
    # definition as a single target type.
    @property
    def name(self):
        return self.target.name

    @property
    def precision(self):
        return self.target.precision

    @property
    def scale(self):
        return self.target.scale

    @property
    def source_format(self):
        return self.options.get('source_format')

    @property
    def destination_format(self):
        return self.options.get('target_format')


def parse_atomic_type(type_definition: str) -> AtomicTypeSpec:
    definition = str(type_definition).strip().lower()
    decimal_match = _DECIMAL_PATTERN.fullmatch(definition)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        if precision < 1 or precision > 38:
            raise ValueError('Decimal precision must be between 1 and 38')
        if scale < 0 or scale > precision:
            raise ValueError('Decimal scale must be between 0 and precision')
        return AtomicTypeSpec('decimal', precision=precision, scale=scale)

    name = TYPE_ALIASES.get(definition, definition)
    if name not in SUPPORTED_TYPES:
        raise ValueError(f'Unsupported data type: {type_definition}')
    return AtomicTypeSpec(name)


def parse_data_type(type_definition: str) -> DataTypeSpec:
    """Parse concise shorthand types and explicit source|target|key=value conversions."""
    parts = [part.strip() for part in str(type_definition).split('|')]
    if not parts or not parts[0]:
        raise ValueError('A data type definition cannot be blank')

    first_type = parse_atomic_type(parts[0])
    if len(parts) == 1:
        spec = DataTypeSpec(AtomicTypeSpec('string'), first_type, shorthand=True)
        validate_conversion(spec)
        return spec

    # Flexible temporal shorthand: timestamp|%m/%d/%Y|%Y-%m-%d and its date equivalent.
    if first_type.name in TEMPORAL_TYPES and not _is_type(parts[1]) and '=' not in parts[1]:
        options = {'source_format': parts[1]}
        if len(parts) > 2 and parts[2]:
            options['target_format'] = parts[2]
        if len(parts) > 3:
            raise ValueError(f'Too many shorthand temporal parameters: {type_definition}')
        spec = DataTypeSpec(AtomicTypeSpec('string'), first_type, options, shorthand=True)
        validate_conversion(spec)
        return spec

    target_type = parse_atomic_type(parts[1])
    options = _parse_options(parts[2:])
    spec = DataTypeSpec(first_type, target_type, options)
    validate_conversion(spec)
    return spec


def validate_conversion(spec: DataTypeSpec):
    unsupported_options = set(spec.options) - SUPPORTED_OPTIONS
    if unsupported_options:
        raise ValueError(f'Unsupported conversion option(s): {", ".join(sorted(unsupported_options))}')

    epoch_unit = spec.options.get('epoch_unit')
    if epoch_unit and epoch_unit not in EPOCH_UNITS:
        raise ValueError(f'Unsupported epoch unit: {epoch_unit}')

    source_name = spec.source.name
    target_name = spec.target.name
    if source_name in TEMPORAL_TYPES and target_name == 'boolean':
        raise ValueError(f'{source_name} cannot be converted to boolean')
    if source_name == 'boolean' and target_name in TEMPORAL_TYPES:
        raise ValueError(f'boolean cannot be converted to {target_name}')

    temporal_to_numeric = source_name in TEMPORAL_TYPES and target_name in NUMERIC_TYPES
    numeric_to_temporal = source_name in NUMERIC_TYPES and target_name in TEMPORAL_TYPES
    if temporal_to_numeric or numeric_to_temporal:
        if not epoch_unit:
            raise ValueError('Numeric/timestamp conversions require epoch_unit')
    elif epoch_unit:
        raise ValueError('epoch_unit requires a numeric/timestamp conversion')

    source_format = spec.options.get('source_format')
    if source_format and source_name not in TEMPORAL_TYPES and not (
            source_name == 'string' and target_name in TEMPORAL_TYPES):
        raise ValueError('source_format requires a temporal source or target')

    target_format = spec.options.get('target_format')
    if target_format and not spec.shorthand and not (
            target_name == 'string' and source_name in TEMPORAL_TYPES.union(NUMERIC_TYPES)):
        raise ValueError('target_format requires a temporal/numeric source and string target')

    input_boolean_options = {'false_values', 'true_values'}
    if set(spec.options).intersection(input_boolean_options) and not (
            source_name == 'boolean' or (source_name == 'string' and target_name == 'boolean')):
        raise ValueError('true_values/false_values require a boolean source or target')

    output_boolean_options = {'false_value', 'true_value'}
    if set(spec.options).intersection(output_boolean_options) and not (
            source_name == 'boolean' and target_name == 'string'):
        raise ValueError('true_value/false_value require boolean-to-string conversion')


def to_spark_datetime_format(python_format: Optional[str], flexible_width: bool = False) -> Optional[str]:
    """Translate the Python strptime tokens used by scenarios to Spark patterns."""
    if not python_format:
        return None

    result = python_format
    for python_token, spark_token in PYTHON_TO_SPARK_DATETIME_PATTERNS.items():
        if flexible_width and python_token in ('%m', '%d', '%H', '%I', '%M', '%S'):
            spark_token = spark_token[0]
        result = result.replace(python_token, spark_token)
    return result


def option_values(spec: DataTypeSpec, option_name: str, defaults):
    value = spec.options.get(option_name)
    return [item.strip().lower() for item in value.split(',')] if value else list(defaults)


def _is_type(value: str) -> bool:
    try:
        parse_atomic_type(value)
        return True
    except ValueError:
        return False


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

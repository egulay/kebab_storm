import os
from collections.abc import Mapping
from pathlib import Path

import confuse
import yaml

_THIS_FOLDER = Path(__file__).resolve().parent
_SUPPORTED_ENVIRONMENTS = frozenset({'local', 'dev', 'prod'})
_ENVIRONMENT_VARIABLE = 'KEBAB_STORM_ENV'
_CONFIG_OVERRIDES = {
    'KEBAB_STORM_SPARK_MASTER': 'spark_master',
    'KEBAB_STORM_LOGGING_LOCATION': 'kebab_storm_logging_location',
    'KEBAB_STORM_DATA_LOCATION': 'default_data_location',
    'KEBAB_STORM_STORAGE_PROVIDER': 'storage_provider',
}
_SUPPORTED_STORAGE_PROVIDERS = frozenset({'parquet', 'iceberg'})


class Settings:
    def __init__(self, environment=None):
        self.active_profile = self._select_environment(environment)
        default_config = self._read_config(_THIS_FOLDER / 'default.yml')
        profile_config = self._read_config(_THIS_FOLDER / self.active_profile / 'env.yml')
        self.active_config = confuse.Configuration('KebabStorm', __name__, read=False)
        self.active_config.set(self._merge(default_config, profile_config))

        environment_overrides = {
            config_key: os.environ[environment_key]
            for environment_key, config_key in _CONFIG_OVERRIDES.items()
            if os.environ.get(environment_key)
        }
        if environment_overrides:
            self.active_config.set(environment_overrides)

        self.spark_config = [[key, value] for key, value in self.active_config['spark_config'].get().items()]
        self.spark_master = self.active_config['spark_master'].get(str)
        self.enable_hive_support = self.active_config['enable_hive_support'].get(bool)
        self.logging_location = self.active_config['kebab_storm_logging_location'].get(str)
        self.default_data_location = self.active_config['default_data_location'].get(str)
        self.storage_provider = self.active_config['storage_provider'].get(str).strip().lower()
        if self.storage_provider not in _SUPPORTED_STORAGE_PROVIDERS:
            supported = ', '.join(sorted(_SUPPORTED_STORAGE_PROVIDERS))
            raise ValueError(f'Unsupported storage_provider value: {self.storage_provider!r}. '
                             f'Expected one of: {supported}')
        self.iceberg_catalog = self.active_config['iceberg_catalog'].get(str).strip()
        self.iceberg_namespace = self.active_config['iceberg_namespace'].get(str).strip()
        if self.storage_provider == 'iceberg' and not (self.iceberg_catalog and self.iceberg_namespace):
            raise ValueError('iceberg_catalog and iceberg_namespace are required for Iceberg storage')
        self.banner = (_THIS_FOLDER / 'banner.txt').read_text(encoding='utf-8')

    @staticmethod
    def _select_environment(environment):
        active_profile = str(environment or os.getenv(_ENVIRONMENT_VARIABLE, 'local')).strip().lower()
        if active_profile not in _SUPPORTED_ENVIRONMENTS:
            supported = ', '.join(sorted(_SUPPORTED_ENVIRONMENTS))
            raise ValueError(
                f'Unsupported {_ENVIRONMENT_VARIABLE} value: {active_profile!r}. '
                f'Expected one of: {supported}'
            )
        return active_profile

    @staticmethod
    def _read_config(path):
        with path.open(encoding='utf-8') as config_file:
            config = yaml.safe_load(config_file)
        if not isinstance(config, Mapping):
            raise ValueError(f'Configuration file must contain a YAML mapping: {path}')
        return config

    @classmethod
    def _merge(cls, base, override):
        merged = dict(base)
        for key, value in override.items():
            if isinstance(merged.get(key), Mapping) and isinstance(value, Mapping):
                merged[key] = cls._merge(merged[key], value)
            else:
                merged[key] = value
        return merged

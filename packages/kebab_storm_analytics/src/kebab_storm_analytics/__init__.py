"""Read-only analytics access to Kebab Storm encrypted datasets."""

from kebab_storm_analytics.dataset import read_dataset
from kebab_storm_analytics.scenario import ScenarioValidationError
from kebab_storm_analytics.vault import VaultScenarioError

__all__ = ['read_dataset', 'ScenarioValidationError', 'VaultScenarioError']
__version__ = '0.1.0'

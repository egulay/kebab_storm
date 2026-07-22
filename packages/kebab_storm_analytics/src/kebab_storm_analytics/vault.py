import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from kebab_storm_analytics.scenario import validate_scenario


class VaultScenarioError(RuntimeError):
    """Raised when a scenario cannot be retrieved from Vault."""


def load_scenario_from_vault(
        secret_path=None, vault_addr=None, vault_token=None, vault_namespace=None,
        allow_insecure_http=None, timeout_seconds=10):
    address = str(vault_addr or os.getenv('VAULT_ADDR', '')).strip().rstrip('/')
    token = str(vault_token or os.getenv('VAULT_TOKEN', '')).strip()
    path = str(secret_path or os.getenv('KEBAB_STORM_SCENARIO_SECRET', '')).strip()
    namespace = str(vault_namespace or os.getenv('VAULT_NAMESPACE', '')).strip()

    if not address:
        raise VaultScenarioError('VAULT_ADDR is required')
    if not token:
        raise VaultScenarioError('VAULT_TOKEN is required')
    if not path:
        raise VaultScenarioError('A Vault scenario secret path is required')

    parsed_address = urlparse(address)
    if parsed_address.scheme not in {'http', 'https'} or not parsed_address.netloc:
        raise VaultScenarioError('VAULT_ADDR must be an absolute HTTP or HTTPS URL')
    insecure_allowed = _environment_boolean('KEBAB_STORM_ALLOW_INSECURE_VAULT') \
        if allow_insecure_http is None else bool(allow_insecure_http)
    if parsed_address.scheme != 'https' and not insecure_allowed:
        raise VaultScenarioError(
            'Vault must use HTTPS; set KEBAB_STORM_ALLOW_INSECURE_VAULT=true only for the local Docker demo'
        )

    api_path = _kv_v2_api_path(path)
    url = f'{address}/v1/{"/".join(quote(part, safe="") for part in api_path.split("/"))}'
    headers = {'X-Vault-Token': token, 'Accept': 'application/json'}
    if namespace:
        headers['X-Vault-Namespace'] = namespace

    try:
        with urlopen(Request(url, headers=headers), timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except HTTPError as error:
        raise VaultScenarioError(f'Vault returned HTTP {error.code} while reading the scenario') from error
    except (URLError, TimeoutError) as error:
        raise VaultScenarioError(
            f'Unable to reach Vault at {address}: {error}. '
            'Verify VAULT_ADDR and the notebook or container DNS/network configuration.'
        ) from error
    except json.JSONDecodeError as error:
        raise VaultScenarioError(f'Vault at {address} returned a response that is not valid JSON') from error

    try:
        scenario_value = payload['data']['data']['scenario']
    except (KeyError, TypeError) as error:
        raise VaultScenarioError('Vault KV v2 response does not contain data.data.scenario') from error

    if isinstance(scenario_value, str):
        try:
            scenario = json.loads(scenario_value)
        except json.JSONDecodeError as error:
            raise VaultScenarioError('Vault scenario value is not valid JSON') from error
    elif isinstance(scenario_value, dict):
        scenario = scenario_value
    else:
        raise VaultScenarioError('Vault scenario value must be a JSON object or JSON string')
    return validate_scenario(scenario)


def _kv_v2_api_path(secret_path: str) -> str:
    path = secret_path.strip('/')
    if path.startswith('v1/'):
        path = path[3:]
    mount, separator, relative_path = path.partition('/')
    if not mount or not separator or not relative_path:
        raise VaultScenarioError('Vault secret path must include a mount and secret name')
    if relative_path.startswith('data/'):
        return f'{mount}/{relative_path}'
    return f'{mount}/data/{relative_path}'


def _environment_boolean(name: str) -> bool:
    return os.getenv(name, '').strip().lower() in {'1', 'true', 'yes', 'y'}

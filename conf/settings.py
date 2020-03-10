import os

import yaml

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(THIS_FOLDER, 'global.yml'), 'r') as _global_config_yml:
    global_config = yaml.safe_load(_global_config_yml)
    default_environment = global_config['default_environment']
    with open(os.path.join(THIS_FOLDER, default_environment, 'env.yml'), 'r') as _active_config_yml:
        active_config = yaml.safe_load(_active_config_yml)
    with open(os.path.join(THIS_FOLDER, 'banner.txt'), 'r') as _banner:
        DRIVER_BANNER = _banner.read()

ACTIVE_PROFILE = global_config['default_environment']

SPARK_CONFIG = [[k, v] for k, v in active_config['spark_config'].items()]
SPARK_MASTER = active_config['spark_master']

KEBAB_STORM_LOGGING_LOCATION = active_config['kebab_storm_logging_location']
DEFAULT_DATA_LOCATION = active_config['default_data_location']

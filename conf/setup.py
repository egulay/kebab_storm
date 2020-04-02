import os

import confuse
import yaml

_THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))


class Settings:
    def __init__(self):
        with open(os.path.join(_THIS_FOLDER, 'global.yml'), 'r') as _global_config_yml:
            _global_config = yaml.safe_load(_global_config_yml)
            self.active_profile = _global_config['default_environment']

            self.active_config = confuse.LazyConfig('KebabStorm', __name__)
            self.active_config.set_file(os.path.join(_THIS_FOLDER, _global_config['default_environment'], 'env.yml'))

            self.spark_config = [[k, v] for k, v in self.active_config['spark_config'].get().items()]
            self.spark_master = self.active_config['spark_master'].get()
            self.logging_location = self.active_config['kebab_storm_logging_location'].get()
            self.default_data_location = self.active_config['default_data_location'].get()

            with open(os.path.join(_THIS_FOLDER, 'banner.txt'), 'r') as _banner:
                self.banner = _banner.read()

import time
from datetime import datetime

from pyspark import SparkConf

from conf import settings
from util.logger import get_logger
from util.spark_util import SparkProvider

spark_session = SparkProvider.setup_spark('Project: Kebab Storm', settings.spark_master,
                                          extra_dependencies=[], conf=SparkConf().setAll(settings.spark_config))

print(settings.banner)
time.sleep(0.005)

logger = get_logger(__name__, settings.logging_location,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log', settings.active_profile)

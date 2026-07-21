import logging
from contextlib import contextmanager
from datetime import datetime

from pyspark import SparkConf

from conf import settings
from util.logger import configure_logging
from util.spark_util import managed_spark_session


@contextmanager
def spark_application(logger_name: str, app_name: str = 'Project: Kebab Storm'):
    configure_logging(
        settings.logging_location,
        f'{datetime.today():%Y-%m-%d}.log',
        settings.active_profile,
    )
    logger = logging.getLogger(logger_name)
    print(settings.banner)
    with managed_spark_session(
            app_name,
            settings.spark_master,
            conf=SparkConf().setAll(settings.spark_config),
            enable_hive_support=settings.enable_hive_support) as spark_session:
        yield spark_session, logger

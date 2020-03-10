import os
from typing import List, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

from util.constants import STANDALONE


class SparkProvider:

    def __init__(self,
                 app_name: str,
                 conf: Optional[SparkConf] = None,
                 extra_dependencies: Optional[List[str]] = None,
                 extra_files: Optional[List[str]] = None):
        self.spark = self.set_up_spark(app_name, self.master, conf, extra_dependencies, extra_files)

    @property
    def master(self) -> str:
        return os.getenv('SPARK_MASTER', STANDALONE)

    @staticmethod
    def setup_spark(app_name: str,
                    master: str = STANDALONE,
                    conf: SparkConf = None,
                    extra_dependencies: List[str] = None,
                    extra_files: List[str] = None) -> SparkSession:
        conf = conf if conf else SparkConf()

        if extra_dependencies:
            spark_dependencies = ','.join(extra_dependencies)
            conf.set('spark.jars.packages', spark_dependencies)

        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(master) \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()

        extra_files = extra_files if extra_files else []
        for extra_file in extra_files:
            spark.sparkContext.addPyFile(extra_file)

        return spark

    @staticmethod
    def shut_down_spark(spark):
        spark.stop()

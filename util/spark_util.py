from contextlib import contextmanager
from collections.abc import Iterable

from pyspark import SparkConf
from pyspark.sql import SparkSession

from util.constants import STANDALONE


def create_spark_session(
        app_name: str,
        master: str = STANDALONE,
        conf: SparkConf | None = None,
        packages: Iterable[str] | None = None,
        python_files: Iterable[str] | None = None,
        files: Iterable[str] | None = None,
        enable_hive_support: bool = False) -> SparkSession:

    spark_conf = conf or SparkConf()
    if packages:
        spark_conf.set('spark.jars.packages', ','.join(packages))

    builder = SparkSession.builder.appName(app_name).master(master).config(conf=spark_conf)
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()

    for python_file in python_files or ():
        spark.sparkContext.addPyFile(python_file)
    for resource_file in files or ():
        spark.sparkContext.addFile(resource_file)
    return spark


@contextmanager
def managed_spark_session(*args, **kwargs):

    spark = create_spark_session(*args, **kwargs)
    try:
        yield spark
    finally:
        spark.stop()

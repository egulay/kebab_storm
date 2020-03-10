import json
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import TimestampType

from conf.settings import KEBAB_STORM_LOGGING_LOCATION
from util.constants import DAY_PARTITION_FIELD_NAME, SOFT_DELETED_FIELD_NAME, DATE_IMPORTED_FIELD_NAME
from util.logger import get_logger
from util.etl_util import generic_validate_udf, generic_cast, generic_map_as, get_day_partition_name_and_year
from util.scenario_util import load_scenario, find_fields, get_id_field_name

logger = get_logger(__name__, KEBAB_STORM_LOGGING_LOCATION,
                    f'{datetime.today().strftime("%Y-%m-%d")}.log')


def validate_and_refine(scenario_json_path, source: DataFrame, day) -> DataFrame:
    scenario = load_scenario(scenario_json_path)

    columns_to_refine = find_fields(scenario)
    logger.info(f'Refinement will be applied to following column(s): {columns_to_refine}')

    validated_df = reduce(lambda x_df, col_name: x_df.withColumn(col_name, generic_validate_udf(
        func.struct(col_name, func.lit(json.dumps(scenario)), func.lit(col_name),
                    get_id_field_name(scenario))).cast(
        generic_cast(scenario, col_name))), columns_to_refine, source)

    mapped_df = reduce(lambda x_df, col_name: x_df.withColumnRenamed(col_name, generic_map_as(scenario, col_name)),
                       columns_to_refine, validated_df)

    return mapped_df \
        .withColumn(DATE_IMPORTED_FIELD_NAME, func.lit(day)) \
        .withColumn(DAY_PARTITION_FIELD_NAME, func.lit(day)) \
        .withColumn(SOFT_DELETED_FIELD_NAME, func.lit(None).cast(TimestampType()))

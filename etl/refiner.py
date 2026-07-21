import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import TimestampType

from util.constants import DATE_IMPORTED_FIELD_NAME, DAY_PARTITION_FIELD_NAME, FIELD_MAP_AS, FIELD_NAME, FIELD_TYPE, \
    FIELDS, SOFT_DELETED_FIELD_NAME
from util.etl_util import conversion_error_condition, native_convert
from util.scenario_util import load_scenario, find_fields

logger = logging.getLogger(__name__)


def validate_and_refine(scenario_json_path, source: DataFrame, day) -> DataFrame:
    scenario = load_scenario(scenario_json_path)

    columns_to_refine = find_fields(scenario)
    logger.info(f'Refinement will be applied to following column(s): {", ".join(map(str, columns_to_refine)).strip()}')

    fields_by_name = {field[FIELD_NAME]: field for field in scenario[FIELDS]}
    projected_columns = []
    error_counts = []
    for column_name in source.columns:
        field = fields_by_name.get(column_name)
        if not field:
            projected_columns.append(func.col(column_name))
            continue

        projected_columns.append(
            native_convert(func.col(column_name), field[FIELD_TYPE]).alias(field[FIELD_MAP_AS])
        )
        error_counts.append(func.sum(func.when(
            conversion_error_condition(func.col(column_name), field[FIELD_TYPE]), 1
        ).otherwise(0)).alias(column_name))

    if error_counts:
        counts = source.agg(*error_counts).first().asDict()
        for column_name, error_count in counts.items():
            if error_count:
                logger.warning(f'{error_count} value(s) in {column_name} could not be converted and were set to null')

    mapped_df = source.select(*projected_columns)

    return mapped_df \
        .withColumn(DATE_IMPORTED_FIELD_NAME, func.lit(day)) \
        .withColumn(DAY_PARTITION_FIELD_NAME, func.lit(day)) \
        .withColumn(SOFT_DELETED_FIELD_NAME, func.lit(None).cast(TimestampType()))

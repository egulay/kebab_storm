from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, IntegerType, FloatType, DateType, StructField, StructType


def column_to_list(df, col_name):
    return [x[col_name] for x in df.select(col_name).collect()]


def two_columns_to_dictionary(df, key_col_name, value_col_name):
    k, v = key_col_name, value_col_name
    return {x[k]: x[v] for x in df.select(k, v).collect()}


def to_list_of_dictionaries(df):
    return list(map(lambda r: r.asDict(), df.collect()))


def equivalent_type(f):
    if f == 'datetime64[ns]':
        return DateType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


def pandas_to_spark(pandas_df, session: SparkSession):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return session.createDataFrame(pandas_df, p_schema)

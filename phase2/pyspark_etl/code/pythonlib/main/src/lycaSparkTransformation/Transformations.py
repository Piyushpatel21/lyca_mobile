import pyspark.sql.functions as F
from pyspark.sql.types import *

DEFAULT_VALUE_DICT = {'string': '0', 'number': 0, 'date': '1970-01-01', 'datetime': '1970-01-01 00:00:00'}


def trimWhiteSpaces(column):
    return F.trim(column)


def fillNull(df, default_value_dict=DEFAULT_VALUE_DICT):
    col_default_values = {}
    for elem in df.schema:
        if elem.dataType == StringType():
            col_default_values[elem.name] = default_value_dict['string']
        elif elem.dataType in [IntegerType(), DoubleType(), FloatType()]:
            col_default_values[elem.name] = default_value_dict['number']
        elif elem.dataType == DateType():
            col_default_values[elem.name] = default_value_dict['date']
        elif elem.dataType == TimestampType():
            col_default_values[elem.name] = default_value_dict['datetime']

    return df.fillna(col_default_values)


def trimAllCols(df):
    final_df = df
    for elem in df.schema:
        if elem.dataType == StringType():
            print("Triming {name}".format(name=elem.name))
            final_df = final_df.withColumn(elem.name, trimWhiteSpaces(F.col(elem.name)))

    return final_df

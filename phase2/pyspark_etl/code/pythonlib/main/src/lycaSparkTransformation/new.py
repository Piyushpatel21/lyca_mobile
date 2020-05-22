#
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as fa
# from pyspark.sql.functions import udf
# from pyspark.sql.types import *
#
#
# sparkSession = SparkSession.builder.master("local").appName("appname").getOrCreate()
#
# def chekNull(x):
#     if x is not None:
#         return 'AA'
#
#
# udf_calc = udf(chekNull, StringType())
# sourceDF = sparkSession.createDataFrame([(1, 'a', 'c'), (2, "b", 'd')], ['n', 'n1', 'n2'])
# fields = sourceDF.schema.fields
# stringFields = filter(lambda f: isinstance(f.dataType, StringType), fields)
# nonStringFields = map(lambda f: fa.col(f.name), filter(lambda f: not isinstance(f.dataType, StringType), fields))
# # stringFieldsTransformed = map(lambda f: fa.upper(fa.col(f.name)), stringFields)
# stringFieldsTransformed = map(lambda f: udf_calc(fa.col(f.name)), stringFields)
# allFields = [*stringFieldsTransformed, *nonStringFields]
# df = sourceDF.select(allFields).show()
#
#
# # stringFieldsTransformed = map(lambda f: 'AA' if fa.col(f.name) == 'A' else fa.col(f.name), stringFields)

run_date = 201
batchID = 1
var =None
if not (run_date and batchID):
    var = False
else:
    var = True

print(var)
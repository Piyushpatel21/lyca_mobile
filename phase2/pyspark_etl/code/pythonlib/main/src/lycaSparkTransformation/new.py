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

# run_date = 201
# batchID = 1
# var =None
# if not (run_date and batchID):
#     var = False
# else:
#     var = True
#
# print(var)
# from datetime import datetime, timedelta
#
#
# def hourRounder(t):
#     # Rounds to nearest hour by adding a timedelta hour if minute >= 30
#     return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
#             + timedelta(hours=t.minute // 30))
#
#
# def getTimeInterval(now):
#     return now + timedelta(hours=6)
#
#
# prevDate = datetime.now() + timedelta(days=-1)
# run_date = prevDate.date().strftime('%Y%m%d')
# batch_from = hourRounder(prevDate)
# batch_to = getTimeInterval(batch_from)
#
# print(prevDate)
#
# print(hourRounder(prevDate))

dm_normal_count = [10]
dm_normal_dupl_count = [30]
batchid = 1
dm_normal_status = 'Complete'
metaQuery = ("update uk_rrbs_dm.log_batch_status_rrbs set dm_normal_status='{dm_normal_status}', dm_normal_count={dm_normal_count}, dm_normal_dupl_count={dm_normal_dupl_count} where batch_id={batch_id}"
                .format(batch_id=batchid, dm_normal_status=dm_normal_status, dm_normal_count=''.join(str(e) for e in dm_normal_count),dm_normal_dupl_count=''.join(str(e) for e in dm_normal_dupl_count)))
print(metaQuery)
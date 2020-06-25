import argparse
import sys
from datetime import datetime, timedelta
from lycaSparkTransformation.TransformActionChain import TransformActionChain
from lycaSparkTransformation.SparkSessionBuilder import SparkSessionBuilder
from lycaSparkTransformation.LycaCommonETLLoad import LycaCommonETLLoad


def start_execution(args):
    lycaETL = LycaCommonETLLoad(args.get('module'), args.get('submodule'), args.get('configfile'), args.get('connfile'),
                                args.get('master'), args.get('run_date'), args.get('batchID'))
    args = lycaETL.parseArguments()
    prevDate = datetime.now() + timedelta(days=-1)
    if not (args.get('run_date') and args.get('batchID')):
        run_date = prevDate.date().strftime('%Y%m%d')
    else:
        run_date = args.get('run_date')
    appname = args.get('module') + '-' + args.get('submodule')
    configfile = args.get('configfile')
    connfile = args.get('connfile')
    sparkSessionBuild = SparkSessionBuilder(args.get('master'), appname).sparkSessionBuild()
    sparkSession = sparkSessionBuild.get("sparkSession")
    logger = sparkSessionBuild.get("logger")
    tf = TransformActionChain(sparkSession, logger, args.get('module'), args.get('submodule'), configfile, connfile, run_date, prevDate)
    if not (args.get('run_date') and args.get('batchID')):
        batch_id = tf.getBatchID()
    else:
        batch_id = args.get('batchID')
    if not batch_id:
        logger.error("Batch ID not available for current timestamp run_date : ".format(run_date=run_date) )
        sys.exit(1)
    logger.info("Running application for : run_date={run_date}, batch_id={batch_id}".format(batch_id=batch_id, run_date=run_date))
    propColumns = tf.srcSchema()
    print(propColumns.get("tgtSchema"))
    duplicateData, lateUnique, normalUnique, recordCount = tf.getSourceData(batch_id, propColumns.get("srcSchema"), propColumns.get("checkSumColumns"))

    print(normalUnique.collect())
    #normalUnique.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in normalUnique.columns]).show()
    for elem in normalUnique.schema:
        print(elem.name + " : " + str(elem.dataType))

    df_from_file = sparkSession.read.csv('../resources/new_sample_file.cdr', header=False)
    for col_name, target_col, expected_col in zip(normalUnique.schema, normalUnique.collect()[0], df_from_file.collect()[0]):
        print(col_name)
        print(target_col)
        print(expected_col)

    # normalDB, lateDB,  = tf.getDbDuplicate()
    # normalNew, normalDuplicate, normalcdr_count, normalcdr_dupl_count = tf.getNormalCDR(normalUnique, normalDB, batch_id)
    # lateNew, lateDuplicate, latecdr_count, latecdr_dupl_count = tf.getLateCDR(lateUnique, lateDB)
    # dfmetadata = recordCount.join(normalcdr_count, on='filename', how='left_outer') \
    #                         .join(normalcdr_dupl_count, on='filename', how='left_outer') \
    #                         .join(latecdr_count, on='filename', how='left_outer') \
    #                         .join(latecdr_dupl_count, on='filename', how='left_outer')
    # print("we are processing : normalNew={normalNew}, lateNew={lateNew}, lateDuplicate={lateDuplicate}, "
    #       "normalDuplicate={normalDuplicate} "
    #       .format(normalNew=normalcdr_count.count(), lateNew=latecdr_count.count(), lateDuplicate=latecdr_dupl_count.count(), normalDuplicate=normalcdr_dupl_count.count()))
    # tf.writetoDataMart(sparkSession, propColumns.get("tgtSchema"))
    # tf.writetoLateCDR(lateNew, propColumns.get("tgtSchema"))
    # tf.writetoDuplicateCDR(lateDuplicate, propColumns.get("tgtSchema"))
    # tf.writetoDuplicateCDR(duplicateData, propColumns.get("tgtSchema"))
    # tf.writetoDuplicateCDR(normalDuplicate, propColumns.get("tgtSchema"))
    # tf.writetoDataMart(lateNew, propColumns.get("tgtSchema"))
    # tf.writeBatchFileStatus(dfmetadata, batch_id)


def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_date', help='run date required for trigger pipeline')
    parser.add_argument('--batchID', help='run date required for trigger pipeline')
    parser.add_argument('--module', help='module name required to process data')
    parser.add_argument('--submodule', help='submodule name required to process data')
    parser.add_argument('--configfile', help='application module level config file path')
    parser.add_argument('--connfile', help='connection config file path')
    parser.add_argument('--master', help='session for glue')
    known_arguments, unknown_arguments = parser.parse_known_args()
    arguments = vars(known_arguments)
    if arguments:
        if not (arguments.get('module') and arguments.get('submodule')):
            print("--run_date --module, --submodule required for trigger pipeline")
            sys.exit(1)
    return arguments


if __name__ == '__main__':
     args = parseArguments()
     start_execution(args)

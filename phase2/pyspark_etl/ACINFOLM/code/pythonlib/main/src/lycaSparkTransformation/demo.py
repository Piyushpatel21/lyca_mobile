def getSourceData(self, batchid, srcSchema, checkSumColumns) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    self.logger.info("***** reading source data from s3 *****")
    file_list = self.redshiftprop.getFileList(self.sparkSession, batchid)
    path = self.property.get(
        "sourceFilePath") + "/" + self.module.upper() + "/" + "UK" + "/" + self.subModule.upper() + "/" + self.run_date[
                                                                                                          :4] + "/" + self.run_date[
                                                                                                                      4:6] + "/" + self.run_date[
                                                                                                                                   6:8] + "/"
    try:
        df_source_raw = self.trans.readSourceFile(self.sparkSession, path, srcSchema, batchid, checkSumColumns,
                                                  file_list)
        if self.property.get("subModule") in ("mnp_portin_request", "mnp_portout_request "):
            gprsModuleTransformation = GprsDataTransformation()
            df_source_with_datatype = gprsModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = gprsModuleTransformation.generateDerivedColumnsForGprs(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        elif self.property.get("subModule") == "voice":
            voiceModuleTransformation = VoiceDataTransformation()
            df_source_with_datatype = voiceModuleTransformation.convertTargetDataType(df_source_raw, srcSchema)
            df_source = voiceModuleTransformation.generateDerivedColumnsForVoice(df_source_with_datatype)
        else:
            df_source = df_source_raw
        s3_batchreadcount = df_source.agg(
            py_function.count('batch_id').cast(IntegerType()).alias('s3_batchreadcount')).rdd.flatMap(
            lambda row: row).collect()
        s3_filecount = df_source.agg(
            py_function.countDistinct('filename').cast(IntegerType()).alias('s3_filecount')).rdd.flatMap(
            lambda row: row).collect()
        batch_status = 'Started'
        metaQuery = (
            "INSERT INTO uk_rrbs_dm.log_batch_status_mno (BATCH_ID, S3_BATCHREADCOUNT, S3_FILECOUNT, BATCH_STATUS, BATCH_START_DT) values({batch_id},{s3_batchreadcount},{s3_filecount},'{batch_status}','{batch_start_dt}')"
            .format(batch_id=batchid, s3_batchreadcount=''.join(str(e) for e in s3_batchreadcount),
                    s3_filecount=''.join(str(e) for e in s3_filecount), batch_status=batch_status,
                    batch_start_dt=self.batch_start_dt))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
        df_duplicate = self.trans.getDuplicates(df_source, "rec_checksum")
        batch_status = 'In-Progress'
        intrabatch_dupl_count = df_duplicate.agg(
            py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_DUPL_COUNT')).rdd.flatMap(
            lambda row: row).collect()
        intrabatch_dist_dupl_count = df_duplicate.select(df_duplicate["rec_checksum"]).distinct().count()
        metaQuery = (
            "update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_DEDUPL_STATUS='Complete', INTRABATCH_DUPL_COUNT={intrabatch_dupl_count}, BATCH_STATUS='{batch_status}', INTRABATCH_DIST_DUPL_COUNT={intrabatch_dist_dupl_count} where BATCH_ID={batch_id} and BATCH_END_DT is null"
            .format(batch_id=batchid, batch_status=batch_status,
                    intrabatch_dupl_count=''.join(str(e) for e in intrabatch_dupl_count),
                    intrabatch_dist_dupl_count=intrabatch_dist_dupl_count))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
        df_unique_late = self.trans.getUnique(df_source, "rec_checksum")
        intrabatch_late_count = df_unique_late.agg(
            py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_NEW_LATE_COUNT')).rdd.flatMap(
            lambda row: row).collect()
        metaQuery = (
            "update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_NEW_LATE_COUNT={intrabatch_late_count} where batch_id={batch_id} and batch_end_dt is null".format(
                batch_id=batchid, intrabatch_late_count=''.join(str(e) for e in intrabatch_late_count)))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
        df_unique_normal = self.trans.getUnique(lateOrNormalCdr, "rec_checksum").filter("normalOrlate == 'Normal'")
        intrabatch_new_count = df_unique_normal.agg(
            py_function.count('batch_id').cast(IntegerType()).alias('INTRABATCH_NEW_NORMAL_COUNT')).rdd.flatMap(
            lambda row: row).collect()
        intrabatch_status = 'Complete'
        metaQuery = (
            "update uk_rrbs_dm.log_batch_status_mno set INTRABATCH_NEW_NORMAL_COUNT={intrabatch_new_count}, INTRABATCH_DEDUPL_STATUS='{intrabatch_status}' where BATCH_ID={batch_id} and BATCH_END_DT is null".format(
                batch_id=batchid, intrabatch_status=intrabatch_status,
                intrabatch_new_count=''.join(str(e) for e in intrabatch_new_count)))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
        self.logger.info("***** source data prepared for transformation *****")
        record_count = df_source.groupBy('filename').agg(
            py_function.count('batch_id').cast(IntegerType()).alias('RECORD_COUNT'))
        return df_duplicate, df_unique_late, df_unique_normal, record_count
    except Exception as ex:
        metaQuery = (
            "INSERT INTO uk_rrbs_dm.log_batch_status_mno (batch_id,batch_status, batch_start_dt, batch_end_dt) values({batch_id}, '{batch_status}', '{batch_start_dt}', '{batch_end_dt}')".format(
                batch_id=batchid, batch_status='Failed', batch_start_dt=self.batch_start_dt,
                batch_end_dt=datetime.now()))
        self.redshiftprop.writeBatchStatus(self.sparkSession, metaQuery)
        self.logger.error("Failed to create source data : {error}".format(error=ex))
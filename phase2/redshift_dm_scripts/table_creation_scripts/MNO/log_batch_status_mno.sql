create table log_batch_status_mno
(
    batch_id                    integer encode az64 distkey,
    s3_batchreadcount           integer encode az64,
    s3_filecount                integer encode az64,
    intrabatch_new_normal_count integer encode az64,
    intrabatch_new_late_count   integer encode az64,
    intrabatch_dupl_count       integer encode az64,
    intrabatch_dist_dupl_count  integer encode az64,
    intrabatch_dedupl_status    varchar(20),
    dm_normal_count             integer encode az64,
    dm_normal_dbdupl_count      integer encode az64,
    dm_normal_status            varchar(20),
    ldm_latecdr_count           integer encode az64,
    ldm_latecdr_dbdupl_count    integer encode az64,
    ldm_latecdr_status          varchar(20),
    batch_start_dt              timestamp encode az64,
    batch_end_dt                timestamp encode az64,
    batch_status                varchar(50)
)
    diststyle key;

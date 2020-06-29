-- Call to load batch data into log_batch_files_rrbs From log_landing.
call sp_create_batch_rrbs('2020-04-01 00:00:00', '1', 'RRBS/GBR/SMS', 'uk_logs')
-- Parameter list : runtime, number of batches for a day, File location prefix (detail most prefix), log table schema


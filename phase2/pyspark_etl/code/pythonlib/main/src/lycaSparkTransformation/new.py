from datetime import datetime

metaQuery = ("INSERT INTO uk_rrbs_dm.log_batch_status_rrbs (batch_id,batch_status, batch_start_dt, batch_end_dt) values({batch_id}, '{batch_status}', '{batch_start_dt}', '{batch_end_dt}')"
             .format(batch_id=10, batch_status='Failed', batch_start_dt=datetime.now(), batch_end_dt=datetime.now()))

print(metaQuery)
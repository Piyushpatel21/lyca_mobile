/* Work in progress  */
--#Sample scripts---------------------------------------------------------------
CREATE TABLE ukdev.uk_dev_fact.fact_rrbs_uk_voice
DISTSTYLE EVEN
AS SELECT * FROM ukdev.uk_dev_stg.stg_rrbs_uk_voice;
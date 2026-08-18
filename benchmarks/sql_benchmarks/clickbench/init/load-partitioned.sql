CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/hits_partitioned/';

CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw
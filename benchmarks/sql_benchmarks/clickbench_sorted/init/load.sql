# Run benchmark with prefer_existing_sort configuration
# This allows DataFusion to optimize away redundant sorts while maintaining parallelism

set datafusion.optimizer.prefer_existing_sort=true

CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION 'data/hits_sorted.parquet' WITH ORDER ("${SORTED_BY:-EventTime}" ${SORTED_ORDER:-ASC});

CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw;
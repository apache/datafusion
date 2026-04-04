-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT MAX(fv) FROM (
    SELECT FIRST_VALUE("WatchID" ORDER BY "EventTime") as fv
    FROM hits
    GROUP BY "OS"
);

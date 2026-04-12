-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT MAX(len) FROM (
    SELECT LENGTH(FIRST_VALUE("URL" ORDER BY "EventTime")) as len
    FROM hits
    GROUP BY "OS"
);

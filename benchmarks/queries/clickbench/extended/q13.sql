-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "CounterID", COUNT(*) AS page_views FROM hits WHERE "URL" LIKE 'http%' GROUP BY "CounterID" ORDER BY page_views DESC LIMIT 10;

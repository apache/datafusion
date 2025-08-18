-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "CounterID", AVG(length("URL")) AS l, COUNT(*) AS c FROM hits WHERE "URL" <> '' GROUP BY "CounterID" HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

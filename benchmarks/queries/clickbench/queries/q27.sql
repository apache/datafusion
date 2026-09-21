-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

-- DataFusion length(...) counts characters; use octet_length(...) for ClickBench byte-length semantics.
SELECT "CounterID", AVG(octet_length("URL")) AS l, COUNT(*) AS c FROM hits WHERE "URL" <> '' GROUP BY "CounterID" HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

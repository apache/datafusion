-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

-- DataFusion length(...) counts characters; use octet_length(...) for ClickBench byte-length semantics.
SELECT REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(octet_length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM hits WHERE "Referer" <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

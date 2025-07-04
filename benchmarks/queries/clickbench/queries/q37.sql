-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "Title", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND "DontCountHits" = 0 AND "IsRefresh" = 0 AND "Title" <> '' GROUP BY "Title" ORDER BY PageViews DESC LIMIT 10;

-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "EventTime" LIMIT 10;

-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT SUM("CounterID") AS counter_id_sum
FROM hits
WHERE "URL" < 'zzzz';

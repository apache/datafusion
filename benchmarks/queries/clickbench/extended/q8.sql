-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "RegionID", "UserAgent", "OS", AVG(to_timestamp("ResponseEndTiming")-to_timestamp("ResponseStartTiming")) as avg_response_time, AVG(to_timestamp("ResponseEndTiming")-to_timestamp("ConnectTiming")) as avg_latency FROM hits GROUP BY "RegionID", "UserAgent", "OS" ORDER BY avg_latency DESC limit 10;
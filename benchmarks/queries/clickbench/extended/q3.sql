-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "SocialSourceNetworkID", "RegionID", COUNT(*), AVG("Age"), AVG("ParamPrice"), STDDEV("ParamPrice") as s, VAR("ParamPrice")  FROM hits GROUP BY "SocialSourceNetworkID", "RegionID" HAVING s IS NOT NULL ORDER BY s DESC LIMIT 10;

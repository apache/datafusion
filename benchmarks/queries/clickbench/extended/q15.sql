-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT MAX(c) FROM (
    SELECT covar_samp("ResolutionWidth", "ResolutionHeight") as c
    FROM hits
    GROUP BY "UserID"
);

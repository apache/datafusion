-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT "ClientIP", "WatchID",  COUNT(*) c, MIN("ResponseStartTiming") tmin, MEDIAN("ResponseStartTiming") tmed, MAX("ResponseStartTiming") tmax FROM hits WHERE "JavaEnable" = 0  GROUP BY  "ClientIP", "WatchID" HAVING c > 1 ORDER BY tmed DESC LIMIT 10;

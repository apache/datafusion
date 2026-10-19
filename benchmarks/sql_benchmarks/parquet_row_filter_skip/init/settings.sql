-- Session settings for the parquet_row_filter_skip suite. `init` runs after
-- `load` and before the asserts and the benchmarked query, so these apply to
-- the measured scan (the COPY in the load script does not need them).
--
-- information_schema is enabled so the template can assert that
-- pushdown_filters actually took effect: without pushdown there is no
-- RowFilter to skip and the suite would silently measure nothing.
set datafusion.catalog.information_schema = true;
set datafusion.execution.parquet.pushdown_filters = true;

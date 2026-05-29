# ClickBench partitioned dataset was written by an ancient version of pyarrow that
# that wrote strings with the wrong logical type. To read it correctly, we must
# automatically convert binary to string.
  
SET datafusion.execution.parquet.binary_as_string = true;
SET datafusion.execution.parquet.pushdown_filters = ${PUSHDOWN_FILTERS:-false};
SET datafusion.execution.parquet.reorder_filters = ${REORDER_FILTERS:-false};
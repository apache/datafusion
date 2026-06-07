# ClickBench partitioned dataset was written by an ancient version of PyArrow that
# wrote strings with the wrong logical type. To read it correctly, we must
# automatically convert binary to string.
  
SET datafusion.execution.parquet.binary_as_string = true;
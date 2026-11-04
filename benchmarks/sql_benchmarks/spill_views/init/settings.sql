-- Session settings for the spill_views suite. `init` runs after `load`, so
-- the COPY in the load script runs without the memory limit.
set datafusion.catalog.information_schema = true;
set datafusion.execution.target_partitions = 4;
set datafusion.runtime.memory_limit = '${SPILL_LIMIT}';

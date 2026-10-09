-- Session settings for the h2o window_sorted subgroup. `init` runs after
-- `load`, so these apply to the measured query and not to the COPY that writes
-- the sorted file.
--
-- enable_window_topn turns on the WindowTopN rewrite, making the baseline the
-- existing per-partition top-K operator rather than an unoptimized
-- window-plus-filter plan. The flag defaults to false, so without this the
-- subgroup would measure a different plan shape than its name claims.
--
-- information_schema is enabled so the template can assert the flags actually
-- took effect.
--
-- prefer_existing_sort decides *how* a declared input ordering is honored once
-- some operator requires it. The heap top-K operator requires no input ordering,
-- so on its own the ordering is simply dropped: the hash repartition that
-- co-locates each partition key is inserted with preserve_order: false, and that
-- is not a defect. For an operator that does require the ordering, the planner
-- then has two ways to satisfy it — an order-preserving repartition, or a
-- SortExec that re-sorts data already sorted on disk. This setting picks the
-- former. Without it such an operator would measure a full sort and the sorted
-- input would be pointless.
set datafusion.catalog.information_schema = true;
set datafusion.optimizer.enable_window_topn = true;
set datafusion.optimizer.prefer_existing_sort = true;
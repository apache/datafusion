-- information_schema is enabled so the template can assert the flag took effect.
set datafusion.catalog.information_schema = true;
set datafusion.optimizer.enable_piecewise_merge_join = true;

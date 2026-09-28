DROP TABLE IF EXISTS lhs;
DROP TABLE IF EXISTS rhs;
reset datafusion.optimizer.enable_piecewise_merge_join;
reset datafusion.catalog.information_schema;

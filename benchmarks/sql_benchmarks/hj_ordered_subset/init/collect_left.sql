-- Force HashJoinExec mode=CollectLeft, also for the 10-day build side.
set datafusion.optimizer.hash_join_single_partition_threshold = 1073741824;
set datafusion.optimizer.hash_join_single_partition_threshold_rows = 1000000000;

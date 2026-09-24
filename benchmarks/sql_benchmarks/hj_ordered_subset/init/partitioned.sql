-- Force HashJoinExec mode=Partitioned: the estimated size of a filtered
-- build side is often below the CollectLeft thresholds.
set datafusion.optimizer.hash_join_single_partition_threshold = 0;
set datafusion.optimizer.hash_join_single_partition_threshold_rows = 0;

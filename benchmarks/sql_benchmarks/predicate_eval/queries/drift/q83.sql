-- Same predicate again, over a dataset that swaps which conjunct is selective
-- every 131072 rows (16 batches at the default batch size). Because
-- `CREATE TABLE ... AS SELECT` fans the rows out with `RoundRobinBatch`, whole
-- batches are dealt round-robin and each partition sees the alternation within
-- its own stream -- roughly every 16 / target_partitions batches -- rather than
-- one partition-wide bias (see load/drift_blocks.sql). No single ordering is
-- right for the whole scan. cf. q80/q81 (one early flip), q82 (one late flip).
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

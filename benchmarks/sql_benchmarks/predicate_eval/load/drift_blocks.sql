-- Repeated-drift dataset: the two mirrored predicates of `drift.sql` swap which
-- one is selective every 131072 rows (= 16 batches at the default 8192-row
-- batch size), so the best order flips many times over one scan instead of once.
--
--   a_sel = 0  selective (~0.1%) in even-numbered blocks, unselective (~50%)
--              in odd-numbered ones; b_sel = 0 is the mirror.
--
-- Intended shape and what actually happens. The original intent was a *per
-- partition* skew: each partition favouring a different conjunct for its whole
-- stream, so one global decision cannot be right for all of them. That is not
-- reachable from the data alone. `CREATE TABLE ... AS SELECT` collects the
-- SELECT's partitioned output into the MemTable, and the plan for a
-- `generate_series` scan is a single partition fanned out by a
-- `RepartitionExec` with `RoundRobinBatch(target_partitions)`, which deals
-- *whole batches* round-robin: partition `p` ends up holding batches
-- `p, p+P, p+2P, ...` in order. Any contiguous block of rows is therefore
-- sprayed across every partition, and the block index -- not the partition --
-- decides which conjunct wins. (Verified on a 12-core box: a 1M-row
-- `CREATE TABLE ... AS SELECT ... FROM generate_series(...)` scans as
-- `DataSourceExec: partitions=12, partition_sizes=[11, 11, 11, 10, ...]`, i.e.
-- 123 batches dealt out round-robin.)
--
-- So this is a *within-partition* alternating-block shape, and is documented as
-- such: each partition sees the flip repeatedly, after roughly 16 /
-- target_partitions batches of its own input. The one arrangement that would
-- make it partition-constant -- a block of exactly one batch, with an even
-- target_partitions, so batch parity is constant within a partition -- depends
-- on both the batch size and the partition count, so it is deliberately not
-- relied on here.
--
-- PRED_ROWS sizes the table; the block length is absolute, so the number of
-- flips grows with the table.
CREATE TABLE t AS
SELECT
  value AS seq,
  CASE WHEN (value / 131072) % 2 = 0 THEN value % 1000 ELSE value % 2    END AS a_sel,
  CASE WHEN (value / 131072) % 2 = 0 THEN value % 2    ELSE value % 1000 END AS b_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

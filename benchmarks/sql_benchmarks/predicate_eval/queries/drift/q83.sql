-- Per-partition skew: the same 16 files, one whole file per partition, so every
-- stream sees a single fixed profile for its whole life and one pooled decision is
-- backwards for half of them -- only a per-stream decision fixes that.
-- File-backed because CTAS round-robins batches across partitions. cf. q82.
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

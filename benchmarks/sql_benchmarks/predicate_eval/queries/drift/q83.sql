-- Per-partition skew: one whole file per stream, so each stream sees a single fixed
-- profile and one pooled decision is backwards for half of them.
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

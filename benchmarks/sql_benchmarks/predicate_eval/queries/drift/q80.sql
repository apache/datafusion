-- Selectivity drifts across the scan: `a_sel = 0` matches ~0.1% over the first 2%
-- of rows and ~50% after, and `b_sel = 0` is the mirror.
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

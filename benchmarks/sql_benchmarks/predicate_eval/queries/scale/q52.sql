-- q50 at PRED_ROWS=5_000_000 (~610 batches). See q50.
SELECT count(*) FROM t
WHERE c0 < 90
  AND regexp_like(s, 'rare');

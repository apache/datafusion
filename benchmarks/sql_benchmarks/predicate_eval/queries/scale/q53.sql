-- q50 at PRED_ROWS=50_000_000 (~6100 batches); builds a ~9 GB table. See q50.
SELECT count(*) FROM t
WHERE c0 < 90
  AND regexp_like(s, 'rare');

-- Same predicates as costsel/q03 (`c0 < 90` ~90% cheap, `regexp_like(s, 'rare')`
-- ~0.1% expensive). q50..q53 sweep table size; here PRED_ROWS=5_000, roughly a
-- single batch.
SELECT count(*) FROM t
WHERE c0 < 90
  AND regexp_like(s, 'rare');

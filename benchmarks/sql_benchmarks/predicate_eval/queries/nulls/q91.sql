-- From the `nulls` dataset: same two compares as q90, but `c0`/`c1` are NULL on
-- ~50%/~67% of rows, so the predicate results are three-valued. cf. q90.
SELECT count(*) FROM t
WHERE c0 < 50
  AND c1 < 50;

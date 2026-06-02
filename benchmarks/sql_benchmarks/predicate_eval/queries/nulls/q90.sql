-- No-null baseline, from the `ints` dataset: two cheap compares (~50% each) over
-- columns with no NULLs. cf. q91 (same query, null-heavy columns).
SELECT count(*) FROM t
WHERE c0 < 50
  AND c1 < 50;

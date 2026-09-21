-- k = 4: three ~90% compares then one ~5% compare.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c2 < 90
  AND c3 < 5;

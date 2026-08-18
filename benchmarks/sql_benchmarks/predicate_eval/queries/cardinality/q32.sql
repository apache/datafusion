-- k = 8: seven ~90% compares followed by one ~5% compare. See q30.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c2 < 90
  AND c3 < 90
  AND c4 < 90
  AND c5 < 90
  AND c6 < 90
  AND c7 < 5;

-- k = 16: fifteen ~90% compares followed by one ~5% compare. See q30.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c2 < 90
  AND c3 < 90
  AND c4 < 90
  AND c5 < 90
  AND c6 < 90
  AND c7 < 90
  AND c8 < 90
  AND c9 < 90
  AND c10 < 90
  AND c11 < 90
  AND c12 < 90
  AND c13 < 90
  AND c14 < 90
  AND c15 < 5;

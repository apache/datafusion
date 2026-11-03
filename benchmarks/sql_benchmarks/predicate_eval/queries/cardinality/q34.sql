-- k = 8 as in q32, but over the 64-column `ints_wide` table.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c2 < 90
  AND c3 < 90
  AND c4 < 90
  AND c5 < 90
  AND c6 < 90
  AND c7 < 5;

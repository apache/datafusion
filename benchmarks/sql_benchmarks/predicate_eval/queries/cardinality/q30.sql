-- k = 2: one ~90% compare then one ~5% compare (q30..q33 sweep k = 2/4/8/16).
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 5;

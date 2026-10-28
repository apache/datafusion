-- Four equally cheap compares, each ~50%: nothing to reorder.
SELECT count(*) FROM t
WHERE c0 < 50
  AND c1 < 50
  AND c2 < 50
  AND c3 < 50;

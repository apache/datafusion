-- Anti-correlated: `x_anti` is `99 - x`, so the pair is empty though each matches ~50%.
SELECT count(*) FROM t
WHERE x < 50
  AND x_anti < 50;

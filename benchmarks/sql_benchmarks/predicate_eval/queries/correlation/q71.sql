-- Positively correlated: `x_pos` is a copy of `x`, so the pair still matches ~20%.
SELECT count(*) FROM t
WHERE x < 20
  AND x_pos < 20;

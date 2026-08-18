-- Hidden: `x` and `ind` are independent, each ~20%, so the conjunction matches
-- ~4% and the second predicate is just as selective among the first's survivors
-- as on its own. Baseline for the correlation sweep. cf. q71, q72.
SELECT count(*) FROM t
WHERE x < 20
  AND ind < 20;

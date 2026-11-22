-- Independent: `x` and `ind` are uncorrelated, each ~20%, so the pair matches ~4%.
SELECT count(*) FROM t
WHERE x < 20
  AND ind < 20;

-- Same three conjuncts as q90 with the nullable selective one (`c_sel < 5`,
-- ~4% true, NULL on 10% of rows) written last. Because a NULL-containing left
-- side disables AND pre-selection, neither order gets to skip work -- an
-- adaptive reorderer that ranks by selectivity alone will move `c_sel < 5` to
-- the front and gain nothing. cf. q90.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c_sel < 5;

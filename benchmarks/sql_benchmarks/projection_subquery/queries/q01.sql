-- Shape: bare uncorrelated IN in the SELECT list. The subquery has no
-- correlation, so the only join key is the outer key against the inner key.
-- This is the plain form of the quadratic plan.
SELECT
  count(*) FILTER (WHERE m) AS true_count,
  count(*) FILTER (WHERE m IS NULL) AS null_count
FROM (SELECT id, id IN (SELECT id FROM inner_t) AS m FROM outer_t);

-- Shape: IN correlated on `<` instead of `=`. The `IN` equality is the hash key
-- and the `<` correlation stays a residual filter of the one null-aware mark
-- join, which applies it when it decides whether a NULL makes the mark UNKNOWN.
SELECT
  count(*) FILTER (WHERE m) AS true_count,
  count(*) FILTER (WHERE m IS NULL) AS null_count
FROM (SELECT id, id IN (SELECT i.id FROM inner_t i WHERE i.z < o.z) AS m FROM outer_t o);

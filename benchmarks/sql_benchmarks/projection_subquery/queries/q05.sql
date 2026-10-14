-- Shape: NOT IN in the SELECT list. It is the negation of q01 and it has the
-- same three-valued logic, so it must plan the same way.
SELECT
  count(*) FILTER (WHERE m) AS true_count,
  count(*) FILTER (WHERE m IS NULL) AS null_count
FROM (SELECT id, id NOT IN (SELECT id FROM inner_t) AS m FROM outer_t);

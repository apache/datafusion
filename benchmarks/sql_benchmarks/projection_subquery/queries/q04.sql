-- Shape: two independent projected IN subqueries in one SELECT list. Each one
-- is decorrelated on its own, so this measures whether the cost of the first
-- one simply doubles.
SELECT
  count(*) FILTER (WHERE a) AS a_count,
  count(*) FILTER (WHERE b) AS b_count
FROM (
  SELECT
    id IN (SELECT id FROM inner_t) AS a,
    id IN (SELECT id FROM inner_t WHERE z < 500) AS b
  FROM outer_t
);

-- Shape: the same bare IN, wrapped in COALESCE so the NULL result becomes
-- false. The wrapper is the common way to use a projected IN, and it must not
-- stop the decorrelation.
SELECT
  count(*) FILTER (WHERE m) AS true_count
FROM (SELECT id, COALESCE((id IN (SELECT id FROM inner_t))::boolean, false) AS m FROM outer_t);

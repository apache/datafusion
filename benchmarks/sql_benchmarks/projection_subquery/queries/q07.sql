-- Shape: IN correlated on `<` instead of `=`. There is no equality to hash on,
-- so this query keeps the nested-loop mark joins. It is the control: its cost
-- must stay the same when the hashable cases get faster.
SELECT
  count(*) FILTER (WHERE m) AS true_count,
  count(*) FILTER (WHERE m IS NULL) AS null_count
FROM (SELECT id, id IN (SELECT i.id FROM inner_t i WHERE i.z < o.z) AS m FROM outer_t o);

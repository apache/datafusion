-- Shape: IN correlated on an equality. The correlation adds a second join key,
-- so the whole join condition stays hashable.
SELECT
  count(*) FILTER (WHERE m) AS true_count,
  count(*) FILTER (WHERE m IS NULL) AS null_count
FROM (SELECT id, id IN (SELECT i.id FROM inner_t i WHERE i.z = o.z) AS m FROM outer_t o);

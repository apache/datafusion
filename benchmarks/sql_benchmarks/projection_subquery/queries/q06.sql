-- Shape: correlated EXISTS in the SELECT list. EXISTS has no NULL result, so
-- one mark join is enough. It is the reference for what the IN queries should
-- cost.
SELECT
  count(*) FILTER (WHERE e) AS true_count
FROM (SELECT id, EXISTS (SELECT 1 FROM inner_t i WHERE i.id = o.id) AS e FROM outer_t o);

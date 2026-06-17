-- Unsupported path: wide projection + ASC LIMIT.
-- Shows row-level filter benefit when RG reorder tightens TopK threshold.
SELECT *
FROM lineitem
ORDER BY l_orderkey
LIMIT 100

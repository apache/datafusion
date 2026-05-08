-- Inexact path: TopK + DESC LIMIT with larger fetch (1000).
-- Larger LIMIT means more row_replacements; RG reorder reduces the
-- total replacement count by tightening the threshold faster.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 1000

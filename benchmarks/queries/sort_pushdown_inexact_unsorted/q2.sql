-- Unsupported path: TopK + ASC LIMIT with larger fetch.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey
LIMIT 1000

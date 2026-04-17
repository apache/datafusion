-- Overlapping RGs: DESC LIMIT with larger fetch.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 1000

-- Unsupported path: DESC LIMIT (no declared ordering = no reverse scan).
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

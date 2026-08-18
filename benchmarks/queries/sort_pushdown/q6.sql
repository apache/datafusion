-- Reverse scan: ORDER BY DESC LIMIT larger fetch (narrow projection)
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 1000

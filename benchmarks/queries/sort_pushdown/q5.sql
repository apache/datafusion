-- Reverse scan: ORDER BY DESC LIMIT (narrow projection)
-- With --sorted: reverse_row_groups=true + TopK + stats init + cumulative prune
-- Without --sorted: full TopK sort over all data
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

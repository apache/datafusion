-- Sort elimination + limit pushdown
-- With --sorted: SortExec removed + limit pushed to DataSourceExec
-- Without --sorted: TopK sort over all data
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey
LIMIT 100

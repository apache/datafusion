-- Sort elimination: ORDER BY sort key ASC (full scan)
-- With --sorted: SortExec removed, sequential scan in file order
-- Without --sorted: full SortExec required
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey

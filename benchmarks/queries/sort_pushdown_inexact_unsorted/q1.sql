-- Unsupported path: TopK + ASC LIMIT on file without declared ordering.
-- Tests RG reorder benefit when no WITH ORDER is declared — the
-- Unsupported path in try_pushdown_sort triggers RG reorder.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey
LIMIT 100

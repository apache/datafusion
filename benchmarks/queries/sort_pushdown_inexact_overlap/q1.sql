-- Overlapping RGs: TopK + DESC LIMIT on file with partially overlapping
-- row groups (simulates streaming data with network jitter).
-- RG reorder places highest-max RG first for fastest threshold convergence.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

-- Inexact path: TopK + DESC LIMIT on ASC-declared file.
-- With RG reorder, the first RG read contains the highest max value,
-- so TopK's threshold tightens quickly and subsequent RGs get filtered
-- efficiently via dynamic filter pushdown.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

-- Unsupported path: wide projection + DESC LIMIT.
SELECT *
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

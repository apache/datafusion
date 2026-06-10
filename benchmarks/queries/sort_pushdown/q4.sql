-- Sort elimination + limit: wide projection
SELECT *
FROM lineitem
ORDER BY l_orderkey
LIMIT 100

-- Unsupported path: wide projection + ASC LIMIT with larger fetch.
SELECT *
FROM lineitem
ORDER BY l_orderkey
LIMIT 1000

-- Overlapping RGs: wide projection + DESC LIMIT larger fetch.
SELECT *
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 1000

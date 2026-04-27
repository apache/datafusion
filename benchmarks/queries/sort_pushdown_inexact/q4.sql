-- Inexact path: wide projection + DESC LIMIT with larger fetch.
-- Combines wide-row row-level filter benefit with larger LIMIT to
-- demonstrate cumulative gains from RG reorder.
SELECT *
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 1000

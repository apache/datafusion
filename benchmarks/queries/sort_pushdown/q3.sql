-- Sort elimination: wide projection (all columns)
-- Tests sort elimination benefit with larger row payload
SELECT *
FROM lineitem
ORDER BY l_orderkey

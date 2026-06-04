-- RIGHT JOIN, ORDER BY column from preserved (right) side.
-- Symmetric to q1: the Sort(fetch) is pushed below the join over the
-- orders scan (the right/preserved side).
SELECT o_orderkey, o_totalprice
FROM customer RIGHT JOIN orders ON c_custkey = o_custkey
ORDER BY o_totalprice
LIMIT 10
-- LEFT JOIN, ORDER BY column from preserved (left) side, small LIMIT.
-- Canonical case for push_down_topk_through_join: the Sort(fetch=10) is
-- duplicated below the join over the customer scan, so only the top 10
-- rows (by c_acctbal) are joined against orders.
SELECT c_custkey, c_acctbal
FROM customer LEFT JOIN orders ON c_custkey = o_custkey
ORDER BY c_acctbal
LIMIT 10
-- LEFT JOIN, multi-column ORDER BY (both columns from preserved side).
-- All sort exprs must come from the preserved side for the rule to fire;
-- this query checks that multi-column sorts are still pushed.
SELECT c_custkey, c_acctbal, c_nationkey
FROM customer LEFT JOIN orders ON c_custkey = o_custkey
ORDER BY c_acctbal, c_nationkey
LIMIT 100
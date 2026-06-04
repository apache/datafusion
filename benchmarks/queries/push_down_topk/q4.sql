-- CROSS JOIN, ORDER BY column from one side.
-- Cross joins preserve every row from both sides; the rule pushes the
-- Sort(fetch) below the join over the side referenced by ORDER BY.
SELECT c_custkey, c_acctbal
FROM customer CROSS JOIN nation
ORDER BY c_acctbal
LIMIT 10
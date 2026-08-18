-- Negative case: ORDER BY references the probe (non-preserved) side.
-- The rule MUST NOT fire here — orders is the right side of a LEFT JOIN
-- so it isn't preserved (rows can be NULL when there's no match), and
-- pushing a Sort with fetch onto orders would change semantics.
-- Included so the bench harness can verify the rule's selectivity.
SELECT c_custkey, o_totalprice
FROM customer LEFT JOIN orders ON c_custkey = o_custkey
ORDER BY o_totalprice
LIMIT 10
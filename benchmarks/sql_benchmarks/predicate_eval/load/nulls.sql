-- Null-density dataset: integer columns that are NULL on a controlled fraction
-- of rows, so predicate results are three-valued (the no-null `ints` dataset
-- has none). Among the non-null rows, `cN < k` still has ~k% selectivity.
--
--   c0  NULL on ~50% of rows (even values)
--   c1  NULL on ~67% of rows (value % 3 <> 0)
--   c2  NULL on ~50% of rows (odd values)
--   c3  NULL on ~80% of rows (value % 5 <> 0)
--
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  CASE WHEN value % 2 = 0 THEN (value * 1)  % 100 END AS c0,
  CASE WHEN value % 3 = 0 THEN (value * 7)  % 100 END AS c1,
  CASE WHEN value % 2 = 1 THEN (value * 9)  % 100 END AS c2,
  CASE WHEN value % 5 = 0 THEN (value * 11) % 100 END AS c3
FROM generate_series(1, ${PRED_ROWS:-1000000});

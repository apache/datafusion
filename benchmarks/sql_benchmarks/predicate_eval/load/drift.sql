-- Drift dataset: two predicates whose *relative* selectivity flips partway
-- through the scan, so whole-table selectivity differs from per-batch
-- selectivity. Rows are emitted in `seq` order, so batches observe the drift in
-- order.
--
--   a_sel = 0  is selective (~0.1%) in the first 10% of rows, unselective
--              (~50%) afterwards.
--   b_sel = 0  is the mirror: unselective early, selective late.
--
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  value AS seq,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 10 THEN value % 1000 ELSE value % 2    END AS a_sel,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 10 THEN value % 2    ELSE value % 1000 END AS b_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

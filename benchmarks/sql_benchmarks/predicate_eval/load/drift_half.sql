-- Late-drift dataset: the same two mirrored predicates as `drift.sql`, but the
-- flip happens at the *halfway* point of the table instead of 10% in. Rows are
-- emitted in `seq` order, so batches observe the drift in order.
--
--   a_sel = 0  is selective (~0.1%) over the first half of the rows,
--              unselective (~50%) over the second half.
--   b_sel = 0  is the mirror: unselective first, selective second.
--
-- Why a second dataset rather than a knob on `drift.sql`: at the default
-- PRED_ROWS the 10% flip in `drift.sql` lands within the first few batches of
-- the scan, so even a one-shot warm-up (say, 8 batches) already sees the
-- post-flip order and stays right for the remaining ~90% of the scan. Flipping
-- at the halfway point instead makes a warm-up-and-freeze decision wrong for
-- half of the rows, which is the case a re-evaluating reorderer has to notice.
--
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  value AS seq,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 2 THEN value % 1000 ELSE value % 2    END AS a_sel,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 2 THEN value % 2    ELSE value % 1000 END AS b_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

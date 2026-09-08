-- Drift dataset: rows are emitted in `seq` order and which predicate is more
-- selective flips partway through, so whole-table selectivity differs from
-- per-batch selectivity. `a_sel = 0` matches ~0.1% over the first 2% of rows and
-- ~50% after; `b_sel = 0` is the mirror. At the default PRED_ROWS, which sizes the
-- table, that flip lands inside the pooled 8-batch warm-up (~65k rows).
CREATE TABLE t AS
SELECT
  value AS seq,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 50 THEN value % 1000 ELSE value % 2    END AS a_sel,
  CASE WHEN value < ${PRED_ROWS:-1000000} / 50 THEN value % 2    ELSE value % 1000 END AS b_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

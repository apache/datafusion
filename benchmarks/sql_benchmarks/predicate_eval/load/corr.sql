-- Correlation dataset: a base column plus derived columns that control the
-- *conditional* selectivity of one predicate given another (its selectivity
-- among the rows that already passed the other).
--
--   x       uniform [0,100)
--   x_pos   = x         (perfectly positively correlated: `x<k AND x_pos<k`
--                        passes ~k%, not the ~k%^2 an independence assumption
--                        predicts)
--   x_anti  = 99 - x    (anti-correlated: `x<k AND x_anti<k` is empty for k<=50)
--   ind     independent control column, uniform [0,100)
--
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 7)  % 100        AS x,
  (value * 7)  % 100        AS x_pos,
  99 - ((value * 7) % 100)  AS x_anti,
  (value * 13) % 100        AS ind
FROM generate_series(1, ${PRED_ROWS:-1000000});

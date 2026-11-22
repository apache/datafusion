-- Correlation dataset: `x` uniform on [0,100); `x_pos` = x (perfectly positively
-- correlated); `x_anti` = 99 - x (anti-correlated); `ind` uniform on [0,100) and
-- independent of x. PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 7)  % 100        AS x,
  (value * 7)  % 100        AS x_pos,
  99 - ((value * 7) % 100)  AS x_anti,
  (value * 13) % 100        AS ind
FROM generate_series(1, ${PRED_ROWS:-1000000});

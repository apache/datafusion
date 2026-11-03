-- Mixed-cost dataset: cheap integer columns c0..c3 uniform on [0,100) (`cN < k`
-- ~k%) alongside one wide string column `s` carrying three markers matched by an
-- expensive `regexp_like`: 'rare' ~0.1%, 'ten' ~10%, 'aaa' ~90%. PRED_FILL is the
-- string-width knob and PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 1)  % 100 AS c0,
  (value * 3)  % 100 AS c1,
  (value * 7)  % 100 AS c2,
  (value * 9)  % 100 AS c3,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 1009 = 5  THEN 'rare' ELSE 'zzzz' END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 10   = 0  THEN 'ten'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 10  <> 0  THEN 'aaa'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30}) AS s
FROM generate_series(1, ${PRED_ROWS:-1000000});

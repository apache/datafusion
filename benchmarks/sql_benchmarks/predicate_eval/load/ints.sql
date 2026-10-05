-- Seventeen integer columns: c0..c15 uniform on [0,100), so `cN < k` has
-- selectivity ~k%, all equally cheap and mutually decorrelated (the multipliers
-- are coprime to 100); plus `c_sel`, NULL on 10% of rows and uniform on [0,100)
-- elsewhere, so `c_sel < 5` is true on 4% of rows and NULL on 10% -- a selective
-- conjunct that is not NULL-free. PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 1)  % 100 AS c0,
  (value * 3)  % 100 AS c1,
  (value * 7)  % 100 AS c2,
  (value * 9)  % 100 AS c3,
  (value * 11) % 100 AS c4,
  (value * 13) % 100 AS c5,
  (value * 17) % 100 AS c6,
  (value * 19) % 100 AS c7,
  (value * 21) % 100 AS c8,
  (value * 23) % 100 AS c9,
  (value * 27) % 100 AS c10,
  (value * 29) % 100 AS c11,
  (value * 31) % 100 AS c12,
  (value * 33) % 100 AS c13,
  (value * 37) % 100 AS c14,
  (value * 39) % 100 AS c15,
  CASE WHEN value % 10 = 0 THEN NULL ELSE (value * 11) % 100 END AS c_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

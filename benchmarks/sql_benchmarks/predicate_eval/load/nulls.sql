-- Nullable-predicate dataset: cheap integer columns like `ints.sql`, plus one
-- column whose selective predicate is *nullable*.
--
--   c0..c3   uniform [0,100), so `cN < k` has selectivity ~k% and never NULL
--   c_sel    NULL on exactly 10% of rows (value % 10 = 0), uniform [0,100)
--            elsewhere, so `c_sel < 5` is true on exactly 4% of rows, NULL on
--            10%, and false on the rest
--
-- `c_sel < 5` is true for value % 100 in {0,91,82,73,64} (91 is the inverse of
-- 11 mod 100); the NULL rule removes the single residue 0 from that set, which
-- is what turns the nominal 5% into exactly 4%.
--
-- The point of the dataset is the NULLs, not the 4%: a boolean array with any
-- NULL in it disables `BinaryExpr`'s AND pre-selection for the whole batch
-- (`check_short_circuit` bails on `null_count() > 0`), so a selective but
-- nullable conjunct gates nothing no matter where it is written.
--
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 1)  % 100 AS c0,
  (value * 3)  % 100 AS c1,
  (value * 7)  % 100 AS c2,
  (value * 9)  % 100 AS c3,
  CASE WHEN value % 10 = 0 THEN NULL ELSE (value * 11) % 100 END AS c_sel
FROM generate_series(1, ${PRED_ROWS:-1000000});

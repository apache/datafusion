-- Hidden: `c_sel < 5` is the selective conjunct (~4%) but it is *nullable* --
-- `c_sel` is NULL on 10% of rows, so the conjunct evaluates to NULL there.
-- `BinaryExpr` AND only pre-selects when the left-hand boolean array has no
-- NULLs, so writing this conjunct first gates nothing: `c0 < 90` and `c1 < 90`
-- are still evaluated over every row. Written first here; cf. q91 (written
-- last), which is the same work in the other order.
SELECT count(*) FROM t
WHERE c_sel < 5
  AND c0 < 90
  AND c1 < 90;

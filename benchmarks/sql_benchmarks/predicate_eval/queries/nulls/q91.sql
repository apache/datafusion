-- q90 with the nullable selective conjunct written last, which is the same work.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c_sel < 5;

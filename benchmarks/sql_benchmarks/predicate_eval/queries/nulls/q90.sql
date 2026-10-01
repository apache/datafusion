-- The selective conjunct (~4%) is nullable, so AND pre-selection is disabled and
-- writing it first gates nothing. cf. q91.
SELECT count(*) FROM t
WHERE c_sel < 5
  AND c0 < 90
  AND c1 < 90;

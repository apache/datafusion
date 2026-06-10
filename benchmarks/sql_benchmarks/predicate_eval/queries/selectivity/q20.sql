-- Hidden: two equally cheap integer compares of unequal selectivity -- `c4 < 95`
-- matches ~95%, `c0 < 5` matches ~5%. Less selective one written first.
-- cf. q21 (opposite order).
SELECT count(*) FROM t
WHERE c4 < 95
  AND c0 < 5;

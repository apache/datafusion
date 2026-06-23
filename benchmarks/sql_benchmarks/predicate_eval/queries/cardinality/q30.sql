-- Hidden: cheap integer compares; `c1 < 5` matches ~5%, the `c0 < 90` family
-- ~90%. k = 2 here. q30..q33 sweep k = 2/4/8/16 with one ~5% predicate written
-- last among ~90% ones.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 5;

-- Two equally cheap compares of unequal selectivity (~95%, ~5%), less selective
-- first. cf. q21.
SELECT count(*) FROM t
WHERE c4 < 95
  AND c0 < 5;

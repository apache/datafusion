-- Hidden: `x_pos` is a copy of `x`, so `x < 20 AND x_pos < 20` still matches
-- ~20% (not the ~4% independence would imply) -- the second predicate removes
-- none of the first's survivors. cf. q70.
SELECT count(*) FROM t
WHERE x < 20
  AND x_pos < 20;

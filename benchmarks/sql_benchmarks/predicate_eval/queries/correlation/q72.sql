-- Hidden: `x_anti` is `99 - x`, so `x < 50 AND x_anti < 50` is empty -- the
-- second predicate removes all of the first's survivors, though each matches
-- ~50% alone. cf. q70.
SELECT count(*) FROM t
WHERE x < 50
  AND x_anti < 50;

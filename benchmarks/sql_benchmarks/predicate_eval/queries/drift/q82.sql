-- Same predicate as q80, over a dataset whose selectivity flips at the halfway
-- point rather than 10% in: `a_sel = 0` matches ~0.1% over the first half of
-- the rows and ~50% over the second, `b_sel = 0` is the mirror. A reorderer
-- that decides once from a short warm-up is right for the first half and wrong
-- for the second. cf. q80/q81 (early flip), q83 (repeated flips).
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

-- k = 8 over the 64-column `ints_wide` table: the same predicates and the same
-- hidden selectivities as q32 (seven ~90% compares followed by one ~5%
-- compare), but every batch carries 64 columns instead of 16. Only the width of
-- the batches being filtered changes, so this isolates the per-conjunct cost of
-- materializing a filtered batch from the cost of evaluating the predicates.
SELECT count(*) FROM t
WHERE c0 < 90
  AND c1 < 90
  AND c2 < 90
  AND c3 < 90
  AND c4 < 90
  AND c5 < 90
  AND c6 < 90
  AND c7 < 5;

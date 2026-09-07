-- Hidden: `regexp_like(s, 'rare')` matches ~0.1% (scans the wide string) while
-- `c0 < 90` matches ~90% (cheap integer compare). Here the expensive predicate
-- is also the selective one and it is written first, so the as-written order is
-- already the best one. cf. q03 (same pair, written the other way round).
SELECT count(*) FROM t
WHERE regexp_like(s, 'rare')
  AND c0 < 90;

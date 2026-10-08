-- q03 the other way round, so the as-written order is already the best one.
SELECT count(*) FROM t
WHERE regexp_like(s, 'rare')
  AND c0 < 90;

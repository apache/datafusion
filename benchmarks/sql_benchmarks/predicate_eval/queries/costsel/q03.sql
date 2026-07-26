-- Hidden: `c0 < 90` matches ~90% (cheap integer compare); `regexp_like(s,
-- 'rare')` matches ~0.1% (scans the wide string). The cheaper predicate is the
-- less selective one.
SELECT count(*) FROM t
WHERE c0 < 90
  AND regexp_like(s, 'rare');

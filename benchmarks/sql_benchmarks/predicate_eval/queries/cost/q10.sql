-- Hidden: both predicates match ~10%, but `regexp_like(s, 'ten')` scans the
-- string (expensive) while `c0 < 10` is a cheap compare. Equal selectivity,
-- unequal cost; expensive one written first. cf. q11 (opposite order).
SELECT count(*) FROM t
WHERE regexp_like(s, 'ten')
  AND c0 < 10;

-- Equal selectivity (~10%), unequal cost: the expensive regexp written first. cf. q11.
SELECT count(*) FROM t
WHERE regexp_like(s, 'ten')
  AND c0 < 10;

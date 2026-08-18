-- Same predicates and hidden selectivities as q01 ('rare' ~0.1% is the
-- selective one, the rest 75-90%), but with 'rare' written first. cf. q01.
SELECT count(*) FROM t
WHERE regexp_like(s, 'rare')
  AND regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd');

-- Five equally expensive regexps of very different selectivity ('rare' ~0.1%, the
-- rest 75-90%), the selective one written last. cf. q02.
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd')
  AND regexp_like(s, 'rare');

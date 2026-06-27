-- Hidden: four regexp scans of about equal cost, all unselective ('aaa' ~90%,
-- 'bbb' ~86%, 'ccc' ~80%, 'ddd' ~75%). Like q60 the predicates are
-- interchangeable, but here each one is expensive.
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd');

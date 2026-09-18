-- q01 with the selective regexp written first.
SELECT count(*) FROM t
WHERE regexp_like(s, 'rare')
  AND regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd');

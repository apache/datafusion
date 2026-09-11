-- Four equally expensive regexps, all unselective (75-90%): nothing to reorder.
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd');

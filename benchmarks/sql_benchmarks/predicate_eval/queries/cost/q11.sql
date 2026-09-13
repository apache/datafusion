-- q10 with the cheap compare written first.
SELECT count(*) FROM t
WHERE c0 < 10
  AND regexp_like(s, 'ten');

-- Redundant proxy: `c0 = 1` implies the s1/s2/s3 regexes, while the marginally
-- identical s4 regex is independent of it. Written proxy-first, regexes grouped.
SELECT count(*) FROM t
WHERE c0 = 1
  AND regexp_like(s1, 'a.a')
  AND regexp_like(s2, 'c.c')
  AND regexp_like(s3, 'd.d')
  AND regexp_like(s4, 'b.b');

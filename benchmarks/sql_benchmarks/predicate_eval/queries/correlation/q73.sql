-- Hidden: `c0 = 1` is a perfect proxy for the s1/s2/s3 regexes -- after the
-- cheap proxy, each of those keeps every survivor while the equally selective
-- (~30%) s4 regex still discards ~70%. The optimal order is [c0, s4, s1/s2/s3]
-- (one informative regex on 30% of rows, the three redundant ones on 9%), but
-- the four regexes are marginally identical -- same width, same marker offset,
-- same cost, same selectivity -- so ranking them takes their *joint*
-- distribution with the proxy. Written with the redundant regexes first,
-- grouped with their proxy, as an author naturally would.
SELECT count(*) FROM t
WHERE c0 = 1
  AND regexp_like(s1, 'a.a')
  AND regexp_like(s2, 'c.c')
  AND regexp_like(s3, 'd.d')
  AND regexp_like(s4, 'b.b');

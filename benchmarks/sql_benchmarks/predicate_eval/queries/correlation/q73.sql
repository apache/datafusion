-- Hidden: `c0 = 1` is a perfect proxy for all three s1 regexes -- after the
-- cheap proxy, each s1 regex keeps every survivor while the equally selective
-- (~30%) s2 regex still discards ~70%. The optimal order is [c0, s2, s1...]
-- (one informative regex on 30% of rows, the three redundant ones on 9%),
-- but marginal statistics cannot tell the four regexes apart in any position:
-- ranking them takes their *joint* distribution with the proxy. Written with
-- the redundant regexes first, grouped with their proxy, as an author
-- naturally would.
SELECT count(*) FROM t
WHERE c0 = 1
  AND regexp_like(s1, 'a.a')
  AND regexp_like(s1, 'c.c')
  AND regexp_like(s1, 'd.d')
  AND regexp_like(s2, 'b.b');

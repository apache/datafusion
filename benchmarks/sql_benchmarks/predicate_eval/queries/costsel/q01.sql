-- Hidden in the data: the five markers have very different selectivities --
-- 'aaa' ~90%, 'bbb' ~86%, 'ccc' ~80%, 'ddd' ~75%, 'rare' ~0.1% -- while every
-- regexp_like costs about the same. 'rare' (most selective) is written last.
-- cf. q02 (most selective written first).
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd')
  AND regexp_like(s, 'rare');

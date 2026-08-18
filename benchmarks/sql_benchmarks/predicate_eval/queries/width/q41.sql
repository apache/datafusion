-- q40 with wide strings: PRED_FILL=30, ~186 chars/row. See q40.
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd')
  AND regexp_like(s, 'rare');

-- Same predicate set and hidden selectivities as costsel/q01 ('rare' ~0.1%, the
-- rest 75-90%); only the string-column width differs across q40/q41/q42. Narrow:
-- PRED_FILL=2, ~12 chars/row.
SELECT count(*) FROM t
WHERE regexp_like(s, 'aaa')
  AND regexp_like(s, 'bbb')
  AND regexp_like(s, 'ccc')
  AND regexp_like(s, 'ddd')
  AND regexp_like(s, 'rare');

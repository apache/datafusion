-- Correlated-proxy dataset: a cheap integer predicate that is a perfect proxy
-- for three string predicates, plus one independent string predicate.
--
--   c0    = 1 for ~30% of rows (cheap proxy)
--   s1    contains 'aaa', 'ccc' and 'ddd' exactly where c0 = 1  (correlated)
--   s2    contains 'bbb' for an independent ~30% of rows        (independent)
--
-- Marginally, the four regex predicates are indistinguishable: similar cost,
-- the same ~30% selectivity. Their *conditional* selectivities behind the
-- proxy differ completely: after `c0 = 1`, the three s1 regexes keep every
-- survivor (each re-tests the proxy's condition) while the s2 regex still
-- discards ~70%. Only joint statistics can see that; an independence
-- assumption prices all four regexes identically in every position.
--
-- PRED_FILL sets the filler width around each marker (a non-matching
-- `regexp_like` must scan the whole value), and PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  CASE WHEN (value * 7) % 100 < 30 THEN 1 ELSE 0 END AS c0,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN (value * 7) % 100 < 30 THEN 'aaa' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN (value * 7) % 100 < 30 THEN 'ccc' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN (value * 7) % 100 < 30 THEN 'ddd' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s1,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN (value * 13) % 100 < 30 THEN 'bbb' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s2
FROM generate_series(1, ${PRED_ROWS:-1000000});

-- Correlated-proxy dataset: a cheap integer predicate that is a perfect proxy
-- for three string predicates, plus one independent string predicate.
--
--   c0          = 1 for ~30% of rows (cheap proxy)
--   s1, s2, s3  each contain a marker exactly where c0 = 1     (correlated)
--   s4          contains a marker for an independent ~30%      (independent)
--
-- The four string columns are deliberately *identical in shape*: same width,
-- the same single marker at the same offset, each matched by an equally cheap
-- regex with the same ~30% marginal selectivity. Marginally the four regex
-- predicates are therefore indistinguishable -- same cost, same selectivity, in
-- every position -- so neither a marginal cost/selectivity estimator nor
-- runtime timing can prefer one over another. Only their *conditional*
-- behaviour behind the proxy differs: after `c0 = 1`, the s1/s2/s3 regexes keep
-- every survivor (each re-tests the proxy's condition) while the s4 regex still
-- discards ~70%. Only joint statistics can see that; an independence assumption
-- prices all four regexes identically in every position.
--
-- PRED_FILL sets the filler width on each side of the marker (a non-matching
-- `regexp_like` must scan the whole value), and PRED_ROWS sizes the table.
CREATE TABLE t AS
WITH base AS (
  SELECT
    -- The cheap proxy and the independent control share one definition each, so
    -- the perfect-proxy / independence invariants can't drift apart silently.
    (value * 7)  % 100 < 30 AS proxy,   -- ~30%, drives c0 and s1/s2/s3
    (value * 13) % 100 < 30 AS indep    -- ~30%, independent of proxy, drives s4
  FROM generate_series(1, ${PRED_ROWS:-1000000})
)
SELECT
  CASE WHEN proxy THEN 1 ELSE 0 END AS c0,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN proxy THEN 'aaa' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s1,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN proxy THEN 'ccc' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s2,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN proxy THEN 'ddd' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s3,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN indep THEN 'bbb' ELSE 'zzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s4
FROM base;

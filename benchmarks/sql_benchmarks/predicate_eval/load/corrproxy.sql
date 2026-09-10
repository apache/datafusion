-- Correlated-proxy dataset:
--   c0          = 1 for ~30% of rows (cheap proxy)
--   s1, s2, s3  each carry a marker exactly where c0 = 1  (correlated)
--   s4          carries a marker for an independent ~30%  (independent)
-- PRED_FILL sets the filler width each side of the marker, PRED_ROWS the row count.
--
-- The four string columns are identical in shape -- same width, same marker
-- offset, same regex cost, same ~30% marginal selectivity -- so marginally they
-- are indistinguishable in every position. Only their joint distribution with the
-- proxy differs: after `c0 = 1` the s1/s2/s3 regexes keep every survivor while s4
-- still discards ~70%. Ranking them therefore takes joint statistics; an
-- independence assumption prices all four regexes identically everywhere.
CREATE TABLE t AS
WITH base AS (
  SELECT
    (value * 7)  % 100 < 30 AS proxy,
    (value * 13) % 100 < 30 AS indep
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

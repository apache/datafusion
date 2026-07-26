-- Mixed-cost dataset: cheap integer columns (`cN < k` ~ k% selectivity)
-- alongside one wide string column carrying three markers matched by expensive
-- `regexp_like`:
--
--   'rare' present in ~0.1% of rows  (value % 1009 = 5)
--   'ten'  present in ~10%  of rows  (value % 10   = 0)
--   'aaa'  present in ~90%  of rows  (value % 10  <> 0)
--
-- This lets a single table mix cheap integer compares with expensive regexp
-- scans at independently chosen selectivities (e.g. a cheap, unselective compare
-- next to an expensive, selective regexp). PRED_FILL is the string-width knob;
-- PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 1)  % 100 AS c0,
  (value * 3)  % 100 AS c1,
  (value * 7)  % 100 AS c2,
  (value * 9)  % 100 AS c3,
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 1009 = 5  THEN 'rare' ELSE 'zzzz' END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 10   = 0  THEN 'ten'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 10  <> 0  THEN 'aaa'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30}) AS s
FROM generate_series(1, ${PRED_ROWS:-1000000});

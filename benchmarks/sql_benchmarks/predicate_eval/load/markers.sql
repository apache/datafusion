-- Wide-string dataset: five markers embedded in `PRED_FILL`-wide filler so that a
-- non-matching `regexp_like` must scan the whole value (every string predicate
-- is "expensive"). Selectivities are coprime so the predicates are independent:
--
--   'aaa'  present in ~90%  of rows  (value % 10  <> 0)
--   'bbb'  present in ~86%  of rows  (value % 7   <> 0)
--   'ccc'  present in ~80%  of rows  (value % 5   <> 0)
--   'ddd'  present in ~75%  of rows  (value % 4   <> 0)
--   'rare' present in ~0.1% of rows  (value % 1009 = 5)   <- the selective one
--
-- PRED_FILL sets the filler width per marker (the string-column width knob: ~6*PRED_FILL
-- chars per row), and PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 10   <> 0 THEN 'aaa'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 7    <> 0 THEN 'bbb'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 5    <> 0 THEN 'ccc'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 4    <> 0 THEN 'ddd'  ELSE 'zzz'  END
    || repeat('q', ${PRED_FILL:-30})
    || CASE WHEN value % 1009 = 5  THEN 'rare' ELSE 'zzzz' END
    || repeat('q', ${PRED_FILL:-30}) AS s
FROM generate_series(1, ${PRED_ROWS:-1000000});

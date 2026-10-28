-- Wide-string dataset: one column `s` holding five markers in PRED_FILL-wide
-- filler, so a non-matching `regexp_like` must scan the whole value. 'aaa' ~90%,
-- 'bbb' ~86%, 'ccc' ~80%, 'ddd' ~75%, 'rare' ~0.1%; the moduli are coprime, so the
-- markers are independent. PRED_FILL is the string-width knob (~6*PRED_FILL chars
-- per row) and PRED_ROWS sizes the table.
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

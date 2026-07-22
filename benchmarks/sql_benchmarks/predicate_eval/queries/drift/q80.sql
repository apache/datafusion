-- The non-obvious property: selectivity changes across the scan. Rows arrive in
-- `seq` order; `a_sel = 0` matches ~0.1% in the first 10% of rows and ~50%
-- after, `b_sel = 0` is the mirror -- so which predicate is more selective flips
-- partway through. cf. q81 (opposite order).
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

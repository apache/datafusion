-- Same drifting predicates as q80 (a_sel/b_sel flip which is more selective
-- partway through the scan), opposite written order. cf. q80.
SELECT count(*) FROM t
WHERE b_sel = 0
  AND a_sel = 0;

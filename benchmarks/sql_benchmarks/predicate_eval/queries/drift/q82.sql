-- Late flip: one stream reads f=00..f=15 in order, so the drift lands halfway
-- through the scan and a warm-up-and-freeze decision is wrong for half the rows.
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

-- Late drift: one stream reads f00..f15 in order, so `a_sel = 0` is the selective
-- conjunct (~0.1% vs ~50%) over the first half of the scan and the unselective one
-- over the second; a warm-up-and-freeze decision is wrong for half the rows.
-- File-backed because CTAS round-robins batches across partitions. cf. q80/q81.
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

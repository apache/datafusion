-- Control: the same cutoff predicate as skip/q01 but over the scrambled
-- dataset, where every row group spans nearly the whole key range, so no row
-- group is ever fully matched and the per-row RowFilter runs everywhere.
-- Measures the overhead of the fully-matched check when it cannot fire; the
-- skip optimization should be performance-neutral here.
SELECT sum(p0)+sum(p1)+sum(p2)+sum(p3)+sum(p4)+sum(p5)+sum(p6)+sum(p7)+sum(p8)+sum(p9)+sum(p10)+sum(p11)+sum(p12)+sum(p13) AS s
FROM t
WHERE skey >= '0000100000';

-- Matches every row: the minimum key is '0000000001', so every row group --
-- including the first -- is fully matched by statistics and the per-row
-- RowFilter is skipped everywhere. The upper bound for the optimization.
SELECT sum(p0)+sum(p1)+sum(p2)+sum(p3)+sum(p4)+sum(p5)+sum(p6)+sum(p7)+sum(p8)+sum(p9)+sum(p10)+sum(p11)+sum(p12)+sum(p13) AS s
FROM t
WHERE skey >= '0000000000';

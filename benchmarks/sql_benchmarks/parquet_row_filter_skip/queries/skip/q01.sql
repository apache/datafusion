-- Low-selectivity range filter on the clustered string key: every row group
-- except the first is fully matched by statistics, so the per-row RowFilter is
-- skipped on those RGs. `skey` is not projected, so the skip also avoids
-- decoding the filter column on the fully-matched run.
SELECT sum(p0)+sum(p1)+sum(p2)+sum(p3)+sum(p4)+sum(p5)+sum(p6)+sum(p7)+sum(p8)+sum(p9)+sum(p10)+sum(p11)+sum(p12)+sum(p13) AS s
FROM t
WHERE skey >= '0000100000';

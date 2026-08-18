-- Same cutoff predicate as q01, but the filter column IS projected: the
-- fully-matched skip still avoids evaluating the per-row filter, but can no
-- longer avoid decoding `skey`. Measures the common case where the win is
-- smaller than q01's best case.
SELECT min(skey) AS first_key,
       sum(p0)+sum(p1)+sum(p2)+sum(p3)+sum(p4)+sum(p5)+sum(p6)+sum(p7)+sum(p8)+sum(p9)+sum(p10)+sum(p11)+sum(p12)+sum(p13) AS s
FROM t
WHERE skey >= '0000100000';

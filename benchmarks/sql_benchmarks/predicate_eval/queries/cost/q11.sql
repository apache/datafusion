-- Same two predicates as q10 (both ~10%; regexp expensive, compare cheap),
-- opposite written order. cf. q10.
SELECT count(*) FROM t
WHERE c0 < 10
  AND regexp_like(s, 'ten');

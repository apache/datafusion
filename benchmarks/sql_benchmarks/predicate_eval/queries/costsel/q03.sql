-- Cheap unselective compare (~90%) then expensive selective regexp (~0.1%). cf. q04.
SELECT count(*) FROM t
WHERE c0 < 90
  AND regexp_like(s, 'rare');

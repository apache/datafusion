-- Same two equally-cheap compares as q20 (`c4 < 95` ~95%, `c0 < 5` ~5%),
-- opposite written order. cf. q20.
SELECT count(*) FROM t
WHERE c0 < 5
  AND c4 < 95;

-- Hidden: four integer compares of equal cost, each ~50% selective. Nothing is
-- selective and the costs are equal, so the predicates are interchangeable.
SELECT count(*) FROM t
WHERE c0 < 50
  AND c1 < 50
  AND c2 < 50
  AND c3 < 50;

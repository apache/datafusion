-- Sixteen independent integer columns, each uniform on [0,100). The predicate
-- `cN < k` therefore has selectivity ~k%. All columns are equally cheap to
-- evaluate, so only selectivity (not cost) distinguishes orderings here. The
-- multipliers are all coprime to 100, which keeps the residues uniform and the
-- columns mutually decorrelated. PRED_ROWS sizes the table.
CREATE TABLE t AS
SELECT
  (value * 1)  % 100 AS c0,
  (value * 3)  % 100 AS c1,
  (value * 7)  % 100 AS c2,
  (value * 9)  % 100 AS c3,
  (value * 11) % 100 AS c4,
  (value * 13) % 100 AS c5,
  (value * 17) % 100 AS c6,
  (value * 19) % 100 AS c7,
  (value * 21) % 100 AS c8,
  (value * 23) % 100 AS c9,
  (value * 27) % 100 AS c10,
  (value * 29) % 100 AS c11,
  (value * 31) % 100 AS c12,
  (value * 33) % 100 AS c13,
  (value * 37) % 100 AS c14,
  (value * 39) % 100 AS c15
FROM generate_series(1, ${PRED_ROWS:-1000000});

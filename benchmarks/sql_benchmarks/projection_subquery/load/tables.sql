-- Two small integer tables for the projected IN / EXISTS subquery benchmarks.
--
-- `outer_t.id` is dense on 1..PSQ_ROWS and `inner_t.id` is the even numbers,
-- so about half of the outer keys have a match. Every 97th inner key is NULL,
-- which keeps three-valued logic in play: a projected `IN` must return NULL,
-- not false, when there is no match and the inner side holds a NULL. That is
-- the reason the decorrelation needs more than one mark join.
--
-- `z` is a low-cardinality column (1000 groups) used by the correlated
-- queries, once with an equality correlation and once with a `<` correlation.
--
-- The tables are deliberately small. The plans under test are quadratic in
-- outer rows times inner rows, so a larger PSQ_ROWS makes the suite take
-- minutes per query instead of seconds.
CREATE TABLE outer_t AS
SELECT
  CAST(value AS INT) AS id,
  CAST(value % 1000 AS INT) AS z
FROM generate_series(1, ${PSQ_ROWS:-30000});

CREATE TABLE inner_t AS
SELECT
  CASE WHEN value % 97 = 0 THEN NULL ELSE CAST(value * 2 AS INT) END AS id,
  CAST(value % 1000 AS INT) AS z
FROM generate_series(1, ${PSQ_ROWS:-30000});

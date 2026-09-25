-- Synthetic tables for the null-aware (NOT IN) hash join benchmarks. Built from
-- `range()`, so there is no data generation step.
--
-- Columns, on both sides:
--   id       Int64 NOT NULL         -- NOT IN over this never needs null-aware handling
--   id_n0    Int64 NULL, 0% NULL    -- nullable, but no NULL is ever present
--   id_n1    Int64 NULL, 1% NULL
--   id_n50   Int64 NULL, 50% NULL
--   z        correlation value for a non-equality correlation (`i.z < o.z`)
--   k        correlation key for an equality correlation (`i.k = o.k`), 16 groups
--
-- `id_n0` is nullable so the planner still builds a null-aware join, but holds no
-- NULL, which is what makes it the zero-NULL baseline for the correlated queries.
-- `id` is only ~half covered by the subquery side, so the anti join returns rows
-- rather than degenerating to an empty or full result.

-- Small tables: used by the correlated and multi-column queries (Q04-Q11), whose cost grows with
-- the product of the two table sizes.
CREATE TABLE small_outer AS
SELECT
  value AS id,
  CASE WHEN value < 0 THEN NULL ELSE value END AS id_n0,
  CASE WHEN value % 100 = 0 THEN NULL ELSE value END AS id_n1,
  CASE WHEN value % 2 = 0 THEN NULL ELSE value END AS id_n50,
  value % 1000 AS z,
  value % 16 AS k
FROM range(0, ${NAJ_ROWS:-10000});

CREATE TABLE small_inner AS
SELECT
  value * 2 AS id,
  CASE WHEN value < 0 THEN NULL ELSE value * 2 END AS id_n0,
  CASE WHEN value % 100 = 0 THEN NULL ELSE value * 2 END AS id_n1,
  CASE WHEN value % 2 = 0 THEN NULL ELSE value * 2 END AS id_n50,
  value % 1000 AS z,
  value % 16 AS k
FROM range(0, ${NAJ_ROWS:-10000});

-- Large tables: used by the uncorrelated queries (Q01-Q03), whose cost is linear
-- in the table size.
CREATE TABLE large_outer AS
SELECT
  value AS id,
  CASE WHEN value < 0 THEN NULL ELSE value END AS id_n0,
  CASE WHEN value % 100 = 0 THEN NULL ELSE value END AS id_n1,
  CASE WHEN value % 2 = 0 THEN NULL ELSE value END AS id_n50
FROM range(0, ${NAJ_LARGE_ROWS:-1000000});

CREATE TABLE large_inner AS
SELECT
  value * 2 AS id,
  CASE WHEN value < 0 THEN NULL ELSE value * 2 END AS id_n0,
  CASE WHEN value % 100 = 0 THEN NULL ELSE value * 2 END AS id_n1,
  CASE WHEN value % 2 = 0 THEN NULL ELSE value * 2 END AS id_n50
FROM range(0, ${NAJ_LARGE_ROWS:-1000000});

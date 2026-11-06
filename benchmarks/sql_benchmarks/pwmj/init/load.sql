-- Builds the two in-memory join inputs. `load` runs before `init` and is not
-- timed, so generating the keys never enters the measurement.
--
-- Keys are scrambled with a fixed multiplicative hash rather than an RNG, so
-- every run and every branch sees the same data. The multiplier is odd and not
-- a multiple of 5, so `lhs.key` is a permutation of 1..=100000.
--
-- Parameters, supplied by pwmj.benchmark.template:
--   RIGHT_ROWS  streamed row count
--   RIGHT_KEY   streamed key expression over `value` and `h`
DROP TABLE IF EXISTS lhs;

DROP TABLE IF EXISTS rhs;

CREATE TABLE lhs AS
SELECT CAST(1 + (value * 2654435761) % 100000 AS INT) AS key,
       CAST(value AS INT) AS payload
FROM range(100000);

CREATE TABLE rhs AS
SELECT CAST(${RIGHT_KEY} AS INT) AS key,
       CAST(value AS INT) AS payload
FROM (SELECT value, (value * 2654435761) % 2000000 AS h FROM range(${RIGHT_ROWS})) t;

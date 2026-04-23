-- ============================================================================
-- Snowflake LATERAL FLATTEN validation queries
--
-- Run this file against a real Snowflake instance to verify that the
-- Unparser-generated SQL is syntactically and semantically correct.
--
-- Each section shows:
--   1. The DataFusion input (SQL parsed by the planner)
--   2. The Snowflake SQL produced by the Unparser
--
-- NOTE: The Unparser emits array literals as [1, 2, 3] (DataFusion syntax).
-- Snowflake requires ARRAY_CONSTRUCT(1, 2, 3). The queries below use
-- ARRAY_CONSTRUCT so they can run directly on Snowflake. The exact Unparser
-- output is shown in the "Unparser output:" comment above each query.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Setup: create and seed test tables
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE source (
    items ARRAY
);

INSERT INTO source SELECT PARSE_JSON('[1, 2, 3]');
INSERT INTO source SELECT PARSE_JSON('["a", "b"]');
INSERT INTO source SELECT NULL;

CREATE OR REPLACE TABLE unnest_table (
    array_col ARRAY
);

INSERT INTO unnest_table SELECT PARSE_JSON('[10, 20, 30]');
INSERT INTO unnest_table SELECT PARSE_JSON('[40, 50]');
INSERT INTO unnest_table SELECT NULL;

CREATE OR REPLACE TABLE multi_array_table (
    column_a ARRAY,
    column_b ARRAY
);

INSERT INTO multi_array_table SELECT PARSE_JSON('[1, 2, 3]'), PARSE_JSON('["x", "y"]');
INSERT INTO multi_array_table SELECT PARSE_JSON('[4]'),       PARSE_JSON('["z"]');

-- ============================================================================
-- Roundtrip tests: SQL parsed → plan → Snowflake SQL
-- ============================================================================

-- --------------------------------------------------------------------------
-- Test: snowflake_unnest_to_lateral_flatten_simple
-- DataFusion input: SELECT * FROM UNNEST([1,2,3])
-- Unparser output:  SELECT "_unnest_1"."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE"
FROM LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_implicit_from
-- DataFusion input: SELECT UNNEST([1,2,3])
-- Unparser output:  SELECT "_unnest_1"."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE"
FROM LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_string_array
-- DataFusion input: SELECT * FROM UNNEST(['a','b','c'])
-- Unparser output:  SELECT "_unnest_1"."VALUE" FROM LATERAL FLATTEN(INPUT => ['a', 'b', 'c']) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE"
FROM LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT('a', 'b', 'c')) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_select_unnest_with_alias
-- DataFusion input: SELECT UNNEST([1,2,3]) as c1
-- Unparser output:  SELECT "_unnest_1"."VALUE" AS "c1" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE" AS "c1"
FROM LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_from_unnest_with_table_alias
-- DataFusion input: SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)
-- Unparser output:  SELECT "t1"."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS "t1"
-- --------------------------------------------------------------------------
SELECT "t1"."VALUE"
FROM LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)) AS "t1";

-- ============================================================================
-- Plan-built tests: LogicalPlan → Snowflake SQL
-- These use a table called "source" with an ARRAY column "items".
-- ============================================================================

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_limit_between_projection_and_unnest
-- Plan: Projection → Limit → Unnest → Projection → TableScan
-- Unparser output: SELECT "_unnest_1"."VALUE" AS "item" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1" LIMIT 5
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE" AS "item"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
LIMIT 5;

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_sort_between_projection_and_unnest
-- Plan: Projection → Sort → Unnest → Projection → TableScan
-- Unparser output: SELECT "_unnest_1"."VALUE" AS "item" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1" ORDER BY "_unnest_1"."VALUE" ASC NULLS FIRST
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE" AS "item"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
ORDER BY "_unnest_1"."VALUE" ASC NULLS FIRST;

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_limit_between_projection_and_unnest_with_subquery_alias
-- Plan: Projection → Limit → Unnest → SubqueryAlias → Projection → TableScan
-- Unparser output: SELECT "_unnest_1"."VALUE" AS "item" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1" LIMIT 10
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE" AS "item"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
LIMIT 10;

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_composed_expression_wrapping_unnest
-- Plan: Projection(CAST(placeholder AS Int64)) → Unnest → Projection → TableScan
-- Unparser output: SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "item_id" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "item_id"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_composed_expression_with_limit
-- Plan: Projection(CAST) → Limit → Unnest → Projection → TableScan
-- Unparser output: SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "item_id" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1" LIMIT 5
-- --------------------------------------------------------------------------
SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "item_id"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
LIMIT 5;

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_multi_expression_projection
-- Plan: Projection([CAST AS Int64, CAST AS Utf8]) → Unnest → Projection → TableScan
-- Unparser output: SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "a", CAST("_unnest_1"."VALUE" AS VARCHAR) AS "b" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "a",
       CAST("_unnest_1"."VALUE" AS VARCHAR) AS "b"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_multi_expression_with_limit
-- Plan: Projection([CAST, CAST]) → Limit → Unnest → Projection → TableScan
-- Unparser output: SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "a", CAST("_unnest_1"."VALUE" AS VARCHAR) AS "b" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1" LIMIT 10
-- --------------------------------------------------------------------------
SELECT CAST("_unnest_1"."VALUE" AS BIGINT) AS "a",
       CAST("_unnest_1"."VALUE" AS VARCHAR) AS "b"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
LIMIT 10;

-- --------------------------------------------------------------------------
-- Test: snowflake_unnest_through_subquery_alias
-- Plan: Projection → Unnest → SubqueryAlias → Projection → TableScan
-- Unparser output: SELECT "_unnest_1"."VALUE" AS "item" FROM "source" CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE" AS "item"
FROM "source"
CROSS JOIN LATERAL FLATTEN(INPUT => "source"."items", OUTER => true) AS "_unnest_1";

-- ============================================================================
-- Roundtrip tests with table columns
-- ============================================================================

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_unnest_from_subselect
-- DataFusion input: SELECT UNNEST(array_col) FROM (SELECT array_col FROM unnest_table WHERE array_col IS NOT NULL LIMIT 3)
-- Unparser output:  SELECT "_unnest_1"."VALUE" FROM (SELECT "unnest_table"."array_col" FROM "unnest_table" WHERE "unnest_table"."array_col" IS NOT NULL LIMIT 3) CROSS JOIN LATERAL FLATTEN(INPUT => "unnest_table"."array_col") AS "_unnest_1"
-- --------------------------------------------------------------------------
SELECT "_unnest_1"."VALUE"
FROM (
    SELECT "unnest_table"."array_col"
    FROM "unnest_table"
    WHERE "unnest_table"."array_col" IS NOT NULL
    LIMIT 3
) CROSS JOIN LATERAL FLATTEN(INPUT => "unnest_table"."array_col") AS "_unnest_1";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_cross_join_unnest_table_column
-- DataFusion input: SELECT * FROM multi_array_table CROSS JOIN UNNEST(column_a) AS a (a)
-- Unparser output:  SELECT "multi_array_table"."column_a", "multi_array_table"."column_b", "a"."VALUE" FROM "multi_array_table" CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_a") AS "a"
-- --------------------------------------------------------------------------
SELECT "multi_array_table"."column_a",
       "multi_array_table"."column_b",
       "a"."VALUE"
FROM "multi_array_table"
CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_a") AS "a";

-- --------------------------------------------------------------------------
-- Test: snowflake_flatten_multiple_unnest_cross_join
-- DataFusion input: SELECT a.a, b.b FROM multi_array_table
--                   CROSS JOIN UNNEST(column_a) AS a (a)
--                   CROSS JOIN UNNEST(column_b) AS b (b)
-- Unparser output:  SELECT "a"."VALUE", "b"."VALUE" FROM "multi_array_table" CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_a") AS "a" CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_b") AS "b"
-- --------------------------------------------------------------------------
SELECT "a"."VALUE",
       "b"."VALUE"
FROM "multi_array_table"
CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_a") AS "a"
CROSS JOIN LATERAL FLATTEN(INPUT => "multi_array_table"."column_b") AS "b";

-- ============================================================================
-- Cleanup
-- ============================================================================
-- DROP TABLE IF EXISTS source;
-- DROP TABLE IF EXISTS unnest_table;
-- DROP TABLE IF EXISTS multi_array_table;

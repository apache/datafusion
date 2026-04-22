-- Inexact path: wide projection (all columns) + DESC LIMIT.
-- Shows the row-level filter benefit: with a tight threshold from the
-- first RG, subsequent RGs skip decoding non-sort columns for filtered
-- rows — bigger wins for wide tables.
SELECT *
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

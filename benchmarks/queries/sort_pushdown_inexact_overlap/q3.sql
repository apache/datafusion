-- Overlapping RGs: wide projection + DESC LIMIT.
-- Row-level filter benefit: tight threshold skips decoding non-sort columns.
SELECT *
FROM lineitem
ORDER BY l_orderkey DESC
LIMIT 100

-- Sort 1M rows by a shuffled key with an all-distinct 64-byte string payload.
-- Spill compaction cannot remove data here, so this measures its cost.
SELECT id, s FROM t ORDER BY id;

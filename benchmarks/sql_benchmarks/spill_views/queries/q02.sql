-- Sort 1M rows by a shuffled key with a 64-byte string payload that has 1000
-- distinct values (a 64 KB dictionary buffer shared by all views).
SELECT id, s FROM t ORDER BY id;

-- Sort 1M rows by a shuffled key with a 64-byte string payload that has one
-- distinct value. The sort spills, and each spilled batch holds views into
-- the shared dictionary buffer.
SELECT id, s FROM t ORDER BY id;

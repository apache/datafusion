-- GROUP BY an all-distinct 64-byte string key. The final aggregation spills
-- its group keys, and the keys in each spilled batch are distinct, so spill
-- compaction cannot remove data here: this measures its cost.
SELECT s, count(*) AS c FROM t GROUP BY s;

-- Same predicate again, over a table whose skew is *per partition* rather than
-- over time: 16 Parquet files, half favouring `a_sel = 0` (~0.1% against
-- `b_sel = 0` at ~50%) and half the mirror, handed to the scan one whole file
-- per partition, so every stream sees a single fixed profile for its whole life
-- (see load/drift_files.sql). A pooled warm-up mixes the two profiles and
-- settles one order for all 16 partitions -- right for half of them, backwards
-- for the other half -- and re-sampling a shared decision does not fix that;
-- only a per-stream decision does.
--
-- Files, not `CREATE TABLE ... AS SELECT`: CTAS fans a `generate_series` scan
-- out with `RoundRobinBatch`, which deals whole *batches* round-robin, so any
-- contiguous block of rows lands in every partition. The load script pins
-- `target_partitions` to the file count and turns off `repartition_file_scans`,
-- without which the groups are re-derived as byte ranges and both profiles end
-- up in one stream again. cf. q80/q81 (one early flip), q82 (one late flip).
SELECT count(*) FROM t
WHERE a_sel = 0
  AND b_sel = 0;

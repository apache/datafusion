-- Split drift dataset, shared by q82 and q83: 16 Parquet files of PRED_ROWS / 16
-- rows each. f00..f07 carry profile A (`a_sel = 0` selective at ~0.1%, `b_sel = 0`
-- unselective at ~50%), f08..f15 the mirror, at the rates q80/q81 use.
-- `FileGroup::split_files` sorts a group's files by path, so `target_partitions =
-- 1` reads f00..f15 in order and meets the flip halfway through the scan (q82),
-- while `= 16` hands every stream one whole file, i.e. one fixed profile (q83).
-- Each query sets that itself; `repartition_file_scans` stays off for both, or the
-- groups are re-derived as byte ranges and both profiles land in one stream again.

-- f00..f07: profile A -- `a_sel = 0` selective, `b_sel = 0` unselective.

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f00.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f01.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f02.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f03.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f04.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f05.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f06.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 1000 AS a_sel, value % 2    AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f07.parquet'
STORED AS PARQUET;

-- f08..f15: profile B -- the mirror.

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f08.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f09.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f10.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f11.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f12.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f13.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f14.parquet'
STORED AS PARQUET;

COPY (SELECT value AS seq, value % 2    AS a_sel, value % 1000 AS b_sel
      FROM generate_series(1, ${PRED_ROWS:-1000000} / 16) ORDER BY value)
TO 'sql_benchmarks/predicate_eval/scratch/drift_split/f15.parquet'
STORED AS PARQUET;

set datafusion.optimizer.repartition_file_scans = false;

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/predicate_eval/scratch/drift_split/';

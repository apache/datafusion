-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

-- A grouped COUNT(DISTINCT <string>) beside a non-distinct COUNT(*). No query in
-- the standard suite has this shape: q22 is the only one that pairs a lone
-- distinct aggregate with a non-distinct count, and its distinct argument is an
-- Int64, which has a specialized GroupsAccumulator. A string argument has none,
-- so one boxed Accumulator, and one hash table, is built for every group.
SELECT "SearchPhrase", COUNT(*) AS c, COUNT(DISTINCT "MobilePhoneModel") AS models FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;

-- Q25: LEFT MARK 1M x 10M | 1:10 | 50%
WITH t1_sorted AS (
    SELECT value % 100000 as key, value as data
FROM range(1000000)
ORDER BY key, data
    ),
    t2_sorted AS (
SELECT value % 100000 as key, value as data
FROM range(10000000)
ORDER BY key, data
    )
SELECT t1_sorted.key, t1_sorted.data
FROM t1_sorted
WHERE t1_sorted.data < 0
   OR EXISTS (
    SELECT 1 FROM t2_sorted
    WHERE t2_sorted.key = t1_sorted.key
      AND t2_sorted.data <> t1_sorted.data
      AND t2_sorted.data % 2 = 0
)
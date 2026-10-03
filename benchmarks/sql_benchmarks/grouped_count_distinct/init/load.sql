CREATE TABLE grouped_distinct AS
SELECT value % 2000 AS g,
       value % 8 AS few_g,
       (value * 48271) % 999983 AS x,
       (value / 2000) % 8 AS low_x,
       arrow_cast((value * 48271) % 999983, 'UInt64') AS unsigned_x,
       CAST((value * 48271) % 999983 AS VARCHAR) AS string_x,
       CAST((value * 48271) % 999983 AS DOUBLE) AS float_x
FROM range(${GROUPED_DISTINCT_ROWS:-4000000});

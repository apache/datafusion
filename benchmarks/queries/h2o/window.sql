-- Basic Window
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER () AS window_basic
FROM large;

-- Sorted Window
SELECT 
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (ORDER BY id3) AS first_order_by,
    row_number() OVER (ORDER BY id3) AS row_number_order_by
FROM large;

-- PARTITION BY
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id1) AS sum_by_id1,
    sum(v2) OVER (PARTITION BY id2) AS sum_by_id2,
    sum(v2) OVER (PARTITION BY id3) AS sum_by_id3
FROM large;

-- PARTITION BY ORDER BY
SELECT 
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (PARTITION BY id2 ORDER BY id3) AS first_by_id2_ordered_by_id3
FROM large;

-- Lead and Lag
SELECT 
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (ORDER BY id3 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS my_lag,
    first_value(v2) OVER (ORDER BY id3 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS my_lead
FROM large;

-- Moving Averages
SELECT 
    id1,
    id2,
    id3,
    v2,
    avg(v2) OVER (ORDER BY id3 ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS my_moving_average
FROM large;

-- Rolling Sum
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (ORDER BY id3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS my_rolling_sum
FROM large;

-- RANGE BETWEEN
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (ORDER BY v2 RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS my_range_between
FROM large;

-- First PARTITION BY ROWS BETWEEN
SELECT 
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS my_lag_by_id2,
    first_value(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS my_lead_by_id2
FROM large;

-- Moving Averages PARTITION BY
SELECT 
    id1,
    id2,
    id3,
    v2,
    avg(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS my_moving_average_by_id2
FROM large;

-- Rolling Sum PARTITION BY
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS my_rolling_sum_by_id2
FROM large;

-- RANGE BETWEEN PARTITION BY
SELECT 
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id2 ORDER BY v2 RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS my_range_between_by_id2
FROM large;
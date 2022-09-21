SELECT
    SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as summation1,
        SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) as summation2,
        SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) as summation3,
        SUM(c2) OVER(ORDER BY c5 RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) as summation4,
        SUM(c2) OVER(ORDER BY c5 RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) as summation5
FROM test;

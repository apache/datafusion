SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1;

SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2;

SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3;

SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4;

SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6;

SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5;

SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3;

SELECT id6, largest2_v3 FROM (SELECT id6, v3 AS largest2_v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2;

SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4;

SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6;
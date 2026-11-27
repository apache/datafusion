-- Inexact path: TopK + DESC LIMIT with a selective WHERE clause.
-- Each range matches about one 20K-row page, so the page index installs
-- a row selection on every surviving RG. After the first RGs, the TopK
-- dynamic filter can prune the remaining RGs by statistics.
-- The ranges use OR, not IN: the page index skips IN lists with more
-- than 20 values, and 20 keys give each RG fewer rows than the LIMIT.
SELECT l_orderkey, l_partkey, l_suppkey
FROM lineitem
WHERE l_orderkey BETWEEN 0 AND 5000
   OR l_orderkey BETWEEN 200000 AND 205000
   OR l_orderkey BETWEEN 400000 AND 405000
   OR l_orderkey BETWEEN 600000 AND 605000
   OR l_orderkey BETWEEN 800000 AND 805000
   OR l_orderkey BETWEEN 1000000 AND 1005000
   OR l_orderkey BETWEEN 1200000 AND 1205000
   OR l_orderkey BETWEEN 1400000 AND 1405000
   OR l_orderkey BETWEEN 1600000 AND 1605000
   OR l_orderkey BETWEEN 1800000 AND 1805000
   OR l_orderkey BETWEEN 2000000 AND 2005000
   OR l_orderkey BETWEEN 2200000 AND 2205000
   OR l_orderkey BETWEEN 2400000 AND 2405000
   OR l_orderkey BETWEEN 2600000 AND 2605000
   OR l_orderkey BETWEEN 2800000 AND 2805000
   OR l_orderkey BETWEEN 3000000 AND 3005000
   OR l_orderkey BETWEEN 3200000 AND 3205000
   OR l_orderkey BETWEEN 3400000 AND 3405000
   OR l_orderkey BETWEEN 3600000 AND 3605000
   OR l_orderkey BETWEEN 3800000 AND 3805000
   OR l_orderkey BETWEEN 4000000 AND 4005000
   OR l_orderkey BETWEEN 4200000 AND 4205000
   OR l_orderkey BETWEEN 4400000 AND 4405000
   OR l_orderkey BETWEEN 4600000 AND 4605000
   OR l_orderkey BETWEEN 4800000 AND 4805000
   OR l_orderkey BETWEEN 5000000 AND 5005000
   OR l_orderkey BETWEEN 5200000 AND 5205000
   OR l_orderkey BETWEEN 5400000 AND 5405000
   OR l_orderkey BETWEEN 5600000 AND 5605000
   OR l_orderkey BETWEEN 5800000 AND 5805000
ORDER BY l_orderkey DESC
LIMIT 10

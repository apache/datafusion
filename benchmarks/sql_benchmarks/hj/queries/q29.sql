-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Q29: 3M build rows, 30K distinct 128-byte string keys, 100 rows per key;
-- 10K probes, 1% hit rate, 10K output pairs.
-- 
-- Exercising expensive build, cheap probe.
SELECT count(*) AS match_count
FROM hj_large_string_build b
JOIN (
  SELECT repeat('x', 120) || lpad(CAST(
    CASE WHEN value % 100 = 0 THEN value * 2 ELSE value * 2 + 1 END
    AS VARCHAR), 8, '0') AS k
  FROM range(10000)
) p ON b.k = p.k;

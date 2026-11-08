-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Set this before CTAS as well as before planning the measured query.
SET datafusion.execution.target_partitions = 1;

-- Unique IDs also make every ORDER BY deterministic when used as the last key.
CREATE TABLE bench AS
SELECT value AS id,
       CASE WHEN value % 10 = 0 THEN NULL ELSE value % 100 END AS nullable_key,
       concat(CAST(value AS VARCHAR), repeat('x', ${STAGED_SORT_PAYLOAD_WIDTH:-32})) AS payload,
       0 AS tie_key,
       CASE WHEN value % 10 = 0 THEN 0 ELSE value END AS sparse_key
FROM (SELECT ${STAGED_SORT_ROWS:-200000} - 1 - value AS value
      FROM range(${STAGED_SORT_ROWS:-200000}));

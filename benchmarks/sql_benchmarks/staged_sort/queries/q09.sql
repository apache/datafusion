-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Issue #25033: unique scores resolve ordering without the expensive second key.
-- Map the shared fixture to the issue's events table, preserving its query.
WITH events AS (
    SELECT id, id AS score, payload FROM bench
)
SELECT id
FROM events
ORDER BY score DESC, md5(payload)
LIMIT 10;

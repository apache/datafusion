-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Same as q09, without LIMIT: full sort on unique descending scores.
-- The expensive second key is unnecessary when scores resolve the order.
WITH events AS (
    SELECT id, id AS score, payload FROM bench
)
SELECT id
FROM events
ORDER BY score DESC, md5(payload);

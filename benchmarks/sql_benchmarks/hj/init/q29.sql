-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

CREATE TABLE hj_large_string_build AS
SELECT repeat('x', 120) || lpad(CAST((value % 30000) * 2 AS VARCHAR), 8, '0') AS k
FROM range(3000000);

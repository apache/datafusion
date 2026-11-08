-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Unique cheap prefix: expensive suffix can be skipped.
SELECT * FROM bench ORDER BY id, lower(payload), md5(payload);

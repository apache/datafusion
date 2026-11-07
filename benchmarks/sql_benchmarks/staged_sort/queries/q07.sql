-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- TopK uses the eager fetch path, not the full staged sort.
SELECT * FROM bench ORDER BY id % 100, md5(payload), id LIMIT 100;

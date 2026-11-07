-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Sparse ties reach lower; unique payloads resolve them before md5.
SELECT * FROM bench ORDER BY sparse_key, lower(payload), md5(payload), id;

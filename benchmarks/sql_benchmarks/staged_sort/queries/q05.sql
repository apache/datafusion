-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- NULLs form a tied prefix group; descending direction and NULLS FIRST.
SELECT * FROM bench ORDER BY nullable_key DESC NULLS FIRST, md5(payload), id;

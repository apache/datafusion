-- SPDX-License-Identifier: Apache-2.0
-- SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
-- SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

-- Same keys as q02, but threshold zero creates multiple sorted runs.
SELECT * FROM bench ORDER BY sparse_key, md5(payload), id;

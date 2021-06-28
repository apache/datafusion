-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at

-- http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT
  c9,
  row_number() OVER (PARTITION BY c2, c9) AS row_number,
  count(c3) OVER (PARTITION BY c2) AS count_c3,
  avg(c3) OVER (PARTITION BY c2) AS avg_c3_by_c2,
  sum(c3) OVER (PARTITION BY c2) AS sum_c3_by_c2,
  max(c3) OVER (PARTITION BY c2) AS max_c3_by_c2,
  min(c3) OVER (PARTITION BY c2) AS min_c3_by_c2
FROM test
ORDER BY c9;

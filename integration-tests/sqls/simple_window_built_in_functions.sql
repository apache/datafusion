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
  row_number() OVER (ORDER BY c9) row_num,
  lead(c9) OVER (ORDER BY c9) lead_c9,
  lag(c9) OVER (ORDER BY c9) lag_c9,
  first_value(c9) OVER (ORDER BY c9) first_c9,
  first_value(c9) OVER (ORDER BY c9 DESC) first_c9_desc,
  last_value(c9) OVER (ORDER BY c9) last_c9,
  last_value(c9) OVER (ORDER BY c9 DESC) last_c9_desc,
  nth_value(c9, 2) OVER (ORDER BY c9) second_c9,
  nth_value(c9, 2) OVER (ORDER BY c9 DESC) second_c9_desc
FROM test
ORDER BY c9;

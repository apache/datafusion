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
  c8,
  LEAD(c8) OVER () next_c8,
  LEAD(c8, 10, 10) OVER() next_10_c8,
  LEAD(c8, 100, 10) OVER() next_out_of_bounds_c8,
  LAG(c8) OVER() prev_c8,
  LAG(c8, -2, 0) OVER() AS prev_2_c8,
  LAG(c8, -200, 10) OVER() AS prev_out_of_bounds_c8

FROM test
ORDER BY c8;

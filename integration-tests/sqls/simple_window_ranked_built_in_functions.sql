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

select
  c9,
  cume_dist() OVER (PARTITION BY c2 ORDER BY c3) cume_dist_by_c3,
  rank() OVER (PARTITION BY c2 ORDER BY c3) rank_by_c3,
  dense_rank() OVER (PARTITION BY c2 ORDER BY c3) dense_rank_by_c3,
  percent_rank() OVER (PARTITION BY c2 ORDER BY c3) percent_rank_by_c3
FROM test
ORDER BY c9;

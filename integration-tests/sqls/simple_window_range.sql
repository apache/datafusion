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
    SUM(c5) OVER(ORDER BY c4 RANGE BETWEEN 3 PRECEDING AND 1 FOLLOWING) as summation2,
    SUM(c4) OVER(ORDER BY c3 RANGE 3 PRECEDING) as summation3,
    SUM(c4) OVER(ORDER BY c5 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation6,
    SUM(c4) OVER(ORDER BY c5 RANGE UNBOUNDED PRECEDING) as summation7,
    SUM(c2) OVER(PARTITION BY c5 ORDER BY c5 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation10,
    SUM(c4) OVER(PARTITION BY c1 ORDER BY c5 RANGE UNBOUNDED PRECEDING) as summation11,
    SUM(c2) OVER(PARTITION BY c1 ORDER BY c5 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation14,
    SUM(c4) OVER(PARTITION BY c5 ORDER BY c5 RANGE UNBOUNDED PRECEDING) as summation15,
    SUM(c2) OVER(PARTITION BY c5, c7, c9 ORDER BY c5 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation20,
    SUM(c2) OVER(PARTITION BY c5 ORDER BY c5 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as summation21
FROM test
ORDER BY c9;

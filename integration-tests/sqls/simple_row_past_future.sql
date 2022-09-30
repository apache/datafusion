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
    SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as summation1,
        SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) as summation2,
        SUM(c2) OVER(ORDER BY c5 ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) as summation3,
        SUM(c2) OVER(ORDER BY c5 RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) as summation4,
        SUM(c2) OVER(ORDER BY c5 RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) as summation5
FROM test
ORDER BY c9;

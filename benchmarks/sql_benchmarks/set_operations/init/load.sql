--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


-- Materialize input data outside the timed query. Both columns participate
-- in the set operation; the string payload is a deterministic function of key.
DROP TABLE IF EXISTS set_left;
DROP TABLE IF EXISTS set_right;

CREATE TABLE set_left AS
SELECT key, concat('v', key) AS payload
FROM (SELECT CAST(${LEFT_KEY} AS BIGINT) AS key FROM range(${SET_ROWS:-1000000})) t;

CREATE TABLE set_right AS
SELECT key, concat('v', key) AS payload
FROM (SELECT CAST(${RIGHT_KEY} AS BIGINT) AS key FROM range(${RIGHT_ROWS})) t;

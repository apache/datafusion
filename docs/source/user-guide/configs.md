<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Configuration Settings

The following configuration options can be passed to `SessionConfig` to control various aspects of query execution.

| key                                     | type    | default | description                                                                                                                                                                                                                                                     |
| --------------------------------------- | ------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| datafusion.optimizer.filterNullJoinKeys | Boolean | false   | When set to true, the optimizer will insert filters before a join between a nullable and non-nullable column to filter out nulls on the nullable side. This filter can add additional overhead when the file format does not fully support predicate push down. |

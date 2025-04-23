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

# Extending DataFusion's operators: custom LogicalPlan and Execution Plans
This module contains an end to end demonstration of creatinga user defined operator in DataFusion.12 .Specifically, it shows how to define a `TopKNode` that implements `ExtensionPlanNode` that add an OptimizerRule to rewrite a `LogicalPlan` to use that node as a `LogicalPlan`, create an `ExecutionPlan` and finally produce results.

## TopK Background:
A "Top K" node is a common query optimization which is used for queries such as "find the top 3 customers by revenue". The(simplified) SQL for such a query might be:

 ```sql
CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
   STORED AS CSV location 'tests/data/customer.csv';
 SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
```
 And a naive plan would be:
 ```
> explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
 +--------------+----------------------------------------+
 | plan_type    | plan                                   |
 +--------------+----------------------------------------+
 | logical_plan | Limit: 3                               |
 |              |   Sort: revenue DESC NULLS FIRST       |
 |              |     Projection: customer_id, revenue   |
 |              |       TableScan: sales                 |
 +--------------+----------------------------------------+
 ```
While this plan produces the correct answer, the careful reader will note it fully sorts the input before discarding everythingother than the top 3 elements.
The same answer can be produced by simply keeping track of the top N elements, reducing the total amount of required buffer memory.


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

# Join Physical Plans

Currently Apache Datafusion supports the following join algorithms:
 - Nested Loop Join
 - Sort Merge Join
 - Hash Join
 - Symmetric Hash Join 
 - Piecewise Merge Join (experimental) 

The physical planner will choose the appropriate algorithm based on the statistics + join 
condition of the two tables.

# Join Algorithm Optimizer Configurations

You can modify join optimization behavior in your queries by setting specific configuration values.
Use the following command to update a configuration:

```
set datafusion.optimizer.<configuration_name>
```

Adjusting the following configuration values influences how the optimizer selects the join algorithm 
used to execute your SQL query:

## Join Optimizer Configurations

Adjusting the following configuration values influences how the optimizer selects the join algorithm
used to execute your SQL query.

### allow_symmetric_joins_without_pruning (bool, default = true)
Controls whether symmetric hash joins are allowed for unbounded data sources even when their inputs
lack ordering or filtering.
 - If disabled, the `SymmetricHashJoin` operator cannot prune its internal buffers to be produced only at the end of execution.

### prefer_hash_join (bool, default = true)
Determines whether the optimizer prefers Hash Join over Sort Merge Join during physical plan selection.
 - true: favors HashJoin for faster execution when sufficient memory is available.
 - false: allows SortMergeJoin to be chosen when more memory-efficient execution is needed.

### enable_piecewise_merge_join (bool, default = false)
Enables the experimental Piecewise Merge Join algorithm.
 - When enabled, the physical planner may select PiecewiseMergeJoin if there is exactly one range 
   filter in the join condition.
 - Piecewise Merge Join is faster than Nested Loop Join performance wise for single range filter 
   except for cases where it is joining two large tables (num_rows > 100,000) that are approximately 
   equal in size.

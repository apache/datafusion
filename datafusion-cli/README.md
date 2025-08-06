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

<!-- Note this file is included in the crates.io page as well https://crates.io/crates/datafusion-cli -->

# DataFusion Command-line Interface

[DataFusion](https://datafusion.apache.org/) is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

DataFusion CLI (`datafusion-cli`) is a small command line utility that runs SQL queries using the DataFusion engine.

# Frequently Asked Questions

## Where can I find more information?

See the [`datafusion-cli` documentation](https://datafusion.apache.org/user-guide/cli/index.html) for further information.

## Memory Profiling

Enable memory tracking for the next query and display the report afterwards:

```text
> \memory_profiling on
Memory profiling enabled for next query
> SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v  FROM generate_series(1,100000) AS t(v)  GROUP BY group_key  ORDER BY group_key;

+-----------+------+----------+
| group_key | cnt  | sum_v    |
+-----------+------+----------+
| 0         | 1000 | 50050000 |
| 1         | 1000 | 49951000 |
| 2         | 1000 | 49952000 |
...

\memory_profiling show

📊 Enhanced Memory Analysis:
🔍 Top Memory Consumers:
  1. ExternalSorterMerge[8]: 20.00 MB (9.8%) [Sorting]
  2. ExternalSorterMerge[2]: 20.00 MB (9.8%) [Sorting]
  3. ExternalSorterMerge[0]: 20.00 MB (9.8%) [Sorting]
  4. ExternalSorterMerge[7]: 20.00 MB (9.8%) [Sorting]
  5. ExternalSorterMerge[4]: 20.00 MB (9.8%) [Sorting]
  6. ExternalSorterMerge[9]: 20.00 MB (9.8%) [Sorting]
  7. ExternalSorterMerge[3]: 20.00 MB (9.8%) [Sorting]
  8. ExternalSorterMerge[1]: 20.00 MB (9.8%) [Sorting]
  9. ExternalSorterMerge[5]: 20.00 MB (9.8%) [Sorting]
  10. ExternalSorterMerge[6]: 20.00 MB (9.8%) [Sorting]

📈 Memory Summary:
  Peak memory usage: 20.00 MB
  Total tracked memory: 203.07 MB

🎯 Memory by Category:
  Other: 1.51 MB (0.7%)
  Aggregation: 1.49 MB (0.7%)
  Sorting: 200.07 MB (98.5%)


\memory_profiling disable   # optional
```

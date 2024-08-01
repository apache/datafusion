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

# Apache DataFusion Substrait Test Data

This folder contains test data for the [substrait] crate.

The substrait crate is at an init stage and many functions not implemented yet. Compared to the [parquet-testing](https://github.com/apache/parquet-testing) submodule, this folder contains only simple test data evolving around the substrait producers and consumers for [logical plans](https://github.com/apache/datafusion/tree/main/datafusion/substrait/src/logical_plan) and [physical plans](https://github.com/apache/datafusion/tree/main/datafusion/substrait/src/physical_plan).

## Test Data

### Example Data

- [empty.csv](https://github.com/apache/datafusion/blob/main/datafusion/substrait/tests/testdata/empty.csv): An empty CSV file.
- [empty.parquet](https://github.com/apache/datafusion/blob/main/datafusion/substrait/tests/testdata/empty.parquet): An empty Parquet file with metadata only.
- [data.csv](https://github.com/apache/datafusion/blob/main/datafusion/substrait/tests/testdata/data.csv): A simple CSV file with 6 columns and 2 rows.
- [data.parquet](https://github.com/apache/datafusion/blob/main/datafusion/substrait/tests/testdata/data.parquet): A simple Parquet generated from the CSV file using `pandas`, e.g.,

  ```python
  import pandas as pd

  df = pandas.read_csv('data.csv')
  df.to_parquet('data.parquet')
  ```

### Add new test data

To add a new test data, create a new file in this folder, reference it in the test source file, e.g.,

```rust
let ctx = SessionContext::new();
let explicit_options = ParquetReadOptions::default();

ctx.register_parquet("data", "tests/testdata/data.parquet", explicit_options)
```

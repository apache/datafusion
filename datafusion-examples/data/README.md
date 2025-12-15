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

## Example datasets

| Filename                 | Path                                                                      | Description                                                                                                                                                                                                                  |
| ------------------------ | ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `aggregate_test_100.csv` | [`data/csv/aggregate_test_100.csv`](./csv/aggregate_test_100.csv)         | Multi-column CSV dataset with mixed data types (strings, signed/unsigned integers, floats, doubles). Used for aggregation, projection, filtering, and query planning examples. Derived from Arrow-style aggregate test data. |
| `cars.csv`               | [`data/csv/cars.csv`](./csv/cars.csv)                                     | Time-series–like dataset containing car identifiers, speed values, and timestamps. Used in window function and time-based query examples (e.g. ordering, window frames).                                                     |
| `regex.csv`              | [`data/csv/regex.csv`](./csv/regex.csv)                                   | Dataset for regular expression examples. Contains input values, regex patterns, replacement strings, and optional flags. Covers ASCII, Unicode, and locale-specific text processing.                                         |
| `window_1.csv`           | [`data/csv/window_1.csv`](./csv/window_1.csv)                             | Numeric dataset designed for window function demonstrations. Includes ordering keys and incremental values suitable for running totals, ranking, and frame-based calculations.                                               |
| `alltypes_plain.parquet` | [`data/parquet/alltypes_plain.parquet`](./parquet/alltypes_plain.parquet) | Parquet file containing columns of many Arrow/DataFusion-supported types (boolean, integers, floating-point, strings, timestamps). Used to demonstrate Parquet scanning, schema inference, and typed execution.              |

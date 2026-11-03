<!--
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

# Apache DataFusion Spark-compatible Expressions

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that provides [Apache Spark] compatible expressions for use with DataFusion.

## Using the DataFusion CLI

Pass `--spark` to `datafusion-cli` to enable the Spark SQL dialect and register
the Spark-compatible scalar, aggregate, and window functions for the session:

```shell
datafusion-cli --spark
```

The option is disabled by default because Spark-compatible functions can
override DataFusion functions with the same name.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[apache spark]: https://spark.apache.org/

## Implementation Guidelines

When implementing these functions, you can check if there are existing implementations
in the [Sail] or [Comet] projects first. If you do port functionality from these
sources, make sure to port over the corresponding tests too, to ensure correctness
and compatibility.

### `simplify()`

DataFusion functions allow you to implement `simplify()` which can let you rewrite
the function call during logical optimization, theoretically allowing you to avoid
implementing physical execution via `invoke_with_args()` if the rewrite is unconditional
(e.g. rewrite to an arithmetic operation).

**However, `invoke_with_args()` must always be implemented for functions in this
crate.** This is because downstream users such as Comet rely on DataFusion for physical
execution, and not logical planning/optimization. That means if a function doesn't
have a physical implementation (`invoke_with_args()`) it is not usable by Comet.

### Supported types

The functions in this crate need only support input types available to Spark; that
is, they do not need to handle unsigned types or types such as `Float16` or `Decimal64`.

[sail]: https://github.com/lakehq/sail
[comet]: https://github.com/apache/datafusion-comet

## Testing Guidelines

Prefer adding tests via SQLLogicTests where possible, see the [Spark SQLLogicTest README].
Resort to adding tests as Rust unit tests where it is impossible or difficult to
test via SLT. This is because direct invocation via Rust skips steps such as input
coercion, and is usually more verbose in the setup needed to pass data in (and
assert output data).

[spark sqllogictest readme]: ../sqllogictest/test_files/spark/README.md

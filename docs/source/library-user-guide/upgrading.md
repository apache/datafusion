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

# Upgrade Guides

## DataFusion `46.0.0`

### Use `invoke_with_args` instead of `invoke()` and `invoke_batch()`

DataFusion is moving to a consistent API for invoking ScalarUDFs,
[`ScalarUDFImpl::invoke_with_args()`], and deprecating
[`ScalarUDFImpl::invoke()`], [`ScalarUDFImpl::invoke_batch()`], and [`ScalarUDFImpl::invoke_no_args()`]

If you see errors such as the following it means the older APIs are being used:

```text
This feature is not implemented: Function concat does not implement invoke but called
```

To fix this error, use [`ScalarUDFImpl::invoke_with_args()`] instead, as shown
below. See [PR 14876] for an example.

Given existing code like this:

```rust
# /*
impl ScalarUDFImpl for SparkConcat {
...
    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            ArrayConcat::new().invoke_batch(args, number_rows)
        } else {
            ConcatFunc::new().invoke_batch(args, number_rows)
        }
    }
}
# */
```

To

```rust
# /* comment out so they don't run
impl ScalarUDFImpl for SparkConcat {
    ...
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args
            .args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::List(_)))
        {
            ArrayConcat::new().invoke_with_args(args)
        } else {
            ConcatFunc::new().invoke_with_args(args)
        }
    }
}
 # */
```

[`scalarudfimpl::invoke()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.invoke
[`scalarudfimpl::invoke_batch()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.invoke_batch
[`scalarudfimpl::invoke_no_args()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.invoke_no_args
[`scalarudfimpl::invoke_with_args()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.invoke_with_args
[pr 14876]: https://github.com/apache/datafusion/pull/14876

### `ParquetExec`, `AvroExec`, `CsvExec`, `JsonExec` deprecated

DataFusion 46 has a major change to how the built in DataSources are organized.
Instead of individual `ExecutionPlan`s for the different file formats they now
all use `DataSourceExec` and the format specific information is embodied in new
traits `DataSource` and `FileSource`.

Here is more information about

- [Design Ticket]
- Change PR [PR #14224]
- Example of an Upgrade [PR in delta-rs]

[design ticket]: https://github.com/apache/datafusion/issues/13838
[pr #14224]: https://github.com/apache/datafusion/pull/14224
[pr in delta-rs]: https://github.com/delta-io/delta-rs/pull/3261

### Cookbook: Changes to `ParquetExecBuilder`

Code that looks for `ParquetExec` like this will no longer work:

```rust
# /* comment to avoid running
    if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        // Do something with ParquetExec here
    }
# */
```

Instead, with `DataSourceExec`, the same information is now on `FileScanConfig` and
`ParquetSource`. The equivalent code is

```rust
# /* comment to avoid running
if let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
  if let Some(scan_config) = datasource_exec.data_source().as_any().downcast_ref::<FileScanConfig>() {
    // FileGroups, and other information is on the FileScanConfig
    // parquet
    if let Some(parquet_source) = scan_config.file_source.as_any().downcast_ref::<ParquetSource>()
    {
      // Information on PruningPredicates and parquet options are here
    }
}
# */
```

### Cookbook: Changes to `ParquetExecBuilder`

Likewise code that builds `ParquetExec` using the `ParquetExecBuilder` such as
the following must be changed:

```rust
# /* comment to avoid running
let mut exec_plan_builder = ParquetExecBuilder::new(
    FileScanConfig::new(self.log_store.object_store_url(), file_schema)
        .with_projection(self.projection.cloned())
        .with_limit(self.limit)
        .with_table_partition_cols(table_partition_cols),
)
.with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))
.with_table_parquet_options(parquet_options);

// Add filter
if let Some(predicate) = logical_filter {
    if config.enable_parquet_pushdown {
        exec_plan_builder = exec_plan_builder.with_predicate(predicate);
    }
};
# */
```

New code should use `FileScanConfig` to build the appropriate `DataSourceExec`:

```rust
# /* comment to avoid running
let mut file_source = ParquetSource::new(parquet_options)
    .with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}));

// Add filter
if let Some(predicate) = logical_filter {
    if config.enable_parquet_pushdown {
        file_source = file_source.with_predicate(Arc::clone(&file_schema), predicate);
    }
};

let file_scan_config = FileScanConfig::new(
    self.log_store.object_store_url(),
    file_schema,
    Arc::new(file_source),
)
.with_statistics(stats)
.with_projection(self.projection.cloned())
.with_limit(self.limit)
.with_table_partition_cols(table_partition_cols);

// Build the actual scan like this
parquet_scan: file_scan_config.build(),
# */
```

### `datafusion-cli` no longer automatically unescapes strings

`datafusion-cli` previously would incorrectly unescape string literals (see [ticket] for more details).

To escape `'` in SQL literals, use `''`:

```sql
> select 'it''s escaped';
+----------------------+
| Utf8("it's escaped") |
+----------------------+
| it's escaped         |
+----------------------+
1 row(s) fetched.
```

To include special characters (such as newlines via `\n`) you can use an `E` literal string. For example

```sql
> select 'foo\nbar';
+------------------+
| Utf8("foo\nbar") |
+------------------+
| foo\nbar         |
+------------------+
1 row(s) fetched.
Elapsed 0.005 seconds.
```

[ticket]: https://github.com/apache/datafusion/issues/13286

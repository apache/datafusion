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


### Changes to `invoke()` and `invoke_batch()` deprecated

We are migrating away from `ScalarUDFImpl::invoke()` and
`ScalarUDFImpl::invoke_batch()` in favor of `ScalarUDFImpl::invoke_with_args()`. (TODO get code links) 

If you see errors such as 
```text
Example
```

You can resolve them by replacing all .invoke() and .invoke_batch()calls with .invoke_with_args(). 
```text
TODO example
```

Example of changes:
- [PR XXXX] TODO


### `ParquetExec`, `AvroExec`, `CsvExec`, `JsonExec` deprecated

See more information
- Change PR [PR #14224](https://github.com/apache/datafusion/pull/14224)
- Example of an Upgrade [PR in delta-rs](https://github.com/delta-io/delta-rs/pull/3261)

DataFusion 46 has a major change to how the built in DataSources are organized. The 

### Cookbook: Changes to `ParquetExecBuilder`
#### Old pattern:

When writing optimizer passes, some code treats ParquetExec specially like this:

```rust
            if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
                // Do something with ParquetExec here
            }
        }
```

#### New Pattern
With the new DataSource exec, most information is now on `FileScanConfig` and `ParquetSource` 

```rust

if let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
  if let Some(scan_config) = datasource_exec.source().as_any().downcast_ref::<FileScanConfig>() {
    // FileGroups, and other information is on the FileScanConfig
    // parquet 
    if let Some(parquet_source) = scan_config.source.as_any().downcast_ref::<ParquetSource>()
    {
      // Information on PruningPredicates and parquet options are here
    }
}
```

### Cookbook: Changes to `ParquetExecBuilder`

#### Old pattern:

```rust
        let mut exec_plan_builder = ParquetExecBuilder::new(
            FileScanConfig::new(self.log_store.object_store_url(), file_schema)
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![vec![]]
                    } else {
                        file_groups.into_values().collect()
                    },
                )
                .with_statistics(stats)
                .with_projection(self.projection.cloned())
                .with_limit(self.limit)
                .with_table_partition_cols(table_partition_cols),
        )
        .with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))
        .with_table_parquet_options(parquet_options);

        // Sometimes (i.e Merge) we want to prune files that don't make the
        // filter and read the entire contents for files that do match the
        // filter
        if let Some(predicate) = logical_filter {
            if config.enable_parquet_pushdown {
                exec_plan_builder = exec_plan_builder.with_predicate(predicate);
            }
        };```

#### New Pattern


```rust
        let mut file_source = ParquetSource::new(parquet_options)
            .with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}));

        // Sometimes (i.e Merge) we want to prune files that don't make the
        // filter and read the entire contents for files that do match the
        // filter
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
        .with_file_groups(
            // If all files were filtered out, we still need to emit at least one partition to
            // pass datafusion sanity checks.
            //
            // See https://github.com/apache/datafusion/issues/11322
            if file_groups.is_empty() {
                vec![vec![]]
            } else {
                file_groups.into_values().collect()
            },
        )
        .with_statistics(stats)
        .with_projection(self.projection.cloned())
        .with_limit(self.limit)
        .with_table_partition_cols(table_partition_cols);```

// Build the actual scan like this
parquet_scan: file_scan_config.build(),

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

```
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

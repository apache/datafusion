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

## DataFusion `52.0.0`

**Note:** DataFusion `52.0.0` has not been released yet. The information provided in this section pertains to features and changes that have already been merged to the main branch and are awaiting release in this version.

You can see the current [status of the `52.0.0`release here](https://github.com/apache/datafusion/issues/18566)

### Changes to DFSchema API

To permit more efficient planning, several methods on `DFSchema` have been
changed to return references to the underlying [`&FieldRef`] rather than
[`&Field`]. This allows planners to more cheaply copy the references via
`Arc::clone` rather than cloning the entire `Field` structure.

You may need to change code to use `Arc::clone` instead of `.as_ref().clone()`
directly on the `Field`. For example:

```diff
- let field = df_schema.field("my_column").as_ref().clone();
+ let field = Arc::clone(df_schema.field("my_column"));
```

### Removal of `pyarrow` feature

The `pyarrow` feature flag has been removed. This feature has been migrated to
the `datafusion-python` repository since version `44.0.0`.

### Statistics handling moved from `FileSource` to `FileScanConfig`

Statistics are now managed directly by `FileScanConfig` instead of being delegated to `FileSource` implementations. This simplifies the `FileSource` trait and provides more consistent statistics handling across all file formats.

**Who is affected:**

- Users who have implemented custom `FileSource` implementations

**Breaking changes:**

Two methods have been removed from the `FileSource` trait:

- `with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource>`
- `statistics(&self) -> Result<Statistics>`

**Migration guide:**

If you have a custom `FileSource` implementation, you need to:

1. Remove the `with_statistics` method implementation
2. Remove the `statistics` method implementation
3. Remove any internal state that was storing statistics

**Before:**

```rust,ignore
#[derive(Clone)]
struct MyCustomSource {
    table_schema: TableSchema,
    projected_statistics: Option<Statistics>,
    // other fields...
}

impl FileSource for MyCustomSource {
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            projected_statistics: Some(statistics),
            // other fields...
        })
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone().unwrap_or_else(||
            Statistics::new_unknown(self.table_schema.file_schema())
        ))
    }

    // other methods...
}
```

**After:**

```rust,ignore
#[derive(Clone)]
struct MyCustomSource {
    table_schema: TableSchema,
    // projected_statistics field removed
    // other fields...
}

impl FileSource for MyCustomSource {
    // with_statistics method removed
    // statistics method removed

    // other methods...
}
```

**Accessing statistics:**

Statistics are now accessed through `FileScanConfig` instead of `FileSource`:

```diff
- let stats = config.file_source.statistics()?;
+ let stats = config.statistics();
```

Note that `FileScanConfig::statistics()` automatically marks statistics as inexact when filters are present, ensuring correctness when filters are pushed down.

### Planner now requires explicit opt-in for WITHIN GROUP syntax

The SQL planner now enforces the aggregate UDF contract more strictly: the
`WITHIN GROUP (ORDER BY ...)` syntax is accepted only if the aggregate UDAF
explicitly advertises support by returning `true` from
`AggregateUDFImpl::supports_within_group_clause()`.

Previously the planner forwarded a `WITHIN GROUP` clause to order-sensitive
aggregates even when they did not implement ordered-set semantics, which could
cause queries such as `SUM(x) WITHIN GROUP (ORDER BY x)` to plan successfully.
This behavior was too permissive and has been changed to match PostgreSQL and
the documented semantics.

Migration: If your UDAF intentionally implements ordered-set semantics and
wants to accept the `WITHIN GROUP` SQL syntax, update your implementation to
return `true` from `supports_within_group_clause()` and handle the ordering
semantics in your accumulator implementation. If your UDAF is merely
order-sensitive (but not an ordered-set aggregate), do not advertise
`supports_within_group_clause()` and clients should use alternative function
signatures (for example, explicit ordering as a function argument) instead.

### `AggregateUDFImpl::supports_null_handling_clause` now defaults to `false`

This method specifies whether an aggregate function allows `IGNORE NULLS`/`RESPECT NULLS`
during SQL parsing, with the implication it respects these configs during computation.

Most DataFusion aggregate functions silently ignored this syntax in prior versions
as they did not make use of it and it was permitted by default. We change this so
only the few functions which do respect this clause (e.g. `array_agg`, `first_value`,
`last_value`) need to implement it.

Custom user defined aggregate functions will also error if this syntax is used,
unless they explicitly declare support by overriding the method.

For example, SQL parsing will now fail for queries such as this:

```sql
SELECT median(c1) IGNORE NULLS FROM table
```

Instead of silently succeeding.

### API change for `CacheAccessor` trait

The remove API no longer requires a mutable instance

### FFI crate updates

Many of the structs in the `datafusion-ffi` crate have been updated to allow easier
conversion to the underlying trait types they represent. This simplifies some code
paths, but also provides an additional improvement in cases where library code goes
through a round trip via the foreign function interface.

To update your code, suppose you have a `FFI_SchemaProvider` called `ffi_provider`
and you wish to use this as a `SchemaProvider`. In the old approach you would do
something like:

```rust,ignore
    let foreign_provider: ForeignSchemaProvider = ffi_provider.into();
    let foreign_provider = Arc::new(foreign_provider) as Arc<dyn SchemaProvider>;
```

This code should now be written as:

```rust,ignore
    let foreign_provider: Arc<dyn SchemaProvider + Send> = ffi_provider.into();
    let foreign_provider = foreign_provider as Arc<dyn SchemaProvider>;
```

For the case of user defined functions, the updates are similar but you
may need to change the way you call the creation of the `ScalarUDF`.
Aggregate and window functions follow the same pattern.

Previously you may write:

```rust,ignore
    let foreign_udf: ForeignScalarUDF = ffi_udf.try_into()?;
    let foreign_udf: ScalarUDF = foreign_udf.into();
```

Instead this should now be:

```rust,ignore
    let foreign_udf: Arc<dyn ScalarUDFImpl> = ffi_udf.try_into()?;
    let foreign_udf = ScalarUDF::new_from_shared_impl(foreign_udf);
```

Additionally, the FFI structure for Scalar UDF's no longer contains a
`return_type` call. This code was not used since the `ForeignScalarUDF`
struct implements the `return_field_from_args` instead.

### Projection handling moved from FileScanConfig to FileSource

Projection handling has been moved from `FileScanConfig` into `FileSource` implementations. This enables format-specific projection pushdown (e.g., Parquet can push down struct field access, Vortex can push down computed expressions into un-decoded data).

**Who is affected:**

- Users who have implemented custom `FileSource` implementations
- Users who use `FileScanConfigBuilder::with_projection_indices` directly

**Breaking changes:**

1. **`FileSource::with_projection` replaced with `try_pushdown_projection`:**

   The `with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource>` method has been removed and replaced with `try_pushdown_projection(&self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>>`.

2. **`FileScanConfig.projection_exprs` field removed:**

   Projections are now stored in the `FileSource` directly, not in `FileScanConfig`.
   Various public helper methods that access projection information have been removed from `FileScanConfig`.

3. **`FileScanConfigBuilder::with_projection_indices` now returns `Result<Self>`:**

   This method can now fail if the projection pushdown fails.

4. **`FileSource::create_file_opener` now returns `Result<Arc<dyn FileOpener>>`:**

   Previously returned `Arc<dyn FileOpener>` directly.
   Any `FileSource` implementation that may fail to create a `FileOpener` should now return an appropriate error.

5. **`DataSource::try_swapping_with_projection` signature changed:**

   Parameter changed from `&[ProjectionExpr]` to `&ProjectionExprs`.

**Migration guide:**

If you have a custom `FileSource` implementation:

**Before:**

```rust,ignore
impl FileSource for MyCustomSource {
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        // Apply projection from config
        Arc::new(Self { /* ... */ })
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(MyOpener { /* ... */ })
    }
}
```

**After:**

```rust,ignore
impl FileSource for MyCustomSource {
    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        // Return None if projection cannot be pushed down
        // Return Some(new_source) with projection applied if it can
        Ok(Some(Arc::new(Self {
            projection: Some(projection.clone()),
            /* ... */
        })))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(MyOpener { /* ... */ }))
    }
}
```

We recommend you look at [#18627](https://github.com/apache/datafusion/pull/18627)
that introduced these changes for more examples for how this was handled for the various built in file sources.

We have added [`SplitProjection`](https://docs.rs/datafusion-datasource/latest/datafusion_datasource/projection/struct.SplitProjection.html) and [`ProjectionOpener`](https://docs.rs/datafusion-datasource/latest/datafusion_datasource/projection/struct.ProjectionOpener.html) helpers to make it easier to handle projections in your `FileSource` implementations.

For file sources that can only handle simple column selections (not computed expressions), use the `SplitProjection` and `ProjectionOpener` helpers to split the projection into pushdownable and non-pushdownable parts:

```rust,ignore
use datafusion_datasource::projection::{SplitProjection, ProjectionOpener};

// In try_pushdown_projection:
let split = SplitProjection::new(projection, self.table_schema())?;
// Use split.file_projection() for what to push down to the file format
// The ProjectionOpener wrapper will handle the rest
```

**For `FileScanConfigBuilder` users:**

```diff
let config = FileScanConfigBuilder::new(url, source)
-   .with_projection_indices(Some(vec![0, 2, 3]))
+   .with_projection_indices(Some(vec![0, 2, 3]))?
    .build();
```

**Handling projections in `FileSource`:**

## DataFusion `51.0.0`

### `arrow` / `parquet` updated to 57.0.0

### Upgrade to arrow `57.0.0` and parquet `57.0.0`

This version of DataFusion upgrades the underlying Apache Arrow implementation
to version `57.0.0`, including several dependent crates such as `prost`,
`tonic`, `pyo3`, and `substrait`. . See the [release
notes](https://github.com/apache/arrow-rs/releases/tag/57.0.0) for more details.

### `MSRV` updated to 1.88.0

The Minimum Supported Rust Version (MSRV) has been updated to [`1.88.0`].

[`1.88.0`]: https://releases.rs/docs/1.88.0/

### `FunctionRegistry` exposes two additional methods

`FunctionRegistry` exposes two additional methods `udafs` and `udwfs` which expose set of registered user defined aggregation and window function names. To upgrade implement methods returning set of registered function names:

```diff
impl FunctionRegistry for FunctionRegistryImpl {
      fn udfs(&self) -> HashSet<String> {
         self.scalar_functions.keys().cloned().collect()
     }
+    fn udafs(&self) -> HashSet<String> {
+        self.aggregate_functions.keys().cloned().collect()
+    }
+
+    fn udwfs(&self) -> HashSet<String> {
+        self.window_functions.keys().cloned().collect()
+    }
}
```

### `datafusion-proto` use `TaskContext` rather than `SessionContext` in physical plan serde methods

There have been changes in the public API methods of `datafusion-proto` which handle physical plan serde.

Methods like `physical_plan_from_bytes`, `parse_physical_expr` and similar, expect `TaskContext` instead of `SessionContext`

```diff
- let plan2 = physical_plan_from_bytes(&bytes, &ctx)?;
+ let plan2 = physical_plan_from_bytes(&bytes, &ctx.task_ctx())?;
```

as `TaskContext` contains `RuntimeEnv` methods such as `try_into_physical_plan` will not have explicit `RuntimeEnv` parameter.

```diff
let result_exec_plan: Arc<dyn ExecutionPlan> = proto
-   .try_into_physical_plan(&ctx, runtime.deref(), &composed_codec)
+.  .try_into_physical_plan(&ctx.task_ctx(), &composed_codec)
```

`PhysicalExtensionCodec::try_decode()` expects `TaskContext` instead of `FunctionRegistry`:

```diff
pub trait PhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
-        registry: &dyn FunctionRegistry,
+        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>>;
```

See [issue #17601] for more details.

[issue #17601]: https://github.com/apache/datafusion/issues/17601

### `SessionState`'s `sql_to_statement` method takes `Dialect` rather than a `str`

The `dialect` parameter of `sql_to_statement` method defined in `datafusion::execution::session_state::SessionState`
has changed from `&str` to `&Dialect`.
`Dialect` is an enum defined in the `datafusion-common`
crate under the `config` module that provides type safety
and better validation for SQL dialect selection

### Reorganization of `ListingTable` into `datafusion-catalog-listing` crate

There has been a long standing request to remove features such as `ListingTable`
from the `datafusion` crate to support faster build times. The structs
`ListingOptions`, `ListingTable`, and `ListingTableConfig` are now available
within the `datafusion-catalog-listing` crate. These are re-exported in
the `datafusion` crate, so this should be a minimal impact to existing users.

See [issue #14462] and [issue #17713] for more details.

[issue #14462]: https://github.com/apache/datafusion/issues/14462
[issue #17713]: https://github.com/apache/datafusion/issues/17713

### Reorganization of `ArrowSource` into `datafusion-datasource-arrow` crate

To support [issue #17713] the `ArrowSource` code has been removed from
the `datafusion` core crate into it's own crate, `datafusion-datasource-arrow`.
This follows the pattern for the AVRO, CSV, JSON, and Parquet data sources.
Users may need to update their paths to account for these changes.

See [issue #17713] for more details.

### `FileScanConfig::projection` renamed to `FileScanConfig::projection_exprs`

The `projection` field in `FileScanConfig` has been renamed to `projection_exprs` and its type has changed from `Option<Vec<usize>>` to `Option<ProjectionExprs>`. This change enables more powerful projection pushdown capabilities by supporting arbitrary physical expressions rather than just column indices.

**Impact on direct field access:**

If you directly access the `projection` field:

```rust,ignore
let config: FileScanConfig = ...;
let projection = config.projection;
```

You should update to:

```rust,ignore
let config: FileScanConfig = ...;
let projection_exprs = config.projection_exprs;
```

**Impact on builders:**

The `FileScanConfigBuilder::with_projection()` method has been deprecated in favor of `with_projection_indices()`:

```diff
let config = FileScanConfigBuilder::new(url, file_source)
-   .with_projection(Some(vec![0, 2, 3]))
+   .with_projection_indices(Some(vec![0, 2, 3]))
    .build();
```

Note: `with_projection()` still works but is deprecated and will be removed in a future release.

**What is `ProjectionExprs`?**

`ProjectionExprs` is a new type that represents a list of physical expressions for projection. While it can be constructed from column indices (which is what `with_projection_indices` does internally), it also supports arbitrary physical expressions, enabling advanced features like expression evaluation during scanning.

You can access column indices from `ProjectionExprs` using its methods if needed:

```rust,ignore
let projection_exprs: ProjectionExprs = ...;
// Get the column indices if the projection only contains simple column references
let indices = projection_exprs.column_indices();
```

### `DESCRIBE query` support

`DESCRIBE query` was previously an alias for `EXPLAIN query`, which outputs the
_execution plan_ of the query. With this release, `DESCRIBE query` now outputs
the computed _schema_ of the query, consistent with the behavior of `DESCRIBE table_name`.

### `datafusion.execution.time_zone` default configuration changed

The default value for `datafusion.execution.time_zone` previously was a string value of `+00:00` (GMT/Zulu time).
This was changed to be an `Option<String>` with a default of `None`. If you want to change the timezone back
to the previous value you can execute the sql:

```sql
SET
TIMEZONE = '+00:00';
```

This change was made to better support using the default timezone in scalar UDF functions such as
`now`, `current_date`, `current_time`, and `to_timestamp` among others.

### Refactoring of `FileSource` constructors and `FileScanConfigBuilder` to accept schemas upfront

The way schemas are passed to file sources and scan configurations has been significantly refactored. File sources now require the schema (including partition columns) to be provided at construction time, and `FileScanConfigBuilder` no longer takes a separate schema parameter.

**Who is affected:**

- Users who create `FileScanConfig` or file sources (`ParquetSource`, `CsvSource`, `JsonSource`, `AvroSource`) directly
- Users who implement custom `FileFormat` implementations

**Key changes:**

1. **FileSource constructors now require TableSchema**: All built-in file sources now take the schema in their constructor:

   ```diff
   - let source = ParquetSource::default();
   + let source = ParquetSource::new(table_schema);
   ```

2. **FileScanConfigBuilder no longer takes schema as a parameter**: The schema is now passed via the FileSource:

   ```diff
   - FileScanConfigBuilder::new(url, schema, source)
   + FileScanConfigBuilder::new(url, source)
   ```

3. **Partition columns are now part of TableSchema**: The `with_table_partition_cols()` method has been removed from `FileScanConfigBuilder`. Partition columns are now passed as part of the `TableSchema` to the FileSource constructor:

   ```diff
   + let table_schema = TableSchema::new(
   +     file_schema,
   +     vec![Arc::new(Field::new("date", DataType::Utf8, false))],
   + );
   + let source = ParquetSource::new(table_schema);
     let config = FileScanConfigBuilder::new(url, source)
   -     .with_table_partition_cols(vec![Field::new("date", DataType::Utf8, false)])
         .with_file(partitioned_file)
         .build();
   ```

4. **FileFormat::file_source() now takes TableSchema parameter**: Custom `FileFormat` implementations must be updated:
   ```diff
   impl FileFormat for MyFileFormat {
   -   fn file_source(&self) -> Arc<dyn FileSource> {
   +   fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
   -       Arc::new(MyFileSource::default())
   +       Arc::new(MyFileSource::new(table_schema))
       }
   }
   ```

**Migration examples:**

For Parquet files:

```diff
- let source = Arc::new(ParquetSource::default());
- let config = FileScanConfigBuilder::new(url, schema, source)
+ let table_schema = TableSchema::new(schema, vec![]);
+ let source = Arc::new(ParquetSource::new(table_schema));
+ let config = FileScanConfigBuilder::new(url, source)
      .with_file(partitioned_file)
      .build();
```

For CSV files with partition columns:

```diff
- let source = Arc::new(CsvSource::new(true, b',', b'"'));
- let config = FileScanConfigBuilder::new(url, file_schema, source)
-     .with_table_partition_cols(vec![Field::new("year", DataType::Int32, false)])
+ let options = CsvOptions {
+     has_header: Some(true),
+     delimiter: b',',
+     quote: b'"',
+     ..Default::default()
+ };
+ let table_schema = TableSchema::new(
+     file_schema,
+     vec![Arc::new(Field::new("year", DataType::Int32, false))],
+ );
+ let source = Arc::new(CsvSource::new(table_schema).with_csv_options(options));
+ let config = FileScanConfigBuilder::new(url, source)
      .build();
```

### Introduction of `TableSchema` and changes to `FileSource::with_schema()` method

A new `TableSchema` struct has been introduced in the `datafusion-datasource` crate to better manage table schemas with partition columns. This struct helps distinguish between:

- **File schema**: The schema of actual data files on disk
- **Partition columns**: Columns derived from directory structure (e.g., Hive-style partitioning)
- **Table schema**: The complete schema combining both file and partition columns

As part of this change, the `FileSource::with_schema()` method signature has changed from accepting a `SchemaRef` to accepting a `TableSchema`.

**Who is affected:**

- Users who have implemented custom `FileSource` implementations will need to update their code
- Users who only use built-in file sources (Parquet, CSV, JSON, AVRO, Arrow) are not affected

**Migration guide for custom `FileSource` implementations:**

```diff
 use datafusion_datasource::file::FileSource;
-use arrow::datatypes::SchemaRef;
+use datafusion_datasource::TableSchema;

 impl FileSource for MyCustomSource {
-    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
+    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
         Arc::new(Self {
-            schema: Some(schema),
+            // Use schema.file_schema() to get the file schema without partition columns
+            schema: Some(Arc::clone(schema.file_schema())),
             ..self.clone()
         })
     }
 }
```

For implementations that need access to partition columns:

```rust,ignore
fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
    Arc::new(Self {
        file_schema: Arc::clone(schema.file_schema()),
        partition_cols: schema.table_partition_cols().clone(),
        table_schema: Arc::clone(schema.table_schema()),
        ..self.clone()
    })
}
```

**Note**: Most `FileSource` implementations only need to store the file schema (without partition columns), as shown in the first example. The second pattern of storing all three schema components is typically only needed for advanced use cases where you need access to different schema representations for different operations (e.g., ParquetSource uses the file schema for building pruning predicates but needs the table schema for filter pushdown logic).

**Using `TableSchema` directly:**

If you're constructing a `FileScanConfig` or working with table schemas and partition columns, you can now use `TableSchema`:

```rust
use datafusion_datasource::TableSchema;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

// Create a TableSchema with partition columns
let file_schema = Arc::new(Schema::new(vec![
    Field::new("user_id", DataType::Int64, false),
    Field::new("amount", DataType::Float64, false),
]));

let partition_cols = vec![
    Arc::new(Field::new("date", DataType::Utf8, false)),
    Arc::new(Field::new("region", DataType::Utf8, false)),
];

let table_schema = TableSchema::new(file_schema, partition_cols);

// Access different schema representations
let file_schema_ref = table_schema.file_schema();      // Schema without partition columns
let full_schema = table_schema.table_schema();          // Complete schema with partition columns
let partition_cols_ref = table_schema.table_partition_cols(); // Just the partition columns
```

### `AggregateUDFImpl::is_ordered_set_aggregate` has been renamed to `AggregateUDFImpl::supports_within_group_clause`

This method has been renamed to better reflect the actual impact it has for aggregate UDF implementations.
The accompanying `AggregateUDF::is_ordered_set_aggregate` has also been renamed to `AggregateUDF::supports_within_group_clause`.
No functionality has been changed with regards to this method; it still refers only to permitting use of `WITHIN GROUP`
SQL syntax for the aggregate function.

## DataFusion `50.0.0`

### ListingTable automatically detects Hive Partitioned tables

DataFusion 50.0.0 automatically infers Hive partitions when using the `ListingTableFactory` and `CREATE EXTERNAL TABLE`. Previously,
when creating a `ListingTable`, datasets that use Hive partitioning (e.g.
`/table_root/column1=value1/column2=value2/data.parquet`) would not have the Hive columns reflected in
the table's schema or data. The previous behavior can be
restored by setting the `datafusion.execution.listing_table_factory_infer_partitions` configuration option to `false`.
See [issue #17049] for more details.

[issue #17049]: https://github.com/apache/datafusion/issues/17049

### `MSRV` updated to 1.86.0

The Minimum Supported Rust Version (MSRV) has been updated to [`1.86.0`].
See [#17230] for details.

[`1.86.0`]: https://releases.rs/docs/1.86.0/
[#17230]: https://github.com/apache/datafusion/pull/17230

### `ScalarUDFImpl`, `AggregateUDFImpl` and `WindowUDFImpl` traits now require `PartialEq`, `Eq`, and `Hash` traits

To address error-proneness of `ScalarUDFImpl::equals`, `AggregateUDFImpl::equals`and
`WindowUDFImpl::equals` methods and to make it easy to implement function equality correctly,
the `equals` and `hash_value` methods have been removed from `ScalarUDFImpl`, `AggregateUDFImpl`
and `WindowUDFImpl` traits. They are replaced the requirement to implement the `PartialEq`, `Eq`,
and `Hash` traits on any type implementing `ScalarUDFImpl`, `AggregateUDFImpl` or `WindowUDFImpl`.
Please see [issue #16677] for more details.

Most of the scalar functions are stateless and have a `signature` field. These can be migrated
using regular expressions

- search for `\#\[derive\(Debug\)\](\n *(pub )?struct \w+ \{\n *signature\: Signature\,\n *\})`,
- replace with `#[derive(Debug, PartialEq, Eq, Hash)]$1`,
- review all the changes and make sure only function structs were changed.

[issue #16677]: https://github.com/apache/datafusion/issues/16677

### `AsyncScalarUDFImpl::invoke_async_with_args` returns `ColumnarValue`

In order to enable single value optimizations and be consistent with other
user defined function APIs, the `AsyncScalarUDFImpl::invoke_async_with_args` method now
returns a `ColumnarValue` instead of a `ArrayRef`.

To upgrade, change the return type of your implementation

```rust
# /* comment to avoid running
impl AsyncScalarUDFImpl for AskLLM {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ColumnarValue> {
        ..
      return array_ref; // old code
    }
}
# */
```

To return a `ColumnarValue`

```rust
# /* comment to avoid running
impl AsyncScalarUDFImpl for AskLLM {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ColumnarValue> {
        ..
      return ColumnarValue::from(array_ref); // new code
    }
}
# */
```

See [#16896](https://github.com/apache/datafusion/issues/16896) for more details.

### `ProjectionExpr` changed from type alias to struct

`ProjectionExpr` has been changed from a type alias to a struct with named fields to improve code clarity and maintainability.

**Before:**

```rust,ignore
pub type ProjectionExpr = (Arc<dyn PhysicalExpr>, String);
```

**After:**

```rust,ignore
#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    pub expr: Arc<dyn PhysicalExpr>,
    pub alias: String,
}
```

To upgrade your code:

- Replace tuple construction `(expr, alias)` with `ProjectionExpr::new(expr, alias)` or `ProjectionExpr { expr, alias }`
- Replace tuple field access `.0` and `.1` with `.expr` and `.alias`
- Update pattern matching from `(expr, alias)` to `ProjectionExpr { expr, alias }`

This mainly impacts use of `ProjectionExec`.

This change was done in [#17398]

[#17398]: https://github.com/apache/datafusion/pull/17398

### `SessionState`, `SessionConfig`, and `OptimizerConfig` returns `&Arc<ConfigOptions>` instead of `&ConfigOptions`

To provide broader access to `ConfigOptions` and reduce required clones, some
APIs have been changed to return a `&Arc<ConfigOptions>` instead of a
`&ConfigOptions`. This allows sharing the same `ConfigOptions` across multiple
threads without needing to clone the entire `ConfigOptions` structure unless it
is modified.

Most users will not be impacted by this change since the Rust compiler typically
automatically dereference the `Arc` when needed. However, in some cases you may
have to change your code to explicitly call `as_ref()` for example, from

```rust
# /* comment to avoid running
let optimizer_config: &ConfigOptions = state.options();
#  */
```

To

```rust
# /* comment to avoid running
let optimizer_config: &ConfigOptions = state.options().as_ref();
#  */
```

See PR [#16970](https://github.com/apache/datafusion/pull/16970)

### API Change to `AsyncScalarUDFImpl::invoke_async_with_args`

The `invoke_async_with_args` method of the `AsyncScalarUDFImpl` trait has been
updated to remove the `_option: &ConfigOptions` parameter to simplify the API
now that the `ConfigOptions` can be accessed through the `ScalarFunctionArgs`
parameter.

You can change your code like this

```rust
# /* comment to avoid running
impl AsyncScalarUDFImpl for AskLLM {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        ..
    }
    ...
}
# */
```

To this:

```rust
# /* comment to avoid running

impl AsyncScalarUDFImpl for AskLLM {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ArrayRef> {
        let options = &args.config_options;
        ..
    }
    ...
}
# */
```

### Schema Rewriter Module Moved to New Crate

The `schema_rewriter` module and its associated symbols have been moved from `datafusion_physical_expr` to a new crate `datafusion_physical_expr_adapter`. This affects the following symbols:

- `DefaultPhysicalExprAdapter`
- `DefaultPhysicalExprAdapterFactory`
- `PhysicalExprAdapter`
- `PhysicalExprAdapterFactory`

To upgrade, change your imports to:

```rust
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapter, DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapter, PhysicalExprAdapterFactory
};
```

### Upgrade to arrow `56.0.0` and parquet `56.0.0`

This version of DataFusion upgrades the underlying Apache Arrow implementation
to version `56.0.0`. See the [release notes](https://github.com/apache/arrow-rs/releases/tag/56.0.0)
for more details.

### Added `ExecutionPlan::reset_state`

In order to fix a bug in DataFusion `49.0.0` where dynamic filters (currently only generated in the presence of a query such as `ORDER BY ... LIMIT ...`)
produced incorrect results in recursive queries, a new method `reset_state` has been added to the `ExecutionPlan` trait.

Any `ExecutionPlan` that needs to maintain internal state or references to other nodes in the execution plan tree should implement this method to reset that state.
See [#17028] for more details and an example implementation for `SortExec`.

[#17028]: https://github.com/apache/datafusion/pull/17028

### Nested Loop Join input sort order cannot be preserved

The Nested Loop Join operator has been rewritten from scratch to improve performance and memory efficiency. From the micro-benchmarks: this change introduces up to 5X speed-up and uses only 1% memory in extreme cases compared to the previous implementation.

However, the new implementation cannot preserve input sort order like the old version could. This is a fundamental design trade-off that prioritizes performance and memory efficiency over sort order preservation.

See [#16996] for details.

[#16996]: https://github.com/apache/datafusion/pull/16996

### Add `as_any()` method to `LazyBatchGenerator`

To help with protobuf serialization, the `as_any()` method has been added to the `LazyBatchGenerator` trait. This means you will need to add `as_any()` to your implementation of `LazyBatchGenerator`:

```rust
# /* comment to avoid running

impl LazyBatchGenerator for MyBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    ...
}

# */
```

See [#17200](https://github.com/apache/datafusion/pull/17200) for details.

### Refactored `DataSource::try_swapping_with_projection`

We refactored `DataSource::try_swapping_with_projection` to simplify the method and minimize leakage across the ExecutionPlan <-> DataSource abstraction layer.
Reimplementation for any custom `DataSource` should be relatively straightforward, see [#17395] for more details.

[#17395]: https://github.com/apache/datafusion/pull/17395/

### `FileOpenFuture` now uses `DataFusionError` instead of `ArrowError`

The `FileOpenFuture` type alias has been updated to use `DataFusionError` instead of `ArrowError` for its error type. This change affects the `FileOpener` trait and any implementations that work with file streaming operations.

**Before:**

```rust,ignore
pub type FileOpenFuture = BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>>;
```

**After:**

```rust,ignore
pub type FileOpenFuture = BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch>>>>;
```

If you have custom implementations of `FileOpener` or work directly with `FileOpenFuture`, you'll need to update your error handling to use `DataFusionError` instead of `ArrowError`. The `FileStreamState` enum's `Open` variant has also been updated accordingly. See [#17397] for more details.

[#17397]: https://github.com/apache/datafusion/pull/17397

### FFI user defined aggregate function signature change

The Foreign Function Interface (FFI) signature for user defined aggregate functions
has been updated to call `return_field` instead of `return_type` on the underlying
aggregate function. This is to support metadata handling with these aggregate functions.
This change should be transparent to most users. If you have written unit tests to call
`return_type` directly, you may need to change them to calling `return_field` instead.

This update is a breaking change to the FFI API. The current best practice when using the
FFI crate is to ensure that all libraries that are interacting are using the same
underlying Rust version. Issue [#17374] has been opened to discuss stabilization of
this interface so that these libraries can be used across different DataFusion versions.

See [#17407] for details.

[#17407]: https://github.com/apache/datafusion/pull/17407
[#17374]: https://github.com/apache/datafusion/issues/17374

### Added `PhysicalExpr::is_volatile_node`

We added a method to `PhysicalExpr` to mark a `PhysicalExpr` as volatile:

```rust,ignore
impl PhysicalExpr for MyRandomExpr {
  fn is_volatile_node(&self) -> bool {
    true
  }
}
```

We've shipped this with a default value of `false` to minimize breakage but we highly recommend that implementers of `PhysicalExpr` opt into a behavior, even if it is returning `false`.

You can see more discussion and example implementations in [#17351].

[#17351]: https://github.com/apache/datafusion/pull/17351

## DataFusion `49.0.0`

### `MSRV` updated to 1.85.1

The Minimum Supported Rust Version (MSRV) has been updated to [`1.85.1`]. See
[#16728] for details.

[`1.85.1`]: https://releases.rs/docs/1.85.1/
[#16728]: https://github.com/apache/datafusion/pull/16728

### `DataFusionError` variants are now `Box`ed

To reduce the size of `DataFusionError`, several variants that were previously stored inline are now `Box`ed. This reduces the size of `Result<T, DataFusionError>` and thus stack usage and async state machine size. Please see [#16652] for more details.

The following variants of `DataFusionError` are now boxed:

- `ArrowError`
- `SQL`
- `SchemaError`

This is a breaking change. Code that constructs or matches on these variants will need to be updated.

For example, to create a `SchemaError`, instead of:

```rust
# /* comment to avoid running
use datafusion_common::{DataFusionError, SchemaError};
DataFusionError::SchemaError(
  SchemaError::DuplicateUnqualifiedField { name: "foo".to_string() },
  Box::new(None)
)
# */
```

You now need to `Box` the inner error:

```rust
# /* comment to avoid running
use datafusion_common::{DataFusionError, SchemaError};
DataFusionError::SchemaError(
  Box::new(SchemaError::DuplicateUnqualifiedField { name: "foo".to_string() }),
  Box::new(None)
)
# */
```

[#16652]: https://github.com/apache/datafusion/issues/16652

### Metadata on Arrow Types is now represented by `FieldMetadata`

Metadata from the Arrow `Field` is now stored using the `FieldMetadata`
structure. In prior versions it was stored as both a `HashMap<String, String>`
and a `BTreeMap<String, String>`. `FieldMetadata` is a easier to work with and
is more efficient.

To create `FieldMetadata` from a `Field`:

```rust
# /* comment to avoid running
 let metadata = FieldMetadata::from(&field);
# */
```

To add metadata to a `Field`, use the `add_to_field` method:

```rust
# /* comment to avoid running
let updated_field = metadata.add_to_field(field);
# */
```

See [#16317] for details.

[#16317]: https://github.com/apache/datafusion/pull/16317

### New `datafusion.execution.spill_compression` configuration option

DataFusion 49.0.0 adds support for compressing spill files when data is written to disk during spilling query execution. A new configuration option `datafusion.execution.spill_compression` controls the compression codec used.

**Configuration:**

- **Key**: `datafusion.execution.spill_compression`
- **Default**: `uncompressed`
- **Valid values**: `uncompressed`, `lz4_frame`, `zstd`

**Usage:**

```rust
# /* comment to avoid running
use datafusion::prelude::*;
use datafusion_common::config::SpillCompression;

let config = SessionConfig::default()
    .with_spill_compression(SpillCompression::Zstd);
let ctx = SessionContext::new_with_config(config);
# */
```

Or via SQL:

```sql
SET datafusion.execution.spill_compression = 'zstd';
```

For more details about this configuration option, including performance trade-offs between different compression codecs, see the [Configuration Settings](../user-guide/configs.md) documentation.

### Deprecated `map_varchar_to_utf8view` configuration option

See [issue #16290](https://github.com/apache/datafusion/pull/16290) for more information
The old configuration

```text
datafusion.sql_parser.map_varchar_to_utf8view
```

is now **deprecated** in favor of the unified option below.\
If you previously used this to control only `VARCHAR`→`Utf8View` mapping, please migrate to `map_string_types_to_utf8view`.

---

### New `map_string_types_to_utf8view` configuration option

To unify **all** SQL string types (`CHAR`, `VARCHAR`, `TEXT`, `STRING`) to Arrow’s zero‑copy `Utf8View`, DataFusion 49.0.0 introduces:

- **Key**: `datafusion.sql_parser.map_string_types_to_utf8view`
- **Default**: `true`

**Description:**

- When **true** (default), **all** SQL string types are mapped to `Utf8View`, avoiding full‑copy UTF‑8 allocations and improving performance.
- When **false**, DataFusion falls back to the legacy `Utf8` mapping for **all** string types.

#### Examples

```rust
# /* comment to avoid running
// Disable Utf8View mapping for all SQL string types
let opts = datafusion::sql::planner::ParserOptions::new()
    .with_map_string_types_to_utf8view(false);

// Verify the setting is applied
assert!(!opts.map_string_types_to_utf8view);
# */
```

---

```sql
-- Disable Utf8View mapping globally
SET datafusion.sql_parser.map_string_types_to_utf8view = false;

-- Now VARCHAR, CHAR, TEXT, STRING all use Utf8 rather than Utf8View
CREATE TABLE my_table (a VARCHAR, b TEXT, c STRING);
DESCRIBE my_table;
```

### Deprecating `SchemaAdapterFactory` and `SchemaAdapter`

We are moving away from converting data (using `SchemaAdapter`) to converting the expressions themselves (which is more efficient and flexible).

See [issue #16800](https://github.com/apache/datafusion/issues/16800) for more information
The first place this change has taken place is in predicate pushdown for Parquet.
By default if you do not use a custom `SchemaAdapterFactory` we will use expression conversion instead.
If you do set a custom `SchemaAdapterFactory` we will continue to use it but emit a warning about that code path being deprecated.

To resolve this you need to implement a custom `PhysicalExprAdapterFactory` and use that instead of a `SchemaAdapterFactory`.
See the [default values](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_data_source/default_column_values.rs) for an example of how to do this.
Opting into the new APIs will set you up for future changes since we plan to expand use of `PhysicalExprAdapterFactory` to other areas of DataFusion.

See [#16800] for details.

[#16800]: https://github.com/apache/datafusion/issues/16800

### `TableParquetOptions` Updated

The `TableParquetOptions` struct has a new `crypto` field to specify encryption
options for Parquet files. The `ParquetEncryptionOptions` implements `Default`
so you can upgrade your existing code like this:

```rust
# /* comment to avoid running
TableParquetOptions {
  global,
  column_specific_options,
  key_value_metadata,
}
# */
```

To this:

```rust
# /* comment to avoid running
TableParquetOptions {
  global,
  column_specific_options,
  key_value_metadata,
  crypto: Default::default(), // New crypto field
}
# */
```

## DataFusion `48.0.1`

### `datafusion.execution.collect_statistics` now defaults to `true`

The default value of the `datafusion.execution.collect_statistics` configuration
setting is now true. This change impacts users that use that value directly and relied
on its default value being `false`.

This change also restores the default behavior of `ListingTable` to its previous. If you use it directly
you can maintain the current behavior by overriding the default value in your code.

```rust
# /* comment to avoid running
ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_collect_stat(false)
    // other options
# */
```

## DataFusion `48.0.0`

### `Expr::Literal` has optional metadata

The [`Expr::Literal`] variant now includes optional metadata, which allows for
carrying through Arrow field metadata to support extension types and other uses.

This means code such as

```rust
# /* comment to avoid running
match expr {
...
  Expr::Literal(scalar) => ...
...
}
#  */
```

Should be updated to:

```rust
# /* comment to avoid running
match expr {
...
  Expr::Literal(scalar, _metadata) => ...
...
}
#  */
```

Likewise constructing `Expr::Literal` requires metadata as well. The [`lit`] function
has not changed and returns an `Expr::Literal` with no metadata.

[`expr::literal`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#variant.Literal
[`lit`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html

### `Expr::WindowFunction` is now `Box`ed

`Expr::WindowFunction` is now a `Box<WindowFunction>` instead of a `WindowFunction` directly.
This change was made to reduce the size of `Expr` and improve performance when
planning queries (see [details on #16207]).

This is a breaking change, so you will need to update your code if you match
on `Expr::WindowFunction` directly. For example, if you have code like this:

```rust
# /* comment to avoid running
match expr {
  Expr::WindowFunction(WindowFunction {
    params:
      WindowFunctionParams {
       partition_by,
       order_by,
      ..
    }
  }) => {
    // Use partition_by and order_by as needed
  }
  _ => {
    // other expr
  }
}
# */
```

You will need to change it to:

```rust
# /* comment to avoid running
match expr {
  Expr::WindowFunction(window_fun) => {
    let WindowFunction {
      fun,
      params: WindowFunctionParams {
        args,
        partition_by,
        ..
        },
    } = window_fun.as_ref();
    // Use partition_by and order_by as needed
  }
  _ => {
    // other expr
  }
}
#  */
```

[details on #16207]: https://github.com/apache/datafusion/pull/16207#issuecomment-2922659103

### The `VARCHAR` SQL type is now represented as `Utf8View` in Arrow

The mapping of the SQL `VARCHAR` type has been changed from `Utf8` to `Utf8View`
which improves performance for many string operations. You can read more about
`Utf8View` in the [DataFusion blog post on German-style strings]

[datafusion blog post on german-style strings]: https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/

This means that when you create a table with a `VARCHAR` column, it will now use
`Utf8View` as the underlying data type. For example:

```sql
> CREATE TABLE my_table (my_column VARCHAR);
0 row(s) fetched.
Elapsed 0.001 seconds.

> DESCRIBE my_table;
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| my_column   | Utf8View  | YES         |
+-------------+-----------+-------------+
1 row(s) fetched.
Elapsed 0.000 seconds.
```

You can restore the old behavior of using `Utf8` by changing the
`datafusion.sql_parser.map_varchar_to_utf8view` configuration setting. For
example

```sql
> set datafusion.sql_parser.map_varchar_to_utf8view = false;
0 row(s) fetched.
Elapsed 0.001 seconds.

> CREATE TABLE my_table (my_column VARCHAR);
0 row(s) fetched.
Elapsed 0.014 seconds.

> DESCRIBE my_table;
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| my_column   | Utf8      | YES         |
+-------------+-----------+-------------+
1 row(s) fetched.
Elapsed 0.004 seconds.
```

### `ListingOptions` default for `collect_stat` changed from `true` to `false`

This makes it agree with the default for `SessionConfig`.
Most users won't be impacted by this change but if you were using `ListingOptions` directly
and relied on the default value of `collect_stat` being `true`, you will need to
explicitly set it to `true` in your code.

```rust
# /* comment to avoid running
ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_collect_stat(true)
    // other options
# */
```

### Processing `FieldRef` instead of `DataType` for user defined functions

In order to support metadata handling and extension types, user defined functions are
now switching to traits which use `FieldRef` rather than a `DataType` and nullability.
This gives a single interface to both of these parameters and additionally allows
access to metadata fields, which can be used for extension types.

To upgrade structs which implement `ScalarUDFImpl`, if you have implemented
`return_type_from_args` you need instead to implement `return_field_from_args`.
If your functions do not need to handle metadata, this should be straightforward
repackaging of the output data into a `FieldRef`. The name you specify on the
field is not important. It will be overwritten during planning. `ReturnInfo`
has been removed, so you will need to remove all references to it.

`ScalarFunctionArgs` now contains a field called `arg_fields`. You can use this
to access the metadata associated with the columnar values during invocation.

To upgrade user defined aggregate functions, there is now a function
`return_field` that will allow you to specify both metadata and nullability of
your function. You are not required to implement this if you do not need to
handle metadata.

The largest change to aggregate functions happens in the accumulator arguments.
Both the `AccumulatorArgs` and `StateFieldsArgs` now contain `FieldRef` rather
than `DataType`.

To upgrade window functions, `ExpressionArgs` now contains input fields instead
of input data types. When setting these fields, the name of the field is
not important since this gets overwritten during the planning stage. All you
should need to do is wrap your existing data types in fields with nullability
set depending on your use case.

### Physical Expression return `Field`

To support the changes to user defined functions processing metadata, the
`PhysicalExpr` trait, which now must specify a return `Field` based on the input
schema. To upgrade structs which implement `PhysicalExpr` you need to implement
the `return_field` function. There are numerous examples in the `physical-expr`
crate.

### `FileFormat::supports_filters_pushdown` replaced with `FileSource::try_pushdown_filters`

To support more general filter pushdown, the `FileFormat::supports_filters_pushdown` was replaced with
`FileSource::try_pushdown_filters`.
If you implemented a custom `FileFormat` that uses a custom `FileSource` you will need to implement
`FileSource::try_pushdown_filters`.
See `ParquetSource::try_pushdown_filters` for an example of how to implement this.

`FileFormat::supports_filters_pushdown` has been removed.

### `ParquetExec`, `AvroExec`, `CsvExec`, `JsonExec` Removed

`ParquetExec`, `AvroExec`, `CsvExec`, and `JsonExec` were deprecated in
DataFusion 46 and are removed in DataFusion 48. This is sooner than the normal
process described in the [API Deprecation Guidelines] because all the tests
cover the new `DataSourceExec` rather than the older structures. As we evolve
`DataSource`, the old structures began to show signs of "bit rotting" (not
working but no one knows due to lack of test coverage).

[api deprecation guidelines]: https://datafusion.apache.org/contributor-guide/api-health.html#deprecation-guidelines

### `PartitionedFile` added as an argument to the `FileOpener` trait

This is necessary to properly fix filter pushdown for filters that combine partition
columns and file columns (e.g. `day = username['dob']`).

If you implemented a custom `FileOpener` you will need to add the `PartitionedFile` argument
but are not required to use it in any way.

## DataFusion `47.0.0`

This section calls out some of the major changes in the `47.0.0` release of DataFusion.

Here are some example upgrade PRs that demonstrate changes required when upgrading from DataFusion 46.0.0:

- [delta-rs Upgrade to `47.0.0`](https://github.com/delta-io/delta-rs/pull/3378)
- [DataFusion Comet Upgrade to `47.0.0`](https://github.com/apache/datafusion-comet/pull/1563)
- [Sail Upgrade to `47.0.0`](https://github.com/lakehq/sail/pull/434)

### Upgrades to `arrow-rs` and `arrow-parquet` 55.0.0 and `object_store` 0.12.0

Several APIs are changed in the underlying arrow and parquet libraries to use a
`u64` instead of `usize` to better support WASM (See [#7371] and [#6961])

Additionally `ObjectStore::list` and `ObjectStore::list_with_offset` have been changed to return `static` lifetimes (See [#6619])

[#6619]: https://github.com/apache/arrow-rs/pull/6619
[#7371]: https://github.com/apache/arrow-rs/pull/7371

This requires converting from `usize` to `u64` occasionally as well as changes to `ObjectStore` implementations such as

```rust
# /* comment to avoid running
impl Objectstore {
    ...
    // The range is now a u64 instead of usize
    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }
    ...
    // the lifetime is now 'static instead of `_ (meaning the captured closure can't contain references)
    // (this also applies to list_with_offset)
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
}
# */
```

The `ParquetObjectReader` has been updated to no longer require the object size
(it can be fetched using a single suffix request). See [#7334] for details

[#7334]: https://github.com/apache/arrow-rs/pull/7334

Pattern in DataFusion `46.0.0`:

```rust
# /* comment to avoid running
let meta: ObjectMeta = ...;
let reader = ParquetObjectReader::new(store, meta);
# */
```

Pattern in DataFusion `47.0.0`:

```rust
# /* comment to avoid running
let meta: ObjectMeta = ...;
let reader = ParquetObjectReader::new(store, location)
  .with_file_size(meta.size);
# */
```

### `DisplayFormatType::TreeRender`

DataFusion now supports [`tree` style explain plans]. Implementations of
`Executionplan` must also provide a description in the
`DisplayFormatType::TreeRender` format. This can be the same as the existing
`DisplayFormatType::Default`.

[`tree` style explain plans]: https://datafusion.apache.org/user-guide/sql/explain.html#tree-format-default

### Removed Deprecated APIs

Several APIs have been removed in this release. These were either deprecated
previously or were hard to use correctly such as the multiple different
`ScalarUDFImpl::invoke*` APIs. See [#15130], [#15123], and [#15027] for more
details.

[#15130]: https://github.com/apache/datafusion/pull/15130
[#15123]: https://github.com/apache/datafusion/pull/15123
[#15027]: https://github.com/apache/datafusion/pull/15027

### `FileScanConfig` --> `FileScanConfigBuilder`

Previously, `FileScanConfig::build()` directly created ExecutionPlans. In
DataFusion 47.0.0 this has been changed to use `FileScanConfigBuilder`. See
[#15352] for details.

[#15352]: https://github.com/apache/datafusion/pull/15352

Pattern in DataFusion `46.0.0`:

```rust
# /* comment to avoid running
let plan = FileScanConfig::new(url, schema, Arc::new(file_source))
  .with_statistics(stats)
  ...
  .build()
# */
```

Pattern in DataFusion `47.0.0`:

```rust
# /* comment to avoid running
let config = FileScanConfigBuilder::new(url, Arc::new(file_source))
  .with_statistics(stats)
  ...
  .build();
let scan = DataSourceExec::from_data_source(config);
# */
```

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
# /* comment to avoid running
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
# /* comment to avoid running
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
        file_source = file_source.with_predicate(predicate);
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

### Changes to array scalar function signatures

DataFusion 46 has changed the way scalar array function signatures are
declared. Previously, functions needed to select from a list of predefined
signatures within the `ArrayFunctionSignature` enum. Now the signatures
can be defined via a `Vec` of pseudo-types, which each correspond to a
single argument. Those pseudo-types are the variants of the
`ArrayFunctionArgument` enum and are as follows:

- `Array`: An argument of type List/LargeList/FixedSizeList. All Array
  arguments must be coercible to the same type.
- `Element`: An argument that is coercible to the inner type of the `Array`
  arguments.
- `Index`: An `Int64` argument.

Each of the old variants can be converted to the new format as follows:

`TypeSignature::ArraySignature(ArrayFunctionSignature::ArrayAndElement)`:

```rust
# use datafusion::common::utils::ListCoercion;
# use datafusion_expr_common::signature::{ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature};

TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
    arguments: vec![ArrayFunctionArgument::Array, ArrayFunctionArgument::Element],
    array_coercion: Some(ListCoercion::FixedSizedListToList),
});
```

`TypeSignature::ArraySignature(ArrayFunctionSignature::ElementAndArray)`:

```rust
# use datafusion::common::utils::ListCoercion;
# use datafusion_expr_common::signature::{ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature};

TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
    arguments: vec![ArrayFunctionArgument::Element, ArrayFunctionArgument::Array],
    array_coercion: Some(ListCoercion::FixedSizedListToList),
});
```

`TypeSignature::ArraySignature(ArrayFunctionSignature::ArrayAndIndex)`:

```rust
# use datafusion::common::utils::ListCoercion;
# use datafusion_expr_common::signature::{ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature};

TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
    arguments: vec![ArrayFunctionArgument::Array, ArrayFunctionArgument::Index],
    array_coercion: None,
});
```

`TypeSignature::ArraySignature(ArrayFunctionSignature::ArrayAndElementAndOptionalIndex)`:

```rust
# use datafusion::common::utils::ListCoercion;
# use datafusion_expr_common::signature::{ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature};

TypeSignature::OneOf(vec![
    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
        arguments: vec![ArrayFunctionArgument::Array, ArrayFunctionArgument::Element],
        array_coercion: None,
    }),
    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
        arguments: vec![
            ArrayFunctionArgument::Array,
            ArrayFunctionArgument::Element,
            ArrayFunctionArgument::Index,
        ],
        array_coercion: None,
    }),
]);
```

`TypeSignature::ArraySignature(ArrayFunctionSignature::Array)`:

```rust
# use datafusion::common::utils::ListCoercion;
# use datafusion_expr_common::signature::{ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature};

TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
    arguments: vec![ArrayFunctionArgument::Array],
    array_coercion: None,
});
```

Alternatively, you can switch to using one of the following functions which
take care of constructing the `TypeSignature` for you:

- `Signature::array_and_element`
- `Signature::array_and_element_and_optional_index`
- `Signature::array_and_index`
- `Signature::array`

[ticket]: https://github.com/apache/datafusion/issues/13286

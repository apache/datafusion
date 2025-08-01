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

## DataFusion `50.0.0`

**Note:** DataFusion `50.0.0` has not been released yet. The information provided in this section pertains to features and changes that have already been merged to the main branch and are awaiting release in this version.
You can see the current [status of the `50.0.0 `release here](https://github.com/apache/datafusion/issues/16799)

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
See the [default values](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/default_column_values.rs) for an example of how to do this.
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
handle metatdata.

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
let config = FileScanConfigBuilder::new(url, schema, Arc::new(file_source))
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
can be defined via a `Vec` of psuedo-types, which each correspond to a
single argument. Those psuedo-types are the variants of the
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

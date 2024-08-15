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

# Custom Table Provider

Like other areas of DataFusion, you extend DataFusion's functionality by implementing a trait. The `TableProvider` and associated traits, have methods that allow you to implement a custom table provider, i.e. use DataFusion's other functionality with your custom data source.

This section will also touch on how to have DataFusion use the new `TableProvider` implementation.

## Table Provider and Scan

The `scan` method on the `TableProvider` is likely its most important. It returns an `ExecutionPlan` that DataFusion will use to read the actual data during execution of the query.

### Scan

As mentioned, `scan` returns an execution plan, and in particular a `Result<Arc<dyn ExecutionPlan>>`. The core of this is returning something that can be dynamically dispatched to an `ExecutionPlan`. And as per the general DataFusion idea, we'll need to implement it.

#### Execution Plan

The `ExecutionPlan` trait at its core is a way to get a stream of batches. The aptly-named `execute` method returns a `Result<SendableRecordBatchStream>`, which should be a stream of `RecordBatch`es that can be sent across threads, and has a schema that matches the data to be contained in those batches.

There are many different types of `SendableRecordBatchStream` implemented in DataFusion -- you can use a pre existing one, such as `MemoryStream` (if your `RecordBatch`es are all in memory) or implement your own custom logic, depending on your usecase.

Looking at the [example in this repo][ex], the execute method:

```rust
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
}

impl ExecutionPlan for CustomExec {
    fn name(&self) {
        "CustomExec"
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let users: Vec<User> = {
            let db = self.db.inner.lock().unwrap();
            db.data.values().cloned().collect()
        };

        let mut id_array = UInt8Builder::with_capacity(users.len());
        let mut account_array = UInt64Builder::with_capacity(users.len());

        for user in users {
            id_array.append_value(user.id);
            account_array.append_value(user.bank_account);
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(id_array.finish()),
                    Arc::new(account_array.finish()),
                ],
            )?],
            self.schema(),
            None,
        )?))
    }
}
```

This:

1. Gets the users from the database
2. Constructs the individual output arrays (columns)
3. Returns a `MemoryStream` of a single `RecordBatch` with the arrays

I.e. returns the "physical" data. For other examples, refer to the [`CsvExec`][csv] and [`ParquetExec`][parquet] for more complex implementations.

With the `ExecutionPlan` implemented, we can now implement the `scan` method of the `TableProvider`.

#### Scan Revisited

The `scan` method of the `TableProvider` returns a `Result<Arc<dyn ExecutionPlan>>`. We can use the `Arc` to return a reference-counted pointer to the `ExecutionPlan` we implemented. In the example, this is done by:

```rust
impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}
```

With this, and the implementation of the omitted methods, we can now use the `CustomDataSource` as a `TableProvider` in DataFusion.

##### Additional `TableProvider` Methods

`scan` has no default implementation, so it needed to be written. There are other methods on the `TableProvider` that have default implementations, but can be overridden if needed to provide additional functionality.

###### `supports_filters_pushdown`

The `supports_filters_pushdown` method can be overridden to indicate which filter expressions support being pushed down to the data source and within that the specificity of the pushdown.

This returns a `Vec` of `TableProviderFilterPushDown` enums where each enum represents a filter that can be pushed down. The `TableProviderFilterPushDown` enum has three variants:

- `TableProviderFilterPushDown::Unsupported` - the filter cannot be pushed down
- `TableProviderFilterPushDown::Exact` - the filter can be pushed down and the data source can guarantee that the filter will be applied completely to all rows. This is the highest performance option.
- `TableProviderFilterPushDown::Inexact` - the filter can be pushed down, but the data source cannot guarantee that the filter will be applied to all rows. DataFusion will apply `Inexact` filters again after the scan to ensure correctness.

For filters that can be pushed down, they'll be passed to the `scan` method as the `filters` parameter and they can be made use of there.

## Using the Custom Table Provider

In order to use the custom table provider, we need to register it with DataFusion. This is done by creating a `TableProvider` and registering it with the `SessionContext`.

```rust
let ctx = SessionContext::new();

let custom_table_provider = CustomDataSource::new();
ctx.register_table("custom_table", Arc::new(custom_table_provider));
```

This will allow you to use the custom table provider in DataFusion. For example, you could use it in a SQL query to get a `DataFrame`.

```rust
let df = ctx.sql("SELECT id, bank_account FROM custom_table")?;
```

## Recap

To recap, in order to implement a custom table provider, you need to:

1. Implement the `TableProvider` trait
2. Implement the `ExecutionPlan` trait
3. Register the `TableProvider` with the `SessionContext`

## Next Steps

As mentioned the [csv] and [parquet] implementations are good examples of how to implement a `TableProvider`. The [example in this repo][ex] is a good example of how to implement a `TableProvider` that uses a custom data source.

More abstractly, see the following traits for more information on how to implement a custom `TableProvider` for a file format:

- `FileOpener` - a trait for opening a file and inferring the schema
- `FileFormat` - a trait for reading a file format
- `ListingTableProvider` - a useful trait for implementing a `TableProvider` that lists files in a directory

[ex]: https://github.com/apache/datafusion/blob/a5e86fae3baadbd99f8fd0df83f45fde22f7b0c6/datafusion-examples/examples/custom_datasource.rs#L214C1-L276
[csv]: https://github.com/apache/datafusion/blob/a5e86fae3baadbd99f8fd0df83f45fde22f7b0c6/datafusion/core/src/datasource/physical_plan/csv.rs#L57-L70
[parquet]: https://github.com/apache/datafusion/blob/a5e86fae3baadbd99f8fd0df83f45fde22f7b0c6/datafusion/core/src/datasource/physical_plan/parquet.rs#L77-L104

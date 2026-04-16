// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! See `main.rs` for how to run it.

use std::sync::Arc;

use arrow::array::{RecordBatch, record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::assert_batches_eq;
use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::prelude::SessionConfig;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

// Example showing how to implement custom casting rules to adapt file schemas.
// This example enforces strictly widening casts: if the file type is Int64 and
// the table type is Int32, it errors before reading the data. Without this
// custom cast rule DataFusion would apply the narrowing cast and might only
// error after reading a row that it could not cast.
pub async fn custom_file_casts() -> Result<()> {
    println!("=== Creating example data ===");

    // Create a logical / table schema with an Int32 column (nullable)
    let logical_schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

    // Create some data that can be cast (Int16 -> Int32 is widening) and some that cannot (Int64 -> Int32 is narrowing)
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let path = Path::from("good.parquet");
    let batch = record_batch!(("id", Int16, [1, 2, 3]))?;
    write_data(&store, &path, &batch).await?;
    let path = Path::from("bad.parquet");
    let batch = record_batch!(("id", Int64, [1, 2, 3]))?;
    write_data(&store, &path, &batch).await?;

    // Set up query execution
    let mut cfg = SessionConfig::new();
    // Turn on filter pushdown so that the PhysicalExprAdapter is used
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.runtime_env()
        .register_object_store(ObjectStoreUrl::parse("memory://")?.as_ref(), store);

    // Register our good and bad files via ListingTable
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///good.parquet")?)
            .infer_options(&ctx.state())
            .await?
            .with_schema(Arc::clone(&logical_schema))
            .with_expr_adapter_factory(Arc::new(
                CustomCastPhysicalExprAdapterFactory::new(Arc::new(
                    DefaultPhysicalExprAdapterFactory,
                )),
            ));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("good_table", Arc::new(table))?;
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///bad.parquet")?)
            .infer_options(&ctx.state())
            .await?
            .with_schema(Arc::clone(&logical_schema))
            .with_expr_adapter_factory(Arc::new(
                CustomCastPhysicalExprAdapterFactory::new(Arc::new(
                    DefaultPhysicalExprAdapterFactory,
                )),
            ));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("bad_table", Arc::new(table))?;

    println!("\n=== File with narrower schema is cast ===");
    let query = "SELECT id FROM good_table WHERE id > 1";
    println!("Query: {query}");
    let batches = ctx.sql(query).await?.collect().await?;
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 2  |",
        "| 3  |",
        "+----+",
    ];
    arrow::util::pretty::print_batches(&batches)?;
    assert_batches_eq!(expected, &batches);

    println!("\n=== File with wider schema errors ===");
    let query = "SELECT id FROM bad_table WHERE id > 1";
    println!("Query: {query}");
    match ctx.sql(query).await?.collect().await {
        Ok(_) => panic!("Expected error for narrowing cast, but query succeeded"),
        Err(e) => {
            println!("Caught expected error: {e}");
        }
    }
    Ok(())
}

async fn write_data(
    store: &dyn ObjectStore,
    path: &Path,
    batch: &RecordBatch,
) -> Result<()> {
    let mut buf = vec![];
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;

    let payload = PutPayload::from_bytes(buf.into());
    store.put(path, payload).await?;
    Ok(())
}

/// Factory for creating custom cast physical expression adapters
#[derive(Debug)]
struct CustomCastPhysicalExprAdapterFactory {
    inner: Arc<dyn PhysicalExprAdapterFactory>,
}

impl CustomCastPhysicalExprAdapterFactory {
    fn new(inner: Arc<dyn PhysicalExprAdapterFactory>) -> Self {
        Self { inner }
    }
}

impl PhysicalExprAdapterFactory for CustomCastPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let inner = self
            .inner
            .create(logical_file_schema, Arc::clone(&physical_file_schema))?;
        Ok(Arc::new(CustomCastsPhysicalExprAdapter {
            physical_file_schema,
            inner,
        }))
    }
}

/// Custom `PhysicalExprAdapter` that wraps the default adapter and rejects
/// narrowing file-schema casts.
#[derive(Debug, Clone)]
struct CustomCastsPhysicalExprAdapter {
    physical_file_schema: SchemaRef,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for CustomCastsPhysicalExprAdapter {
    fn rewrite(&self, mut expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // First delegate to the inner adapter to handle standard schema adaptation
        // and discover any necessary casts.
        expr = self.inner.rewrite(expr)?;
        // Now apply custom casting rules or swap CastExprs for a custom cast
        // kernel / expression. For example, DataFusion Comet has a custom cast
        // kernel in its native Spark expression implementation.
        expr.transform(|expr| {
            if let Some(cast) = expr.downcast_ref::<CastExpr>() {
                let input_data_type =
                    cast.expr().data_type(&self.physical_file_schema)?;
                let output_data_type = cast.target_field().data_type();
                if !cast.is_bigger_cast(&input_data_type) {
                    return not_impl_err!(
                        "Unsupported CAST from {input_data_type} to {output_data_type}"
                    );
                }
            }
            Ok(Transformed::no(expr))
        })
        .data()
    }
}

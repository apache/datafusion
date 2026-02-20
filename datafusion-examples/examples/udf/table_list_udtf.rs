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

use std::sync::{Arc, LazyLock};

use arrow::array::{RecordBatch, StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    catalog::{MemTable, TableFunctionArgs, TableFunctionImpl, TableProvider},
    common::Result,
    execution::SessionState,
    prelude::SessionContext,
};
use datafusion_common::{DataFusionError, plan_err};
use tokio::{runtime::Handle, task::block_in_place};

const FUNCTION_NAME: &str = "table_list";

// The example shows, how to create UDTF that depends on the session state.
// Defines a `table_list` UDTF that returns a list of tables within the provided session.

pub async fn table_list_udtf() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udtf(FUNCTION_NAME, Arc::new(TableListUdtf));

    // Register different kinds of tables.
    ctx.sql("create view v as select 1")
        .await?
        .collect()
        .await?;
    ctx.sql("create table t(a int)").await?.collect().await?;

    // Print results.
    ctx.sql("select * from table_list()").await?.show().await?;

    Ok(())
}

#[derive(Debug, Default)]
struct TableListUdtf;

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("catalog", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
    ]))
});

impl TableFunctionImpl for TableListUdtf {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        if !args.args.is_empty() {
            return plan_err!(
                "{}: unexpected number of arguments: {}, expected: 0",
                FUNCTION_NAME,
                args.args.len()
            );
        }
        let state = args
            .session
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal("failed to downcast state".into())
            })?;

        let mut catalogs = StringBuilder::new();
        let mut schemas = StringBuilder::new();
        let mut tables = StringBuilder::new();
        let mut types = StringBuilder::new();

        let catalog_list = state.catalog_list();
        for catalog_name in catalog_list.catalog_names() {
            let Some(catalog) = catalog_list.catalog(&catalog_name) else {
                continue;
            };
            for schema_name in catalog.schema_names() {
                let Some(schema) = catalog.schema(&schema_name) else {
                    continue;
                };
                for table_name in schema.table_names() {
                    let Some(provider) = block_in_place(|| {
                        Handle::current().block_on(schema.table(&table_name))
                    })?
                    else {
                        continue;
                    };
                    catalogs.append_value(catalog_name.clone());
                    schemas.append_value(schema_name.clone());
                    tables.append_value(table_name.clone());
                    types.append_value(provider.table_type().to_string())
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&SCHEMA),
            vec![
                Arc::new(catalogs.finish()),
                Arc::new(schemas.finish()),
                Arc::new(tables.finish()),
                Arc::new(types.finish()),
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}

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

use arrow::array::{FixedSizeBinaryArray, RecordBatch};
use arrow_schema::extension::Uuid;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_expr::planner::TypePlanner;
use datafusion_expr::registry::MemoryExtensionTypeRegistry;
use insta::assert_snapshot;
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![uuid_field()]))
}

fn uuid_field() -> Field {
    Field::new("my_uuids", DataType::FixedSizeBinary(16), false).with_extension_type(Uuid)
}

async fn create_test_table() -> Result<DataFrame> {
    let schema = test_schema();

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(FixedSizeBinaryArray::from(vec![
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 5, 6],
        ]))],
    )?;

    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.register_batch("test", batch)?;

    ctx.table("test").await
}

// Test here

#[tokio::test]
async fn test_pretty_print_extension_type_formatter() -> Result<()> {
    let result = create_test_table().await?.to_string().await?;

    assert_snapshot!(
        result,
        @r"
    +--------------------------------------+
    | my_uuids                             |
    +--------------------------------------+
    | 00000000-0000-0000-0000-000000000000 |
    | 00010203-0405-0607-0809-000102030506 |
    +--------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn create_cast_uuid_to_char() -> Result<()> {
    let schema = test_schema();

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(FixedSizeBinaryArray::from(vec![
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 5, 6],
        ]))],
    )?;

    let state = SessionStateBuilder::default()
        .with_type_planner(Arc::new(CustomTypePlanner {}))
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.register_batch("test", batch)?;

    let df = ctx.sql("SELECT my_uuids::VARCHAR FROM test").await?;
    let batches = df.collect().await?;

    assert_batches_eq!(
        [
            "+--------------------------------------+",
            "| test.my_uuids                        |",
            "+--------------------------------------+",
            "| 00000000-0000-0000-0000-000000000000 |",
            "| 00010203-0405-0607-0809-000102030506 |",
            "+--------------------------------------+",
        ],
        &batches
    );

    Ok(())
}

#[tokio::test]
async fn create_cast_char_to_uuid() -> Result<()> {
    let state = SessionStateBuilder::default()
        .with_type_planner(Arc::new(CustomTypePlanner {}))
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);

    let df = ctx
        .sql("SELECT '00010203-0405-0607-0809-000102030506'::UUID AS uuid")
        .await?;
    let batches = df.collect().await?;
    assert_batches_eq!(
        [
            "+----------------------------------+",
            "| uuid                             |",
            "+----------------------------------+",
            "| 00010203040506070809000102030506 |",
            "+----------------------------------+",
        ],
        &batches
    );

    Ok(())
}

#[derive(Debug)]
pub struct CustomTypePlanner {}

impl TypePlanner for CustomTypePlanner {
    fn plan_type_field(
        &self,
        sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<FieldRef>> {
        match sql_type {
            sqlparser::ast::DataType::Uuid => Ok(Some(Arc::new(
                Field::new("", DataType::FixedSizeBinary(16), true).with_metadata(
                    [("ARROW:extension:name".to_string(), "arrow.uuid".to_string())]
                        .into(),
                ),
            ))),
            _ => Ok(None),
        }
    }
}

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
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
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
        .with_canonical_extension_types()?
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.register_batch("test", batch)?;

    ctx.table("test").await
}

#[tokio::test]
async fn test_pretty_print_logical_plan() -> Result<()> {
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

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

//! Parquet schema merge integration tests

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use datafusion_common::Result;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

#[tokio::test]
async fn merge_schema() -> Result<()> {
    let tmp_dir = TempDir::new()?;

    let schema1 = Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int16, false),
            Field::new("c", DataType::Int32, false),
        ],
        HashMap::new(),
    );

    let schema2 = Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int16, false),
        ],
        HashMap::new(),
    );

    create_parquet_file(tmp_dir.path(), "t1.parquet", &schema1).await?;
    create_parquet_file(tmp_dir.path(), "t2.parquet", &schema2).await?;

    let ctx = SessionContext::default();
    ctx.register_parquet(
        "test",
        tmp_dir.path().to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    let df = ctx.table("test")?;

    let expected_schema = Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ],
        HashMap::new(),
    );

    let actual_schema: Schema = df.schema().into();
    assert_eq!(expected_schema, actual_schema);

    let batches = df.collect().await?;
    for batch in batches {
        assert_eq!(&expected_schema, batch.schema().as_ref())
    }

    Ok(())
}

async fn create_parquet_file(path: &Path, filename: &str, schema: &Schema) -> Result<()> {
    let ctx = SessionContext::default();
    let options = CsvReadOptions::new().schema(&schema);
    ctx.register_csv("t", "tests/example.csv", options).await?;
    let t = ctx.table("t")?;
    let path = path.join(filename);
    t.write_parquet(path.to_str().unwrap(), None).await
}

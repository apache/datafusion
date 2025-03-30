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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::error::Result;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_substrait::physical_plan::{consumer, producer};

use datafusion::datasource::memory::DataSourceExec;
use substrait::proto::extensions;

#[tokio::test]
async fn parquet_exec() -> Result<()> {
    let source = Arc::new(ParquetSource::default());

    let scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(Schema::empty()),
        source,
    )
    .with_file_groups(vec![
        FileGroup::new(vec![PartitionedFile::new(
            "file://foo/part-0.parquet".to_string(),
            123,
        )]),
        FileGroup::new(vec![PartitionedFile::new(
            "file://foo/part-1.parquet".to_string(),
            123,
        )]),
    ])
    .build();
    let parquet_exec: Arc<dyn ExecutionPlan> =
        DataSourceExec::from_data_source(scan_config);

    let mut extension_info: (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ) = (vec![], HashMap::new());

    let substrait_rel =
        producer::to_substrait_rel(parquet_exec.as_ref(), &mut extension_info)?;

    let ctx = SessionContext::new();

    let parquet_exec_roundtrip =
        consumer::from_substrait_rel(&ctx, substrait_rel.as_ref(), &HashMap::new())
            .await?;

    let expected = format!("{}", displayable(parquet_exec.as_ref()).indent(true));
    let actual = format!(
        "{}",
        displayable(parquet_exec_roundtrip.as_ref()).indent(true)
    );
    assert_eq!(expected, actual);

    Ok(())
}

#[tokio::test]
async fn simple_select() -> Result<()> {
    roundtrip("SELECT a, b FROM data").await
}

#[tokio::test]
#[ignore = "This test is failing because the translation of the substrait plan to the physical plan is not implemented yet"]
async fn simple_select_alltypes() -> Result<()> {
    roundtrip_alltypes("SELECT bool_col, int_col FROM alltypes_plain").await
}

#[tokio::test]
async fn wildcard_select() -> Result<()> {
    roundtrip("SELECT * FROM data").await
}

#[tokio::test]
#[ignore = "This test is failing because the translation of the substrait plan to the physical plan is not implemented yet"]
async fn wildcard_select_alltypes() -> Result<()> {
    roundtrip_alltypes("SELECT * FROM alltypes_plain").await
}

async fn roundtrip(sql: &str) -> Result<()> {
    let ctx = create_parquet_context().await?;
    let df = ctx.sql(sql).await?;

    roundtrip_parquet(df).await?;

    Ok(())
}

async fn roundtrip_alltypes(sql: &str) -> Result<()> {
    let ctx = create_all_types_context().await?;
    let df = ctx.sql(sql).await?;

    roundtrip_parquet(df).await?;

    Ok(())
}

async fn roundtrip_parquet(df: DataFrame) -> Result<()> {
    let physical_plan = df.create_physical_plan().await?;

    // Convert the plan into a substrait (protobuf) Rel
    let mut extension_info = (vec![], HashMap::new());
    let substrait_plan =
        producer::to_substrait_rel(physical_plan.as_ref(), &mut extension_info)?;

    // Convert the substrait Rel back into a physical plan
    let ctx = create_parquet_context().await?;
    let physical_plan_roundtrip =
        consumer::from_substrait_rel(&ctx, substrait_plan.as_ref(), &HashMap::new())
            .await?;

    // Compare the original and roundtrip physical plans
    let expected = format!("{}", displayable(physical_plan.as_ref()).indent(true));
    let actual = format!(
        "{}",
        displayable(physical_plan_roundtrip.as_ref()).indent(true)
    );
    assert_eq!(expected, actual);

    Ok(())
}

async fn create_parquet_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let explicit_options = ParquetReadOptions::default();

    ctx.register_parquet("data", "tests/testdata/data.parquet", explicit_options)
        .await?;

    Ok(ctx)
}

async fn create_all_types_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    Ok(ctx)
}

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
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_substrait::physical_plan::consumer::PhysicalPlanConsumerOptions;
use datafusion_substrait::physical_plan::{consumer, producer};

use datafusion::datasource::memory::DataSourceExec;
use object_store::path::Path as ObjectStorePath;
use substrait::proto::Rel;
use substrait::proto::extensions;
use substrait::proto::read_rel::ReadType;
use substrait::proto::read_rel::local_files::file_or_files::PathType;
use substrait::proto::rel::RelType;

#[tokio::test]
async fn parquet_exec() -> Result<()> {
    let schema = Arc::new(Schema::empty());
    let source = Arc::new(ParquetSource::new(schema.clone()));

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![
                FileGroup::new(vec![PartitionedFile::new(
                    testdata_object_path("data.parquet"),
                    123,
                )]),
                FileGroup::new(vec![PartitionedFile::new(
                    testdata_object_path("empty.parquet"),
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

    let parquet_exec_roundtrip = consumer::from_substrait_rel_with_options(
        &ctx,
        substrait_rel.as_ref(),
        &HashMap::new(),
        &testdata_physical_plan_options(),
    )
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
async fn local_files_require_allowed_root() -> Result<()> {
    let rel = local_file_rel(testdata_object_path("data.parquet"))?;
    let ctx = SessionContext::new();

    let err = consumer::from_substrait_rel(&ctx, rel.as_ref(), &HashMap::new())
        .await
        .expect_err("local file imports should require an explicit allowed root");

    assert_contains(
        &err,
        "requires an allowed local file root",
        "unexpected error for missing local file root",
    );

    Ok(())
}

#[tokio::test]
async fn local_files_must_stay_under_allowed_root() -> Result<()> {
    let rel = local_file_rel(workspace_path("Cargo.toml").display().to_string())?;
    let ctx = SessionContext::new();

    let err = consumer::from_substrait_rel_with_options(
        &ctx,
        rel.as_ref(),
        &HashMap::new(),
        &testdata_physical_plan_options(),
    )
    .await
    .expect_err("local file imports outside the allowed root should fail");

    assert_contains(
        &err,
        "outside the allowed local file roots",
        "unexpected error for local file root escape",
    );

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

    roundtrip_parquet(df, testdata_physical_plan_options()).await?;

    Ok(())
}

async fn roundtrip_alltypes(sql: &str) -> Result<()> {
    let ctx = create_all_types_context().await?;
    let df = ctx.sql(sql).await?;
    let testdata = datafusion::test_util::parquet_test_data();

    roundtrip_parquet(
        df,
        PhysicalPlanConsumerOptions::new().with_allowed_local_file_root(testdata),
    )
    .await?;

    Ok(())
}

async fn roundtrip_parquet(
    df: DataFrame,
    options: PhysicalPlanConsumerOptions,
) -> Result<()> {
    let physical_plan = df.create_physical_plan().await?;

    // Convert the plan into a substrait (protobuf) Rel
    let mut extension_info = (vec![], HashMap::new());
    let substrait_plan =
        producer::to_substrait_rel(physical_plan.as_ref(), &mut extension_info)?;

    // Convert the substrait Rel back into a physical plan
    let ctx = create_parquet_context().await?;
    let physical_plan_roundtrip = consumer::from_substrait_rel_with_options(
        &ctx,
        substrait_plan.as_ref(),
        &HashMap::new(),
        &options,
    )
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

    ctx.register_parquet(
        "data",
        testdata_path("data.parquet").to_string_lossy().as_ref(),
        explicit_options,
    )
    .await?;

    Ok(ctx)
}

fn testdata_physical_plan_options() -> PhysicalPlanConsumerOptions {
    PhysicalPlanConsumerOptions::new().with_allowed_local_file_root(testdata_path(""))
}

fn testdata_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
        .join(file_name)
}

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(path)
}

fn testdata_object_path(file_name: &str) -> String {
    ObjectStorePath::from_filesystem_path(testdata_path(file_name))
        .unwrap()
        .to_string()
}

fn local_file_rel(path: String) -> Result<Box<Rel>> {
    let schema = Arc::new(Schema::empty());
    let source = Arc::new(ParquetSource::new(schema.clone()));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                testdata_object_path("data.parquet"),
                123,
            )])])
            .build();
    let parquet_exec: Arc<dyn ExecutionPlan> =
        DataSourceExec::from_data_source(scan_config);

    let mut extension_info: (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ) = (vec![], HashMap::new());

    let mut rel = producer::to_substrait_rel(parquet_exec.as_ref(), &mut extension_info)?;
    set_first_local_file_path(&mut rel, path);
    Ok(rel)
}

fn set_first_local_file_path(rel: &mut Rel, path: String) {
    let Some(RelType::Read(read)) = &mut rel.rel_type else {
        panic!("expected read rel");
    };
    let Some(ReadType::LocalFiles(files)) = &mut read.read_type else {
        panic!("expected local files read");
    };
    files.items[0].path_type = Some(PathType::UriPath(path));
}

fn assert_contains(err: &DataFusionError, expected: &str, context: &str) {
    let actual = err.to_string();
    assert!(
        actual.contains(expected),
        "{context}: expected error to contain {expected:?}, got {actual:?}"
    );
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

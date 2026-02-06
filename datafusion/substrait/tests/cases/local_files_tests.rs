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

//! Tests for LocalFiles support in the logical plan consumer

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, TableReference};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::prelude::SessionContext;
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    SubstraitConsumer, from_substrait_plan,
};
use std::sync::Arc;
use substrait::proto::read_rel::local_files::FileOrFiles;
use substrait::proto::read_rel::local_files::file_or_files::PathType;
use substrait::proto::read_rel::{LocalFiles, ReadType};
use substrait::proto::rel::RelType;
use substrait::proto::{NamedStruct, Plan, PlanRel, ReadRel, Rel, Type, plan_rel};

/// Helper to create a simple schema for testing
fn create_test_schema() -> Schema {
    Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ])
}

/// Helper to create a NamedStruct from a Schema
fn schema_to_named_struct(schema: &Schema) -> NamedStruct {
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let types: Vec<substrait::proto::r#type::Struct> =
        vec![substrait::proto::r#type::Struct {
            types: schema
                .fields()
                .iter()
                .map(|f| {
                    let kind = match f.data_type() {
                        DataType::Int64 => substrait::proto::r#type::Kind::I64(
                            substrait::proto::r#type::I64 {
                                nullability: if f.is_nullable() { 1 } else { 2 },
                                type_variation_reference: 0,
                            },
                        ),
                        DataType::Utf8 => substrait::proto::r#type::Kind::String(
                            substrait::proto::r#type::String {
                                nullability: if f.is_nullable() { 1 } else { 2 },
                                type_variation_reference: 0,
                            },
                        ),
                        _ => unimplemented!("Only Int64 and Utf8 supported in test"),
                    };
                    Type { kind: Some(kind) }
                })
                .collect(),
            nullability: 0,
            type_variation_reference: 0,
        }];

    NamedStruct {
        names,
        r#struct: Some(substrait::proto::r#type::Struct {
            types: types[0].types.clone(),
            nullability: 0,
            type_variation_reference: 0,
        }),
    }
}

/// Creates a Substrait Plan with LocalFiles read type
fn create_local_files_plan(file_paths: Vec<(PathType, String)>) -> Plan {
    let schema = create_test_schema();
    let named_struct = schema_to_named_struct(&schema);

    let items: Vec<FileOrFiles> = file_paths
        .into_iter()
        .enumerate()
        .map(|(idx, (path_type, path))| {
            let pt = match path_type {
                PathType::UriPath(_) => PathType::UriPath(path),
                PathType::UriPathGlob(_) => PathType::UriPathGlob(path),
                PathType::UriFile(_) => PathType::UriFile(path),
                PathType::UriFolder(_) => PathType::UriFolder(path),
            };
            FileOrFiles {
                path_type: Some(pt),
                partition_index: idx as u64,
                start: 0,
                length: 0,
                file_format: None,
            }
        })
        .collect();

    let read_rel = ReadRel {
        common: None,
        base_schema: Some(named_struct),
        filter: None,
        best_effort_filter: None,
        projection: None,
        advanced_extension: None,
        read_type: Some(ReadType::LocalFiles(LocalFiles {
            items,
            advanced_extension: None,
        })),
    };

    #[allow(deprecated)]
    Plan {
        version: None,
        extension_uris: vec![],
        extensions: vec![],
        extension_urns: vec![],
        parameter_bindings: vec![],
        type_aliases: vec![],
        relations: vec![PlanRel {
            rel_type: Some(plan_rel::RelType::Root(substrait::proto::RelRoot {
                input: Some(Rel {
                    rel_type: Some(RelType::Read(Box::new(read_rel))),
                }),
                names: vec!["a".to_string(), "b".to_string()],
            })),
        }],
        advanced_extensions: None,
        expected_type_urls: vec![],
    }
}

/// Custom consumer that implements resolve_local_files
struct LocalFilesConsumer {
    extensions: Extensions,
    state: SessionState,
    table: Arc<dyn TableProvider>,
}

#[async_trait]
impl SubstraitConsumer for LocalFilesConsumer {
    async fn resolve_table_ref(
        &self,
        _table_ref: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Return None to test that resolve_local_files is called
        Ok(None)
    }

    async fn resolve_local_files(
        &self,
        _paths: &[String],
        _schema: &DFSchema,
    ) -> Result<Arc<dyn TableProvider>> {
        // Return the pre-configured table
        Ok(Arc::clone(&self.table))
    }

    fn get_extensions(&self) -> &Extensions {
        &self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        &self.state
    }
}

#[tokio::test]
async fn test_local_files_multiple_files() -> Result<()> {
    // Create a plan with multiple files
    let plan = create_local_files_plan(vec![
        (
            PathType::UriFile("".to_string()),
            "file:///path/to/file1.parquet".to_string(),
        ),
        (
            PathType::UriFile("".to_string()),
            "file:///path/to/file2.parquet".to_string(),
        ),
        (
            PathType::UriFile("".to_string()),
            "file:///path/to/file3.parquet".to_string(),
        ),
    ]);

    // Create a consumer that implements resolve_local_files
    let ctx = SessionContext::new();
    let schema = Arc::new(create_test_schema());
    let table = Arc::new(MemTable::try_new(schema, vec![vec![]])?);

    let consumer = LocalFilesConsumer {
        extensions: Extensions::default(),
        state: ctx.state(),
        table,
    };

    // Consume the plan - should succeed with multiple files
    let result =
        datafusion_substrait::logical_plan::consumer::from_substrait_plan_with_consumer(
            &consumer, &plan,
        )
        .await;

    assert!(result.is_ok(), "Should handle multiple files: {result:?}");
    let logical_plan = result.unwrap();

    // Verify the plan structure
    let plan_str = format!("{}", logical_plan.display_indent());
    assert!(
        plan_str.contains("TableScan"),
        "Plan should contain TableScan: {plan_str}",
    );

    Ok(())
}

#[tokio::test]
async fn test_local_files_fallback_behavior() -> Result<()> {
    // Create a plan with single file - use a simple filename without extension
    // to avoid dot being parsed as schema.table separator
    let plan = create_local_files_plan(vec![(
        PathType::UriFile("".to_string()),
        "file:///path/to/testdata".to_string(),
    )]);

    // Create a context with the table registered by filename
    let ctx = SessionContext::new();
    let schema = Arc::new(create_test_schema());
    let table = Arc::new(MemTable::try_new(schema, vec![vec![]])?);
    // Register with the basename that will be extracted from the path ("testdata")
    ctx.register_table("testdata", table)?;

    // Use from_substrait_plan directly - this uses DefaultSubstraitConsumer
    // which doesn't implement resolve_local_files, triggering the fallback
    let result = from_substrait_plan(&ctx.state(), &plan).await;

    assert!(
        result.is_ok(),
        "Should fall back to resolve_table_ref: {result:?}",
    );

    Ok(())
}

#[tokio::test]
async fn test_local_files_path_type_variants() -> Result<()> {
    // Test all PathType variants
    let test_cases = vec![
        (
            PathType::UriPath("".to_string()),
            "file:///path/to/uri_path.parquet",
        ),
        (
            PathType::UriPathGlob("".to_string()),
            "file:///path/to/*.parquet",
        ),
        (
            PathType::UriFile("".to_string()),
            "file:///path/to/uri_file.parquet",
        ),
        (
            PathType::UriFolder("".to_string()),
            "file:///path/to/folder/",
        ),
    ];

    for (path_type, path) in test_cases {
        let plan = create_local_files_plan(vec![(path_type.clone(), path.to_string())]);

        let ctx = SessionContext::new();
        let schema = Arc::new(create_test_schema());
        let table = Arc::new(MemTable::try_new(schema, vec![vec![]])?);

        let consumer = LocalFilesConsumer {
            extensions: Extensions::default(),
            state: ctx.state(),
            table,
        };

        let result =
            datafusion_substrait::logical_plan::consumer::from_substrait_plan_with_consumer(
                &consumer,
                &plan,
            )
            .await;

        assert!(
            result.is_ok(),
            "PathType {path_type:?} with path {path} should work: {result:?}",
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_local_files_no_valid_paths() -> Result<()> {
    // Create a plan with empty items
    let schema = create_test_schema();
    let named_struct = schema_to_named_struct(&schema);

    let read_rel = ReadRel {
        common: None,
        base_schema: Some(named_struct),
        filter: None,
        best_effort_filter: None,
        projection: None,
        advanced_extension: None,
        read_type: Some(ReadType::LocalFiles(LocalFiles {
            items: vec![],
            advanced_extension: None,
        })),
    };

    #[allow(deprecated)]
    let plan = Plan {
        version: None,
        extension_uris: vec![],
        extensions: vec![],
        extension_urns: vec![],
        parameter_bindings: vec![],
        type_aliases: vec![],
        relations: vec![PlanRel {
            rel_type: Some(plan_rel::RelType::Root(substrait::proto::RelRoot {
                input: Some(Rel {
                    rel_type: Some(RelType::Read(Box::new(read_rel))),
                }),
                names: vec!["a".to_string(), "b".to_string()],
            })),
        }],
        advanced_extensions: None,
        expected_type_urls: vec![],
    };

    let ctx = SessionContext::new();

    // This should fail because there are no valid paths
    let result = from_substrait_plan(&ctx.state(), &plan).await;

    assert!(result.is_err(), "Should fail with no valid paths");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("No valid file paths"),
        "Error should mention no valid paths: {err_msg}",
    );

    Ok(())
}

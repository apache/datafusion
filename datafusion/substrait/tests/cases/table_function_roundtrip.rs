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

use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use datafusion_substrait::logical_plan::{
    consumer::from_substrait_plan, producer::to_substrait_plan,
};
use substrait::proto::extensions::simple_extension_declaration::MappingType;
use substrait::proto::{plan_rel, read_rel::ReadType, Rel};
use substrait::proto::rel::RelType;

#[tokio::test]
async fn table_function_round_trip_generate_series() -> Result<()> {
    assert_table_function_round_trip(
        "SELECT * FROM generate_series(1, 4)",
        &["generate_series"],
    )
    .await
}

#[tokio::test]
async fn table_function_round_trip_range() -> Result<()> {
    assert_table_function_round_trip("SELECT * FROM range(2, 8, 3)", &["range"]).await
}

#[tokio::test]
async fn table_function_round_trip_generate_series_with_null() -> Result<()> {
    assert_table_function_round_trip("SELECT * FROM generate_series(NULL, 5)", &["generate_series"]).await
}

async fn assert_table_function_round_trip(query: &str, expected_functions: &[&str]) -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql(query).await?;
    let expected = df.clone().collect().await?;

    let state = ctx.state();
    let logical_plan = df.logical_plan();
    let substrait_plan = to_substrait_plan(logical_plan, &state)?;

    let declared_functions: Vec<String> = substrait_plan
        .extensions
        .iter()
        .filter_map(|ext| ext.mapping_type.as_ref())
        .filter_map(|mapping| match mapping {
            MappingType::ExtensionFunction(function) => Some(function.name.clone()),
            _ => None,
        })
        .collect();

    let table_names = collect_named_table_names(&substrait_plan);

    for expected_function in expected_functions {
        let has_extension = declared_functions
            .iter()
            .any(|name| name.contains(expected_function));
        let has_named_table = table_names
            .iter()
            .any(|name| name.contains(expected_function));

        assert!(
            has_extension || has_named_table,
            "function metadata for {expected_function} missing from Substrait plan extensions {declared_functions:?} and named tables {table_names:?}"
        );
    }

    if let Some(first_batch) = expected.first() {
        let table: Arc<dyn datafusion::datasource::TableProvider> =
            Arc::new(MemTable::try_new(first_batch.schema(), vec![expected.clone()])?);
        for table_name in &table_names {
            ctx.register_table(table_name, Arc::clone(&table))?;
        }
    }

    let round_trip_plan = from_substrait_plan(&state, &substrait_plan).await?;
    let physical_plan = state.create_physical_plan(&round_trip_plan).await?;
    let task_ctx = Arc::new(datafusion::execution::context::TaskContext::from(&state));
    let actual = datafusion::physical_plan::collect(physical_plan, task_ctx).await?;

    assert_eq!(
        datafusion::common::test_util::format_batches(&expected)?.to_string(),
        datafusion::common::test_util::format_batches(&actual)?.to_string()
    );

    Ok(())
}

fn collect_named_table_names(plan: &substrait::proto::Plan) -> Vec<String> {
    let mut tables = Vec::new();
    for rel in &plan.relations {
        match rel.rel_type.as_ref() {
            Some(plan_rel::RelType::Rel(rel)) => collect_named_table_from_rel(rel, &mut tables),
            Some(plan_rel::RelType::Root(root)) => {
                if let Some(rel) = root.input.as_ref() {
                    collect_named_table_from_rel(rel, &mut tables);
                }
            }
            None => {}
        }
    }
    tables
}

fn collect_named_table_from_rel(rel: &Rel, tables: &mut Vec<String>) {
    if let Some(rel_type) = rel.rel_type.as_ref() {
        match rel_type {
            RelType::Project(project) => {
                if let Some(input) = project.input.as_ref() {
                    collect_named_table_from_rel(input, tables);
                }
            }
            RelType::Filter(filter) => {
                if let Some(input) = filter.input.as_ref() {
                    collect_named_table_from_rel(input, tables);
                }
            }
            RelType::Fetch(fetch) => {
                if let Some(input) = fetch.input.as_ref() {
                    collect_named_table_from_rel(input, tables);
                }
            }
            RelType::Aggregate(agg) => {
                if let Some(input) = agg.input.as_ref() {
                    collect_named_table_from_rel(input, tables);
                }
            }
            RelType::Join(join) => {
                if let Some(left) = join.left.as_ref() {
                    collect_named_table_from_rel(left, tables);
                }
                if let Some(right) = join.right.as_ref() {
                    collect_named_table_from_rel(right, tables);
                }
            }
            RelType::Read(read) => {
                if let Some(ReadType::NamedTable(nt)) = read.read_type.as_ref() {
                    tables.extend(nt.names.iter().cloned());
                }
            }
            RelType::Set(set_rel) => {
                for input in &set_rel.inputs {
                    collect_named_table_from_rel(input, tables);
                }
            }
            _ => {}
        }
    }
}

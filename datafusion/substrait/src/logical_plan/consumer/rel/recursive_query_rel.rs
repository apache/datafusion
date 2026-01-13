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

use crate::logical_plan::{
    consumer::SubstraitConsumer, recursive::decode_recursive_query_detail,
};
use datafusion::{
    catalog::cte_worktable::CteWorkTable,
    catalog::default_table_source::DefaultTableSource,
    common::substrait_err,
    logical_expr::{LogicalPlan, RecursiveQuery, TableScan},
};
use std::sync::Arc;
use substrait::proto::ExtensionMultiRel;

/// Validates that the recursive term references the work table via a recursive scan.
///
/// A valid recursive query must have the recursive term reference the work table
/// (identified by `recursive_name`). This is a defensive check to catch invalid plans
/// where the Substrait extension may be malformed.
///
/// Returns `true` if at least one `TableScan` with a `CteWorkTable` matching
/// `recursive_name` is found in the recursive term tree.
fn recursive_term_references_work_table(
    plan: &LogicalPlan,
    recursive_name: &str,
) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => {
            // Check if this is a recursive work-table scan
            is_recursive_work_table_scan(scan, recursive_name)
        }
        _ => {
            // Recursively check all input plans
            plan.inputs()
                .iter()
                .any(|input| recursive_term_references_work_table(input, recursive_name))
        }
    }
}

/// Helper to check if a TableScan references a CteWorkTable with the given name.
fn is_recursive_work_table_scan(scan: &TableScan, recursive_name: &str) -> bool {
    scan.source
        .as_any()
        .downcast_ref::<DefaultTableSource>()
        .and_then(|source| {
            source
                .table_provider
                .as_any()
                .downcast_ref::<CteWorkTable>()
                .map(|table| table.name() == recursive_name)
        })
        .unwrap_or(false)
}

/// Deserializes Substrait ExtensionMultiRel back into DataFusion RecursiveQuery.
///
/// RecursiveQuery is encoded as ExtensionMultiRel with exactly two inputs:
///
/// - `inputs[0]`: static_term
/// - `inputs[1]`: recursive_term
///
/// The detail field contains the encoded name and is_distinct metadata.
pub async fn from_recursive_query_rel(
    consumer: &impl SubstraitConsumer,
    rel: &ExtensionMultiRel,
) -> datafusion::common::Result<LogicalPlan> {
    // Validate structure
    if rel.inputs.len() != 2 {
        return substrait_err!(
            "RecursiveQuery extension must have exactly 2 inputs, found {}",
            rel.inputs.len()
        );
    }

    let Some(detail) = &rel.detail else {
        return substrait_err!("RecursiveQuery extension missing detail");
    };

    // Decode metadata
    let (name, is_distinct) = decode_recursive_query_detail(&detail.value)?;

    // Convert child plans
    let static_term = Arc::new(consumer.consume_rel(&rel.inputs[0]).await?);
    let recursive_term = Arc::new(consumer.consume_rel(&rel.inputs[1]).await?);

    // Validate that the recursive term references the work table to catch invalid plans early.
    // Since Substrait's ExtensionMultiRel is general-purpose, this defensive check ensures
    // the deserialized plan conforms to recursive query semantics.
    if !recursive_term_references_work_table(&recursive_term, &name) {
        return substrait_err!(
            "RecursiveQuery recursive_term must reference the work table '{}', \
             but no such reference was found. This may indicate a malformed Substrait plan.",
            name
        );
    }

    Ok(LogicalPlan::RecursiveQuery(RecursiveQuery {
        name,
        static_term,
        recursive_term,
        is_distinct,
    }))
}

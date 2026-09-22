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

//! One early filter batch for consumers that can turn candidates into scan pruning.

use std::sync::Arc;

use datafusion_common::Result;

use super::FilterExec;
use crate::coop::CooperativeExec;
use crate::execution_plan::plan_contains_expression_id;
use crate::projection::ProjectionExec;
use crate::repartition::RepartitionExec;
use crate::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion_physical_expr::Partitioning;

/// Attach a one-time output threshold only when the dynamic filter reaches a
/// leaf through a single FilterExec and row-preserving, non-buffering wrappers.
/// Round-robin repartition is also allowed below the filter: the filter counts
/// qualifying rows after redistribution, and its output still reaches the producer
/// directly. Above the filter, repartition or other buffering/selective operators
/// can prevent these initial rows reaching the producer promptly and intact.
/// The producer calls this after dynamic filter pushdown has finished, so the
/// expression ID identifies an actual consumer, including remapped expressions.
pub(crate) fn with_startup_filter_output(
    input: &Arc<dyn ExecutionPlan>,
    expression_id: u64,
    rows: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if rows == 0 {
        return Ok(None);
    }
    rewrite(input, expression_id, rows, false)
}

fn rewrite(
    input: &Arc<dyn ExecutionPlan>,
    expression_id: u64,
    rows: usize,
    seen_filter: bool,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(filter) = input.downcast_ref::<FilterExec>() {
        // Fetch already flushes at its own demand. Conservatively exclude a
        // second filter, whose buffering can delay the initial candidates.
        if seen_filter || filter.fetch.is_some() {
            return Ok(None);
        }
        let Some(child) = rewrite(&filter.input, expression_id, rows, true)? else {
            return Ok(None);
        };
        return Ok(Some(Arc::new(FilterExec {
            input: child,
            startup_rows: rows,
            ..filter.clone()
        })));
    }
    let repartition_below_filter = seen_filter
        && input
            .downcast_ref::<RepartitionExec>()
            .is_some_and(|repartition| {
                matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
            });
    if input.is::<ProjectionExec>()
        || input.is::<CooperativeExec>()
        || repartition_below_filter
    {
        let Some(child) = rewrite(input.children()[0], expression_id, rows, seen_filter)?
        else {
            return Ok(None);
        };
        return Ok(Some(Arc::clone(input).replace_children(
            vec![child],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )?));
    }
    Ok((seen_filter
        && input.children().is_empty()
        && plan_contains_expression_id(input, expression_id)?)
    .then(|| Arc::clone(input)))
}

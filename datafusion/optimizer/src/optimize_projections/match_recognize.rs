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

//! MATCH_RECOGNIZE-specific optimization helpers for OptimizeProjections

use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::{internal_datafusion_err, DFSchemaRef, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::match_recognize::MatchRecognizeOutputSpec;
use datafusion_expr::MatchRecognize;

use super::required_indices::RequiredIndices;
use crate::optimize_projections::{
    add_projection_on_top_if_helpful, optimize_projections,
};
use crate::OptimizerConfig;

/// Map MR passthrough prefix positions to positions within the (possibly projected) child schema.
///
/// - passthrough_positions: positions in MR output prefix
/// - passthrough_input_indices: MR prefix position -> original input index
/// - child_projection_indices: indices that define the child schema order (identity if no projection)
fn map_passthrough_positions_to_child_positions(
    passthrough_positions: &[usize],
    passthrough_input_indices: &[usize],
    child_projection_indices: &[usize],
) -> Result<Vec<usize>> {
    use std::collections::HashMap;

    // 1) MR prefix position -> original input index
    let original_input_indices: Vec<usize> = passthrough_positions
        .iter()
        .map(|&pos| {
            passthrough_input_indices.get(pos).copied().ok_or_else(|| {
                internal_datafusion_err!(
                    "Invalid passthrough position {pos} for MatchRecognize"
                )
            })
        })
        .collect::<Result<_>>()?;

    // 2) original input index -> position in child output schema
    let pos_by_idx: HashMap<usize, usize> = child_projection_indices
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();

    original_input_indices
        .into_iter()
        .map(|idx| {
            pos_by_idx.get(&idx).copied().ok_or_else(|| {
                internal_datafusion_err!(
                    "Required passthrough input index {idx} not found in projected set"
                )
            })
        })
        .collect()
}

/// Convert MR prefix positions into original input indices using the passthrough mapping
fn positions_to_input_indices(
    positions: &[usize],
    passthrough_input_indices: &[usize],
) -> Result<Vec<usize>> {
    positions
        .iter()
        .map(|&pos| {
            passthrough_input_indices.get(pos).copied().ok_or_else(|| {
                internal_datafusion_err!(
                    "Invalid passthrough position {pos} for MatchRecognize"
                )
            })
        })
        .collect()
}

/// Prune MR suffix (metadata + classifier bitsets) based on parent-required positions, preserving order
fn prune_mr_suffix(
    suffix_positions: &[usize],
    metadata_columns: &[MrMetadataColumn],
    classifier_bitsets: &[String],
) -> (Vec<MrMetadataColumn>, Vec<String>) {
    let meta_len = metadata_columns.len();
    let mut meta_needed = vec![false; meta_len];
    let mut seen = std::collections::HashSet::new();
    let mut required_bits = Vec::new();

    for &pos in suffix_positions {
        if pos < meta_len {
            meta_needed[pos] = true;
        } else if let Some(sym) = classifier_bitsets.get(pos - meta_len) {
            if seen.insert(sym) {
                required_bits.push(sym.clone());
            }
        }
    }

    let pruned_meta = metadata_columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| meta_needed[i].then_some(*c))
        .collect();
    (pruned_meta, required_bits)
}

fn build_match_recognize_child(
    match_recognize_child: LogicalPlan,
    match_recognize: &MatchRecognize,
    required_indices: &RequiredIndices,
    passthrough_prefix_positions: &[usize],
    suffix_reqs: &RequiredIndices,
    input_schema: &DFSchemaRef,
) -> Result<Transformed<LogicalPlan>> {
    // Optionally add projection above MR input to produce only required columns.
    let child_projection_indices = required_indices.indices();
    let required_exprs = required_indices.get_required_exprs(input_schema);
    let match_recognize_child =
        add_projection_on_top_if_helpful(match_recognize_child, required_exprs)?.data;

    // Map MR prefix positions to positions in child schema
    let mapped_passthrough = map_passthrough_positions_to_child_positions(
        passthrough_prefix_positions,
        &match_recognize.output_spec.passthrough_input_indices,
        child_projection_indices,
    )?;
    let passthrough_required_indices =
        RequiredIndices::new_from_indices(mapped_passthrough);

    // Prune MR suffix preserving order
    let (new_metadata_columns, new_classifier_symbols) = prune_mr_suffix(
        suffix_reqs.indices(),
        &match_recognize.output_spec.metadata_columns,
        &match_recognize.output_spec.classifier_bitset_symbols,
    );

    let spec = MatchRecognizeOutputSpec::new(
        passthrough_required_indices.into_inner(),
        new_metadata_columns,
        new_classifier_symbols,
    );

    MatchRecognize::try_new_with_output_spec(
        Arc::new(match_recognize_child),
        match_recognize.partition_by.clone(),
        match_recognize.order_by.clone(),
        match_recognize.after_skip.clone(),
        match_recognize.rows_per_match.clone(),
        match_recognize.pattern.clone(),
        match_recognize.symbols.clone(),
        match_recognize.defines.clone(),
        spec,
    )
    .map(LogicalPlan::MatchRecognize)
    .map(Transformed::yes)
}

pub(super) fn rewrite_match_recognize(
    match_recognize: MatchRecognize,
    indices: RequiredIndices,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let input_schema = Arc::clone(match_recognize.input.schema());
    // Split outputs into MR prefix (passthrough) vs MR suffix (metadata + classifier bitsets)
    let num_passthrough = match_recognize.output_spec.passthrough_input_indices.len();
    let (child_reqs, suffix_reqs) = indices.split_off(num_passthrough);

    // MR input requirements: passthrough referenced by parent + MR clauses
    let passthrough_prefix_positions = child_reqs.indices().to_vec();
    let base_passthrough_input_indices = positions_to_input_indices(
        &passthrough_prefix_positions,
        &match_recognize.output_spec.passthrough_input_indices,
    )?;

    let required_indices =
        RequiredIndices::new_from_indices(base_passthrough_input_indices)
            .with_exprs(&input_schema, &match_recognize.partition_by)
            .with_exprs(
                &input_schema,
                match_recognize.order_by.iter().map(|se| &se.expr),
            )
            .with_exprs(
                &input_schema,
                match_recognize.defines.iter().map(|(expr, _)| expr),
            );

    let input_owned = Arc::unwrap_or_clone(Arc::clone(&match_recognize.input));
    optimize_projections(input_owned, config, required_indices.clone())?.transform_data(
        |child| {
            build_match_recognize_child(
                child,
                &match_recognize,
                &required_indices,
                &passthrough_prefix_positions,
                &suffix_reqs,
                &input_schema,
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::vec;

    use crate::test::test_table_scan;
    use datafusion_common::Result;
    use datafusion_expr::match_recognize::{
        AfterMatchSkip, Pattern, RowsPerMatch, Symbol,
    };
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, LogicalPlan,
    };

    #[test]
    fn test_match_recognize() -> Result<()> {
        let table_scan = test_table_scan()?;

        // Create a simple MatchRecognize plan
        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")],
            vec![col("b").sort(true, true)],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            vec!["A".to_string()],
            vec![(col("a").gt(lit(0)), "A".to_string())],
        )?;

        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("a"), col("__mr_match_number")])?
            .build()?;

        // The optimization should add a projection on top of the input to only include
        // the columns needed by partition_by, order_by, defines, and measures
        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: partition_by=[a] order_by=[b ASC NULLS FIRST] after_skip=PAST_LAST_ROW pattern=[A] symbols=[A] rows_per_match=ALL_ROWS defines=[A: a > Int32(0)] output={passthrough_columns=[test.a], metadata=[__mr_match_number]}
          TableScan: test projection=[a, b]
        "
        )
    }
}

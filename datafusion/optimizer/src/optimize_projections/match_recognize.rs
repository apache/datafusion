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
//!
//! This module handles projection optimization for MATCH_RECOGNIZE operations.
//! MATCH_RECOGNIZE output consists of two parts:
//! 1. Passthrough columns: Original input columns that are passed through unchanged
//! 2. Generated columns: Metadata (like match numbers) and classifier bitsets
//!
//! The optimization tries to:
//! - Only include required passthrough columns from the input
//! - Only include required metadata and classifier columns in the output
//! - Add projections on the input when beneficial

use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::{internal_datafusion_err, Column, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::match_recognize::MatchRecognizeOutputSpec;
use datafusion_expr::MatchRecognize;

use super::required_indices::RequiredIndices;
use crate::optimize_projections::{
    add_projection_on_top_if_helpful, optimize_projections,
};
use crate::OptimizerConfig;

/// Main entry point for optimizing MATCH_RECOGNIZE operations.
///
/// The optimization process:
/// 1. Determines which output columns are actually required by the parent operation
/// 2. Splits these into passthrough columns (from input) and generated columns
/// 3. Calculates the minimal set of input columns needed for both output and internal operations
/// 4. Optimizes the child plan to only produce required columns
/// 5. Creates a new MATCH_RECOGNIZE with optimized column mappings
pub(super) fn rewrite_match_recognize(
    match_recognize: MatchRecognize,
    required_indices: RequiredIndices,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let input_schema = Arc::clone(match_recognize.input.schema());
    let output_spec = &match_recognize.output_spec;
    let passthrough_input_indices = &output_spec.passthrough_input_indices;
    let metadata_columns = &output_spec.metadata_columns;
    let classifier_bitset_symbols = &output_spec.classifier_bitset_symbols;
    let num_passthrough_columns = passthrough_input_indices.len();

    // Step 1: Split parent's requirements into passthrough vs generated columns
    let (passthrough_reqs, generated_reqs) =
        required_indices.split_off(num_passthrough_columns);
    let required_passthrough_output_positions = passthrough_reqs.indices().to_vec();

    // Step 2: Map required passthrough positions to original input column indices
    // This tells us which actual input columns are needed for passthrough
    let required_input_column_indices: Vec<usize> = required_passthrough_output_positions
        .iter()
        .map(|&position| {
            passthrough_input_indices
                .get(position)
                .copied()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Invalid passthrough position {position} for MatchRecognize"
                    )
                })
        })
        .collect::<Result<_>>()?;

    // Step 3: Add columns needed by MATCH_RECOGNIZE clauses (partition_by, order_by, defines)
    let child_required_indices =
        RequiredIndices::new_from_indices(required_input_column_indices)
            .with_exprs(&input_schema, &match_recognize.partition_by)
            .with_exprs(
                &input_schema,
                match_recognize
                    .order_by
                    .iter()
                    .map(|sort_expr| &sort_expr.expr),
            )
            .with_exprs(
                &input_schema,
                match_recognize.defines.iter().map(|ne| &ne.expr),
            );

    // Step 4: Optimize the child plan with these requirements
    optimize_projections(
        Arc::unwrap_or_clone(Arc::clone(&match_recognize.input)),
        config,
        child_required_indices.clone(),
    )?.transform_data(
        |optimized_child_plan| {
            // Step 5: Add projection on child if it helps
            let required_exprs = child_required_indices.get_required_exprs(&input_schema);
            let child_plan = add_projection_on_top_if_helpful(
                optimized_child_plan,
                required_exprs,
            )?.data;

            // Step 6: Map passthrough columns to positions in the actual child output
            // The child may have pruned columns and/or preserved an existing reorder.
            // Build the mapping by inspecting the child's schema rather than relying on
            // the (sorted) `RequiredIndices` order.
            let child_schema = child_plan.schema();
            // Map each required passthrough column to its position in child output
            let mapped_passthrough_indices: Vec<usize> =
                required_passthrough_output_positions
                    .iter()
                    .map(|&position| {
                        let input_index = passthrough_input_indices[position];
                        let col = Column::from(input_schema.qualified_field(input_index));
                        child_schema
                            .maybe_index_of_column(&col)
                            .ok_or_else(|| {
                                internal_datafusion_err!(
                                    "Required input index {input_index} not found in child output"
                                )
                            })
                    })
                    .collect::<Result<_>>()?;

            // Step 7: Filter generated columns (metadata + classifiers) to only required ones
            let num_metadata_columns = metadata_columns.len();

            let mut required_metadata_cols = Vec::new();
            let mut required_classifier_symbols = Vec::new();
            let mut seen_classifiers = std::collections::HashSet::new();

            for &position in generated_reqs.indices() {
                if position < num_metadata_columns {
                    // This is a metadata column
                    if let Some(&metadata_col) = metadata_columns.get(position) {
                        required_metadata_cols.push(metadata_col);
                    }
                } else {
                    // This is a classifier column
                    let classifier_idx = position - num_metadata_columns;
                    if let Some(symbol) = classifier_bitset_symbols.get(classifier_idx) {
                        if seen_classifiers.insert(symbol) {
                            required_classifier_symbols.push(symbol.clone());
                        }
                    }
                }
            }

            // Step 8: Build the optimized output specification
            let optimized_output_spec = MatchRecognizeOutputSpec::new(
                mapped_passthrough_indices,
                required_metadata_cols,
                required_classifier_symbols,
            );

            // Step 9: Create the new optimized MATCH_RECOGNIZE
            MatchRecognize::try_new(
                Arc::new(child_plan),
                match_recognize.partition_by.clone(),
                match_recognize.order_by.clone(),
                match_recognize.after_skip.clone(),
                match_recognize.rows_per_match.clone(),
                match_recognize.pattern.clone(),
                match_recognize.symbols.clone(),
                match_recognize.defines.clone(),
                optimized_output_spec,
            )
            .map(LogicalPlan::MatchRecognize)
            .map(Transformed::yes)
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
        col, lit, logical_plan::builder::LogicalPlanBuilder, logical_plan::NamedExpr,
        LogicalPlan,
    };

    #[test]
    fn test_match_recognize() -> Result<()> {
        // Set up test table with columns [a, b, c]
        let table_scan = test_table_scan()?;

        // Create MATCH_RECOGNIZE that:
        // - Partitions by column 'a'
        // - Orders by column 'b'
        // - Defines pattern 'A' where a > 0
        // - Outputs all passthrough columns plus metadata
        let symbols = vec!["A".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")],                  // partition_by
            vec![col("b").sort(true, true)], // order_by
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: col("a").gt(lit(0)),
                name: "A".to_string(),
            }], // defines: A is where a > 0
            output_spec,
        )?;

        // Parent operation only needs column 'a' and the match number metadata
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("a"), col("__mr_match_number")])?
            .build()?;

        // Verify optimization: should project only [a, b] from input because:
        // - 'a' is needed for passthrough and partition_by
        // - 'b' is needed for order_by
        // - 'c' is not needed and should be eliminated
        // - metadata __mr_match_number should be preserved in output
        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: partition_by=[a] order_by=[b ASC NULLS FIRST] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A] rows_per_match=[ONE ROW PER MATCH] defines=[A: a > Int32(0)] output={passthrough_columns=[test.a], metadata=[__mr_match_number]}
          TableScan: test projection=[a, b]
        "
        )
    }

    #[test]
    fn test_match_recognize_passthrough_only() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        // One symbol with trivial DEFINE to avoid pulling extra columns
        let symbols = vec!["A".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![],                          // partition_by
            vec![col("b").sort(true, true)], // order_by ensures `b` is needed
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: lit(true),
                name: "A".to_string(),
            }], // defines: trivial
            output_spec,
        )?;

        // Parent only needs passthrough column 'b'
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: order_by=[b ASC NULLS FIRST] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A] rows_per_match=[ONE ROW PER MATCH] defines=[A: Boolean(true)] output={passthrough_columns=[test.b]}
          TableScan: test projection=[b]
        "
        )
    }

    #[test]
    fn test_match_recognize_metadata_only() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        // Define uses `c`; partition/order require `a` and `b`
        let symbols = vec!["A".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")],                  // partition_by
            vec![col("b").sort(true, true)], // order_by
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: col("c").gt(lit(0)),
                name: "A".to_string(),
            }], // defines uses `c`
            output_spec,
        )?;

        // Parent only needs one metadata column
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("__mr_match_sequence_number")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: partition_by=[a] order_by=[b ASC NULLS FIRST] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A] rows_per_match=[ONE ROW PER MATCH] defines=[A: c > Int32(0)] output={metadata=[__mr_match_sequence_number]}
          TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn test_match_recognize_classifier_only_single() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        // Declare two symbols, trivial DEFINE's; partition requires `a`
        let symbols = vec!["A".to_string(), "B".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")], // partition_by requires `a`
            vec![],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![
                NamedExpr {
                    expr: lit(true),
                    name: "A".to_string(),
                },
                NamedExpr {
                    expr: lit(true),
                    name: "B".to_string(),
                },
            ],
            output_spec,
        )?;

        // Parent only needs classifier bitset for symbol 'B'
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("__mr_classifier_b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: partition_by=[a] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A,B] rows_per_match=[ONE ROW PER MATCH] defines=[A: Boolean(true), B: Boolean(true)] output={classifier_flags=[B]}
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_match_recognize_mixed_requirements() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        let symbols = vec!["A".to_string(), "B".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")],                  // partition_by
            vec![col("b").sort(true, true)], // order_by
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: col("a").gt(lit(0)),
                name: "A".to_string(),
            }], // defines uses `a`
            output_spec,
        )?;

        // Parent needs: passthrough 'c', one metadata, and one classifier bitset
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![
                col("c"),
                col("__mr_match_number"),
                col("__mr_classifier_b"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: partition_by=[a] order_by=[b ASC NULLS FIRST] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A,B] rows_per_match=[ONE ROW PER MATCH] defines=[A: a > Int32(0)] output={passthrough_columns=[test.c], metadata=[__mr_match_number], classifier_flags=[B]}
          TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn test_match_recognize_classifier_dedup() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        // Single symbol 'A' declared
        let symbols = vec!["A".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![col("a")],
            vec![],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: lit(true),
                name: "A".to_string(),
            }],
            output_spec,
        )?;

        // Project the same classifier flag twice, with different names to avoid
        // duplicate-expression-name error; output spec should deduplicate to a single flag
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![
                col("__mr_classifier_a"),
                col("__mr_classifier_a").alias("dup"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: __mr_classifier_a, __mr_classifier_a AS dup
          MatchRecognize: partition_by=[a] after_skip=[PAST LAST ROW] pattern=[A] symbols=[A] rows_per_match=[ONE ROW PER MATCH] defines=[A: Boolean(true)] output={classifier_flags=[A]}
            TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_match_recognize_define_columns_included() -> Result<()> {
        // Input schema: [a, b, c]
        let table_scan = test_table_scan()?;

        let symbols = vec!["A".to_string()];
        let output_spec =
            MatchRecognizeOutputSpec::full_from(table_scan.schema(), &symbols);

        let match_recognize = MatchRecognize::try_new(
            Arc::new(table_scan.clone()),
            vec![],
            vec![],
            AfterMatchSkip::PastLastRow,
            RowsPerMatch::OneRow,
            Pattern::Symbol(Symbol::Named("A".to_string())),
            symbols,
            vec![NamedExpr {
                expr: col("c").gt(lit(0)),
                name: "A".to_string(),
            }], // DEFINE references `c`
            output_spec,
        )?;

        // Parent needs only passthrough 'a', but DEFINE requires `c` as well
        let plan = LogicalPlanBuilder::from(LogicalPlan::MatchRecognize(match_recognize))
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        MatchRecognize: after_skip=[PAST LAST ROW] pattern=[A] symbols=[A] rows_per_match=[ONE ROW PER MATCH] defines=[A: c > Int32(0)] output={passthrough_columns=[test.a]}
          TableScan: test projection=[a, c]
        "
        )
    }
}

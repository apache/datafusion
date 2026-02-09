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

//! Two-pass optimizer pipeline that pushes cheap expressions (like struct field
//! access `user['status']`) closer to data sources, enabling early data reduction
//! and source-level optimizations (e.g., Parquet column pruning). See
//! [`ExtractLeafExpressions`] (pass 1) and [`PushDownLeafProjections`] (pass 2).

use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::logical_plan::LogicalPlan;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Extracts `MoveTowardsLeafNodes` sub-expressions from non-projection nodes
/// into **extraction projections** (pass 1 of 2).
///
/// This handles Filter, Sort, Limit, Aggregate, and Join nodes. For Projection
/// nodes, extraction and pushdown are handled by [`PushDownLeafProjections`].
///
/// # Key Concepts
///
/// **Extraction projection**: a projection inserted *below* a node that
/// pre-computes a cheap expression and exposes it under an alias
/// (`__datafusion_extracted_N`). The parent node then references the alias
/// instead of the original expression.
///
/// **Recovery projection**: a projection inserted *above* a node to restore
/// the original output schema when extraction changes it.
/// Schema-preserving nodes (Filter, Sort, Limit) gain extra columns from
/// the extraction projection that bubble up; the recovery projection selects
/// only the original columns to hide the extras.
///
/// # Example
///
/// Given a filter with a struct field access:
///
/// ```text
/// Filter: user['status'] = 'active'
///   TableScan: t [id, user]
/// ```
///
/// This rule:
/// 1. Inserts an **extraction projection** below the filter:
/// 2. Adds a **recovery projection** above to hide the extra column:
///
/// ```text
/// Projection: id, user                                                        <-- recovery projection
///   Filter: __datafusion_extracted_1 = 'active'
///     Projection: user['status'] AS __datafusion_extracted_1, id, user         <-- extraction projection
///       TableScan: t [id, user]
/// ```
///
/// **Important:** The `PushDownFilter` rule is aware of projections created by this rule
/// and will not push filters through them. See `is_extracted_expr_projection` in utils.rs.
#[derive(Default, Debug)]
pub struct ExtractLeafExpressions {}

impl ExtractLeafExpressions {
    /// Create a new [`ExtractLeafExpressions`]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExtractLeafExpressions {
    fn name(&self) -> &str {
        "extract_leaf_expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }
}

// =============================================================================
// Pass 2: PushDownLeafProjections
// =============================================================================

/// Pushes extraction projections down through schema-preserving nodes towards
/// leaf nodes (pass 2 of 2, after [`ExtractLeafExpressions`]).
///
/// Handles two types of projections:
/// - **Pure extraction projections** (all `__datafusion_extracted` aliases + columns):
///   pushes through Filter/Sort/Limit, merges into existing projections, or routes
///   into multi-input node inputs (Join, SubqueryAlias, etc.)
/// - **Mixed projections** (user projections containing `MoveTowardsLeafNodes`
///   sub-expressions): splits into a recovery projection + extraction projection,
///   then pushes the extraction projection down.
///
/// # Example: Pushing through a Filter
///
/// After pass 1, the extraction projection sits directly below the filter:
/// ```text
/// Projection: id, user                                                       <-- recovery
///   Filter: __extracted_1 = 'active'
///     Projection: user['status'] AS __extracted_1, id, user                   <-- extraction
///       TableScan: t [id, user]
/// ```
///
/// Pass 2 pushes the extraction projection through the recovery and filter,
/// and a subsequent `OptimizeProjections` pass removes the (now-redundant)
/// recovery projection:
/// ```text
/// Filter: __extracted_1 = 'active'
///   Projection: user['status'] AS __extracted_1, id, user                     <-- extraction (pushed down)
///     TableScan: t [id, user]
/// ```
#[derive(Default, Debug)]
pub struct PushDownLeafProjections {}

impl PushDownLeafProjections {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownLeafProjections {
    fn name(&self) -> &str {
        "push_down_leaf_projections"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::optimize_projections::OptimizeProjections;
    use crate::test::*;
    use crate::{OptimizerContext, assert_optimized_plan_eq_snapshot};
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, col, lit, logical_plan::builder::LogicalPlanBuilder,
    };
    use datafusion_expr::{Expr, ExpressionPlacement};

    /// A mock UDF that simulates a leaf-pushable function like `get_field`.
    /// It returns `MoveTowardsLeafNodes` when its first argument is Column or MoveTowardsLeafNodes.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockLeafFunc {
        signature: Signature,
    }

    impl MockLeafFunc {
        fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Any(2),
                    datafusion_expr::Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MockLeafFunc {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "mock_leaf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!("This is only used for testing optimization")
        }

        fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
            // Return MoveTowardsLeafNodes if first arg is Column or MoveTowardsLeafNodes
            // (like get_field does)
            match args.first() {
                Some(ExpressionPlacement::Column)
                | Some(ExpressionPlacement::MoveTowardsLeafNodes) => {
                    ExpressionPlacement::MoveTowardsLeafNodes
                }
                _ => ExpressionPlacement::KeepInPlace,
            }
        }
    }

    fn mock_leaf(expr: Expr, name: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(MockLeafFunc::new())),
            vec![expr, lit(name)],
        ))
    }

    // =========================================================================
    // Test assertion macros - 4 stages of the optimization pipeline
    // All stages run OptimizeProjections first to match the actual rule layout.
    // =========================================================================

    /// Stage 1: Original plan with OptimizeProjections (baseline without extraction).
    /// This shows the plan as it would be without our extraction rules.
    macro_rules! assert_original_plan {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(OptimizeProjections::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 2: OptimizeProjections + ExtractLeafExpressions (shows extraction projections).
    macro_rules! assert_after_extract {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 3: OptimizeProjections + Extract + PushDown (extraction pushed through schema-preserving nodes).
    macro_rules! assert_after_pushdown {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
                Arc::new(PushDownLeafProjections::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 4: Full pipeline - OptimizeProjections + Extract + PushDown + OptimizeProjections (final).
    macro_rules! assert_optimized {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
                Arc::new(PushDownLeafProjections::new()),
                Arc::new(OptimizeProjections::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    #[test]
    fn test_extract_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .select(vec![
                table_scan
                    .schema()
                    .index_of_column_by_name(None, "id")
                    .unwrap(),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        // Note: An outer projection is added to preserve the original schema
        assert_optimized!(plan, @r#"
        Projection: test.id
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)
    }

    #[test]
    fn test_no_extraction_for_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1)))?
            .build()?;

        assert_original_plan!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")?;

        assert_after_extract!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")?;

        // No extraction should happen for simple columns
        assert_optimized!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")
    }

    #[test]
    fn test_extract_from_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Projection expressions with MoveTowardsLeafNodes are extracted
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_extract_from_projection_with_subexpression() -> Result<()> {
        // Extraction happens on sub-expressions within projection
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                mock_leaf(col("user"), "name")
                    .is_not_null()
                    .alias("has_name"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)?;

        // The mock_leaf sub-expression is extracted
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_no_extraction_for_column() -> Result<()> {
        // Projections with only columns don't need extraction
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_original_plan!(plan, @"TableScan: test projection=[a, b]")?;

        assert_after_extract!(plan, @"TableScan: test projection=[a, b]")?;

        // No extraction needed
        assert_optimized!(plan, @"TableScan: test projection=[a, b]")
    }

    #[test]
    fn test_filter_with_deduplication() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field_access = mock_leaf(col("user"), "name");
        // Filter with the same expression used twice
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                field_access
                    .clone()
                    .is_not_null()
                    .and(field_access.is_null()),
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL AND mock_leaf(test.user, Utf8("name")) IS NULL
          TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL AND mock_leaf(test.user, Utf8("name")) IS NULL
          TableScan: test projection=[id, user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL AND mock_leaf(test.user, Utf8("name")) IS NULL
          TableScan: test projection=[id, user]
        "#)
    }

    #[test]
    fn test_already_leaf_expression_in_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // A bare mock_leaf expression is already MoveTowardsLeafNodes
        // When compared to a literal, the comparison is KeepInPlace so extraction happens
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "name").eq(lit("test")))?
            .build()?;

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) = Utf8("test")
          TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) = Utf8("test")
          TableScan: test projection=[id, user]
        "#)?;

        assert_optimized!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) = Utf8("test")
          TableScan: test projection=[id, user]
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_group_by() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![mock_leaf(col("user"), "status")], vec![count(lit(1))])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          TableScan: test projection=[user]
        "#)?;

        // Group-by expression is MoveTowardsLeafNodes, so it gets extracted
        // Recovery projection restores original schema on top
        assert_optimized!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_args() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Use count(mock_leaf(...)) since count works with any type
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("user")],
                vec![count(mock_leaf(col("user"), "value"))],
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value")))]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value")))]]
          TableScan: test projection=[user]
        "#)?;

        // Aggregate argument is MoveTowardsLeafNodes, so it gets extracted
        // Recovery projection restores original schema on top
        assert_optimized!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value")))]]
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_with_filter_combined() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        // Both filter and projection extractions are pushed to a single
        // extraction projection above the TableScan.
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_preserves_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name").alias("username")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)?;

        // Original alias "username" should be preserved in outer projection
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)
    }

    /// Test: Projection with different field than Filter
    /// SELECT id, s['label'] FROM t WHERE s['value'] > 150
    /// Both s['label'] and s['value'] should be in a single extraction projection.
    #[test]
    fn test_projection_different_field_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter uses s['value']
            .filter(mock_leaf(col("user"), "value").gt(lit(150)))?
            // Projection uses s['label'] (different field)
            .project(vec![col("user"), mock_leaf(col("user"), "label")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Filter: mock_leaf(test.user, Utf8("value")) > Int32(150)
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Filter: mock_leaf(test.user, Utf8("value")) > Int32(150)
            TableScan: test projection=[user]
        "#)?;

        // Both extractions merge into a single projection above TableScan.
        assert_optimized!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Filter: mock_leaf(test.user, Utf8("value")) > Int32(150)
            TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_deduplication() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field = mock_leaf(col("user"), "name");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![field.clone(), field.clone().alias("name2")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)
    }

    // =========================================================================
    // Additional tests for code coverage
    // =========================================================================

    /// Extractions push through Sort nodes to reach the TableScan.
    /// Covers: find_extraction_target Sort branch, rebuild_path Sort
    #[test]
    fn test_extract_through_sort() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection -> Sort -> TableScan
        // The projection's extraction should push through Sort
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("user").sort(true, true)])?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        // Stage 1: Baseline (no extraction rules)
        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Stage 2: After extraction - projection untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Stage 3: After pushdown - extraction pushed through Sort
        assert_after_pushdown!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Stage 4: Final optimized - projection columns resolved
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)
    }

    /// Extractions push through Limit nodes to reach the TableScan.
    /// Covers: find_extraction_target Limit branch, rebuild_path Limit
    #[test]
    fn test_extract_through_limit() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection -> Limit -> TableScan
        // The projection's extraction should push through Limit
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(10))?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        // Stage 1: Baseline (no extraction rules)
        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Stage 2: After extraction - projection untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Stage 3: After pushdown - extraction pushed through Limit
        assert_after_pushdown!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Stage 4: Final optimized - projection columns resolved
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)
    }

    /// Aliased aggregate functions like count(...).alias("cnt") are handled.
    /// Covers: Expr::Alias branch in extract_from_aggregate_args
    #[test]
    fn test_extract_from_aliased_aggregate() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Use count(mock_leaf(...)).alias("cnt") to trigger Alias branch
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("user")],
                vec![count(mock_leaf(col("user"), "value")).alias("cnt")],
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value"))) AS cnt]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value"))) AS cnt]]
          TableScan: test projection=[user]
        "#)?;

        // The aliased aggregate should have its inner expression extracted
        assert_optimized!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value"))) AS cnt]]
          TableScan: test projection=[user]
        "#)
    }

    /// Aggregates with no MoveTowardsLeafNodes expressions return unchanged.
    /// Covers: early return in extract_from_aggregate when no extractions
    #[test]
    fn test_aggregate_no_extraction() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan()?;
        // GROUP BY col (no MoveTowardsLeafNodes expressions)
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count(col("b"))])?
            .build()?;

        assert_original_plan!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")
    }

    /// Projections containing extracted expression aliases are skipped (already extracted).
    /// Covers: is_extracted_expr_projection skip in extract_from_projection
    #[test]
    fn test_skip_extracted_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Create a projection that already contains an extracted expression alias
        // This simulates what happens after extraction has already occurred
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                mock_leaf(col("user"), "name").alias("__datafusion_extracted_manual"),
                col("user"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)?;

        // Should return unchanged because projection already contains extracted expressions
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)
    }

    /// Multiple extractions merge into a single extracted expression projection.
    /// Covers: merge_into_extracted_projection for schema-preserving nodes
    #[test]
    fn test_merge_into_existing_extracted_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Filter -> existing extracted expression Projection -> TableScan
        // We need to manually build the tree where Filter extracts
        // into an existing extracted expression projection
        let plan = LogicalPlanBuilder::from(table_scan)
            // First extraction from inner filter creates __datafusion_extracted_1
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            // Second filter extraction should merge into existing extracted projection
            .filter(mock_leaf(col("user"), "name").is_not_null())?
            .build()?;

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        // Both extractions end up merged into a single extraction projection above the TableScan
        assert_optimized!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)
    }

    /// Extractions push through passthrough projections (columns only).
    /// Covers: passthrough projection handling in rebuild_path
    #[test]
    fn test_extract_through_passthrough_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection(with extraction) -> Projection(cols only) -> TableScan
        // The passthrough projection should be rebuilt with all columns
        let plan = LogicalPlanBuilder::from(table_scan)
            // Inner passthrough projection (only column references)
            .project(vec![col("user")])?
            // Outer projection with extraction
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Extraction should push through the passthrough projection
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)
    }

    /// Projections with aliased columns (nothing to extract) return unchanged.
    /// Covers: is_fully_extracted early return in extract_from_projection
    #[test]
    fn test_projection_early_return_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Projection with aliased column - nothing to extract
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("x"), col("b")])?
            .build()?;

        assert_original_plan!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")
    }

    /// Projections with arithmetic expressions but no MoveTowardsLeafNodes return unchanged.
    /// This hits the early return when has_extractions is false (after checking expressions).
    #[test]
    fn test_projection_with_arithmetic_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Projection with arithmetic expression - not is_fully_extracted
        // but also has no MoveTowardsLeafNodes expressions
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![(col("a") + col("b")).alias("sum")])?
            .build()?;

        assert_original_plan!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")
    }

    /// Aggregate extractions merge into existing extracted projection created by Filter.
    /// Covers: merge_into_extracted_projection call in extract_from_aggregate
    #[test]
    fn test_aggregate_merge_into_extracted_projection() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Filter creates extracted projection, then Aggregate merges into it
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter extracts first -> creates extracted projection
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            // Aggregate extracts -> should merge into existing extracted projection
            .aggregate(vec![mock_leaf(col("user"), "name")], vec![count(lit(1))])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        // Both extractions should be in a single extracted projection
        assert_optimized!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)
    }

    /// Merging adds new pass-through columns not in the existing extracted projection.
    /// When second filter references different column than first, it gets added during merge.
    #[test]
    fn test_merge_with_new_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Filter on column 'a' creates extracted projection with column 'a'
        // Then filter on column 'b' needs to add column 'b' during merge
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter extracts from column 'a'
            .filter(mock_leaf(col("a"), "x").eq(lit(1)))?
            // Filter extracts from column 'b' - needs to add 'b' to existing projection
            .filter(mock_leaf(col("b"), "y").eq(lit(2)))?
            .build()?;

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.b, Utf8("y")) = Int32(2)
          Filter: mock_leaf(test.a, Utf8("x")) = Int32(1)
            TableScan: test projection=[a, b, c]
        "#)?;

        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(test.b, Utf8("y")) = Int32(2)
          Filter: mock_leaf(test.a, Utf8("x")) = Int32(1)
            TableScan: test projection=[a, b, c]
        "#)?;

        // Both extractions should be in a single extracted projection,
        // with both 'a' and 'b' columns passed through
        assert_optimized!(plan, @r#"
        Filter: mock_leaf(test.b, Utf8("y")) = Int32(2)
          Filter: mock_leaf(test.a, Utf8("x")) = Int32(1)
            TableScan: test projection=[a, b, c]
        "#)
    }

    // =========================================================================
    // Join extraction tests
    // =========================================================================

    /// Create a second table scan with struct field for join tests
    fn test_table_scan_with_struct_named(name: &str) -> Result<LogicalPlan> {
        use arrow::datatypes::Schema;
        let schema = Schema::new(test_table_scan_with_struct_fields());
        datafusion_expr::logical_plan::table_scan(Some(name), &schema, None)?.build()
    }

    /// Extraction from equijoin keys (`on` expressions).
    /// Each key expression is routed to its respective side.
    #[test]
    fn test_extract_from_join_on() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join on mock_leaf(left.user, "id") = mock_leaf(right.user, "id")
        let plan = LogicalPlanBuilder::from(left)
            .join_with_expr_keys(
                right,
                JoinType::Inner,
                (
                    vec![mock_leaf(col("user"), "id")],
                    vec![mock_leaf(col("user"), "id")],
                ),
                None,
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        // Both left and right keys should be extracted into their respective sides
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)
    }

    /// Extraction from non-equi join filter.
    /// Filter sub-expressions are routed based on column references.
    #[test]
    fn test_extract_from_join_filter() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with filter: mock_leaf(left.user, "status") = 'active'
        let plan = LogicalPlanBuilder::from(left)
            .join_on(
                right,
                JoinType::Inner,
                vec![
                    col("test.user").eq(col("right.user")),
                    mock_leaf(col("test.user"), "status").eq(lit("active")),
                ],
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        // Left-side expression should be extracted to left input
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)
    }

    /// Extraction from both left and right sides of a join.
    /// Tests that expressions are correctly routed to each side.
    #[test]
    fn test_extract_from_join_both_sides() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with filters on both sides
        let plan = LogicalPlanBuilder::from(left)
            .join_on(
                right,
                JoinType::Inner,
                vec![
                    col("test.user").eq(col("right.user")),
                    mock_leaf(col("test.user"), "status").eq(lit("active")),
                    mock_leaf(col("right.user"), "role").eq(lit("admin")),
                ],
            )?
            .build()?;

        assert_original_plan!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active") AND mock_leaf(right.user, Utf8("role")) = Utf8("admin")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active") AND mock_leaf(right.user, Utf8("role")) = Utf8("admin")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        // Each side should have its own extraction projection
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active") AND mock_leaf(right.user, Utf8("role")) = Utf8("admin")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)
    }

    /// Join with no MoveTowardsLeafNodes expressions returns unchanged.
    #[test]
    fn test_extract_from_join_no_extraction() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan()?;
        let right = test_table_scan_with_name("right")?;

        // Simple equijoin on columns (no MoveTowardsLeafNodes expressions)
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a"], vec!["a"]), None)?
            .build()?;

        assert_original_plan!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")?;

        assert_after_extract!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")
    }

    /// Join followed by filter with extraction.
    /// Tests extraction from filter above a join that also has extractions.
    #[test]
    fn test_extract_from_filter_above_join() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with extraction in on clause, then filter with extraction
        let plan = LogicalPlanBuilder::from(left)
            .join_with_expr_keys(
                right,
                JoinType::Inner,
                (
                    vec![mock_leaf(col("user"), "id")],
                    vec![mock_leaf(col("user"), "id")],
                ),
                None,
            )?
            .filter(mock_leaf(col("test.user"), "status").eq(lit("active")))?
            .build()?;

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // Join keys are extracted to respective sides
        // Filter expression is now pushed through the Join into the left input
        // (merges with the existing extraction projection on that side)
        assert_optimized!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)
    }

    /// Extraction projection (get_field in SELECT) above a Join pushes into
    /// the correct input side.
    #[test]
    fn test_extract_projection_above_join() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // SELECT mock_leaf(test.user, "status"), mock_leaf(right.user, "role")
        // FROM test JOIN right ON test.id = right.id
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["id"], vec!["id"]), None)?
            .project(vec![
                mock_leaf(col("test.user"), "status"),
                mock_leaf(col("right.user"), "role"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("status")), mock_leaf(right.user, Utf8("role"))
          Inner Join: test.id = right.id
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // After extraction, projection is untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("status")), mock_leaf(right.user, Utf8("role"))
          Inner Join: test.id = right.id
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // After optimization, extraction projections push through the Join
        // into respective input sides (only id needed as passthrough for join key)
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("status")), mock_leaf(right.user, Utf8("role"))
          Inner Join: test.id = right.id
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)
    }

    // =========================================================================
    // Column-rename through intermediate node tests
    // =========================================================================

    /// Projection with leaf expr above Filter above renaming Projection.
    /// Tests that column refs are resolved through the rename in
    /// build_extraction_projection (extract_from_projection path).
    #[test]
    fn test_extract_through_filter_with_column_rename() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(col("x").is_not_null())?
            .project(vec![mock_leaf(col("x"), "a")])?
            .build()?;
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a"))
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a"))
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)
    }

    /// Same as above but with a partial extraction (leaf + arithmetic).
    #[test]
    fn test_extract_partial_through_filter_with_column_rename() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(col("x").is_not_null())?
            .project(vec![mock_leaf(col("x"), "a").is_not_null()])?
            .build()?;
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a")) IS NOT NULL
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a")) IS NOT NULL
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)
    }

    /// Tests merge_into_extracted_projection path (schema-preserving extraction)
    /// through a renaming projection.
    #[test]
    fn test_extract_from_filter_above_renaming_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(mock_leaf(col("x"), "a").eq(lit("active")))?
            .build()?;
        assert_after_extract!(plan, @r#"
        Filter: mock_leaf(x, Utf8("a")) = Utf8("active")
          Projection: test.user AS x
            TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
        Filter: mock_leaf(x, Utf8("a")) = Utf8("active")
          Projection: test.user AS x
            TableScan: test projection=[user]
        "#)
    }

    // =========================================================================
    // SubqueryAlias extraction tests
    // =========================================================================

    /// Extraction projection pushes through SubqueryAlias by remapping
    /// alias-qualified column refs to input-space.
    #[test]
    fn test_extract_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // SELECT mock_leaf(sub.user, 'name') FROM (SELECT * FROM test) AS sub
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .project(vec![mock_leaf(col("sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          SubqueryAlias: sub
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          SubqueryAlias: sub
            TableScan: test projection=[user]
        "#)?;

        // Extraction projection should be pushed below SubqueryAlias
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          SubqueryAlias: sub
            TableScan: test projection=[user]
        "#)
    }

    /// Extraction projection pushes through SubqueryAlias + Filter.
    #[test]
    fn test_extract_through_subquery_alias_with_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .filter(mock_leaf(col("sub.user"), "status").eq(lit("active")))?
            .project(vec![mock_leaf(col("sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          Filter: mock_leaf(sub.user, Utf8("status")) = Utf8("active")
            SubqueryAlias: sub
              TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          Filter: mock_leaf(sub.user, Utf8("status")) = Utf8("active")
            SubqueryAlias: sub
              TableScan: test projection=[user]
        "#)?;

        // Both extractions should push below SubqueryAlias
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          Filter: mock_leaf(sub.user, Utf8("status")) = Utf8("active")
            SubqueryAlias: sub
              TableScan: test projection=[user]
        "#)
    }

    /// Two layers of SubqueryAlias: extraction pushes through both.
    #[test]
    fn test_extract_through_nested_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("inner_sub")?
            .alias("outer_sub")?
            .project(vec![mock_leaf(col("outer_sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(outer_sub.user, Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(outer_sub.user, Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              TableScan: test projection=[user]
        "#)?;

        // Extraction should push through both SubqueryAlias layers
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(outer_sub.user, Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              TableScan: test projection=[user]
        "#)
    }

    /// Plain columns through SubqueryAlias — no extraction needed.
    #[test]
    fn test_subquery_alias_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .project(vec![col("sub.a"), col("sub.b")])?
            .build()?;

        assert_original_plan!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")?;

        // No extraction should happen for plain columns
        assert_optimized!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")
    }

    /// A variant of MockLeafFunc with the same `name()` but a different concrete type.
    /// Used to verify that deduplication uses `Expr` equality, not `schema_name`.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockLeafFuncVariant {
        signature: Signature,
    }

    impl MockLeafFuncVariant {
        fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Any(2),
                    datafusion_expr::Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MockLeafFuncVariant {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "mock_leaf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!("This is only used for testing optimization")
        }

        fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
            match args.first() {
                Some(ExpressionPlacement::Column)
                | Some(ExpressionPlacement::MoveTowardsLeafNodes) => {
                    ExpressionPlacement::MoveTowardsLeafNodes
                }
                _ => ExpressionPlacement::KeepInPlace,
            }
        }
    }

    /// Two UDFs with the same `name()` but different concrete types should NOT be
    /// deduplicated — they are semantically different expressions that happen to
    /// collide on `schema_name()`. Before the fix (schema_name-based dedup), both
    /// would collapse into one alias; with Expr-equality dedup they get two aliases.
    #[test]
    fn test_different_udfs_same_schema_name_not_deduplicated() -> Result<()> {
        let udf_a = Arc::new(ScalarUDF::new_from_impl(MockLeafFunc::new()));
        let udf_b = Arc::new(ScalarUDF::new_from_impl(MockLeafFuncVariant::new()));

        let expr_a = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf_a,
            vec![col("user"), lit("field")],
        ));
        let expr_b = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf_b,
            vec![col("user"), lit("field")],
        ));

        // Verify preconditions: same schema_name but different Expr
        assert_eq!(
            expr_a.schema_name().to_string(),
            expr_b.schema_name().to_string(),
            "Both expressions should have the same schema_name"
        );
        assert_ne!(
            expr_a, expr_b,
            "Expressions should NOT be equal (different UDF instances)"
        );

        // Use both expressions in a filter so they get extracted.
        // With schema_name dedup, both would collapse into one alias since
        // they have the same schema_name. With Expr-equality, each gets
        // its own extraction alias.
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(expr_a.clone().eq(lit("a")).and(expr_b.clone().eq(lit("b"))))?
            .select(vec![
                table_scan
                    .schema()
                    .index_of_column_by_name(None, "id")
                    .unwrap(),
            ])?
            .build()?;

        // After extraction, both expressions should get separate aliases
        assert_after_extract!(plan, @r#"
        Projection: test.id
          Filter: mock_leaf(test.user, Utf8("field")) = Utf8("a") AND mock_leaf(test.user, Utf8("field")) = Utf8("b")
            TableScan: test projection=[id, user]
        "#)
    }

    // =========================================================================
    // Filter pushdown interaction tests
    // =========================================================================

    /// Extraction pushdown through a filter that already had its own
    /// `mock_leaf` extracted.
    ///
    /// The projection above the filter has a `mock_leaf` extraction and a
    /// plain column.  The filter's predicate was already extracted by
    /// `extract_from_plan` in a previous pass, producing its own
    /// `__extracted` alias for the same expression.  The push-down must
    /// merge both extractions into the same leaf projection.
    ///
    /// Reproduces the scenario:
    ///   Projection: mock_leaf(user, "name") [pushed extraction]
    ///     Filter: mock_leaf(user, "status") = 'active' [already extracted]
    ///       TableScan
    #[test]
    fn test_extraction_pushdown_through_filter_with_extracted_predicate() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Filter uses mock_leaf(user, "status"), projection uses mock_leaf(user, "name")
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .project(vec![col("id"), mock_leaf(col("user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        // Projection extraction for mock_leaf(user, "name") must push
        // through the filter and merge with the existing extraction
        // projection that has __extracted_1.
        assert_optimized!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)
    }

    /// Same expression in filter predicate and projection output.
    ///
    /// Both the filter and the projection reference the exact same
    /// `mock_leaf(user, "status")`.  After filter extraction creates
    /// `__extracted_1`, the projection pushdown must handle the duplicate
    /// correctly—either reusing the alias or creating a second one.
    #[test]
    fn test_extraction_pushdown_same_expr_in_filter_and_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field_expr = mock_leaf(col("user"), "status");
        // Filter and projection use the SAME expression
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(field_expr.clone().gt(lit(5)))?
            .project(vec![col("id"), field_expr])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)?;

        // The projection extraction should merge with the filter's
        // extraction. Since it's the same expression, it may reuse the
        // existing alias or create a second one—but the plan must be valid.
        assert_optimized!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)
    }

    /// Left join with a `mock_leaf` filter on the right side AND
    /// the projection also selects `mock_leaf` from the right side.
    ///
    /// The join filter's mock_leaf is extracted by extract_from_plan,
    /// then the projection's mock_leaf is pushed through the join into
    /// the right-side extraction projection.
    #[test]
    fn test_left_join_with_filter_and_projection_extraction() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Left join: left.id = right.id AND mock_leaf(right.user, "status") > 5
        // Projection: left.id, mock_leaf(left.user, "name"), mock_leaf(right.user, "status")
        let plan = LogicalPlanBuilder::from(left)
            .join_on(
                right,
                JoinType::Left,
                vec![
                    col("test.id").eq(col("right.id")),
                    mock_leaf(col("right.user"), "status").gt(lit(5)),
                ],
            )?
            .project(vec![
                col("test.id"),
                mock_leaf(col("test.user"), "name"),
                mock_leaf(col("right.user"), "status"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(right.user, Utf8("status"))
          Left Join:  Filter: test.id = right.id AND mock_leaf(right.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // After extraction, the join filter's mock_leaf is extracted.
        // The projection still has bare mock_leaf expressions.
        assert_after_extract!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(right.user, Utf8("status"))
          Left Join:  Filter: test.id = right.id AND mock_leaf(right.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // Full pipeline: the join filter extraction and the projection
        // extraction both end up on the right side's extraction projection.
        // (Note: the filter condition stays in the join ON clause—there is
        // no separate Filter node because FilterPushdown is not included
        // in this test pipeline.)
        assert_optimized!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(right.user, Utf8("status"))
          Left Join:  Filter: test.id = right.id AND mock_leaf(right.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)
    }

    /// Extraction projection (all `__extracted` + columns) pushed through
    /// a filter whose predicate references a different extracted expression.
    ///
    /// Simulates the result after extract_from_plan processes a filter,
    /// and then split_and_push_projection encounters a pure extraction
    /// projection above that filter.
    #[test]
    fn test_pure_extraction_proj_push_through_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Build: Filter on mock_leaf(user, "status"), then project
        // mock_leaf(user, "name") which will create an extraction projection.
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").gt(lit(5)))?
            .project(vec![
                col("id"),
                mock_leaf(col("user"), "name"),
                mock_leaf(col("user"), "status"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)?;

        // The projection must push through the filter and merge with
        // the existing extraction projection.
        assert_optimized!(plan, @r#"
        Projection: test.id, mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("status"))
          Filter: mock_leaf(test.user, Utf8("status")) > Int32(5)
            TableScan: test projection=[id, user]
        "#)
    }
}

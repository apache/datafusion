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

//! [`ExtractLeafExpressions`] extracts `PlaceAtLeaves` sub-expressions into projections.
//!
//! This optimizer rule normalizes the plan so that all `PlaceAtLeaves` computations
//! (like field accessors) live in Projection nodes, making them eligible for pushdown
//! by the `OptimizeProjections` rule.

use indexmap::IndexSet;
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, ExpressionPlacement, Projection};
use indexmap::IndexMap;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Extracts `PlaceAtLeaves` sub-expressions from all nodes into projections.
///
/// This normalizes the plan so that all `PlaceAtLeaves` computations (like field
/// accessors) live in Projection nodes, making them eligible for pushdown.
///
/// # Example
///
/// Given a filter with a struct field access:
///
/// ```text
/// Filter: user['status'] = 'active'
///   TableScan: t [user]
/// ```
///
/// This rule extracts the field access into a projection:
///
/// ```text
/// Filter: __leaf_1 = 'active'
///   Projection: user['status'] AS __leaf_1, user
///     TableScan: t [user]
/// ```
///
/// The `OptimizeProjections` rule can then push this projection down to the scan.
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
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let alias_generator = config.alias_generator();
        extract_from_plan(plan, alias_generator)
    }
}

/// Extracts `PlaceAtLeaves` sub-expressions from a plan node.
fn extract_from_plan(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    // Handle specific node types that can benefit from extraction.
    //
    // Schema-preserving nodes (output schema = input schema):
    // - Filter: predicate doesn't affect output columns
    // - Sort: ordering doesn't affect output columns
    // - Limit: fetch/skip don't affect output columns
    //
    // Schema-transforming nodes require special handling:
    // - Aggregate: handled separately to preserve output schema
    // - Projection: skipped (handled by OptimizeProjections)
    match &plan {
        // Schema-preserving nodes
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => {}

        // Aggregate needs special handling
        LogicalPlan::Aggregate(_) => {
            return extract_from_aggregate(plan, alias_generator);
        }

        // Skip everything else
        _ => {
            return Ok(Transformed::no(plan));
        }
    }

    // Skip nodes with no children
    if plan.inputs().is_empty() {
        return Ok(Transformed::no(plan));
    }

    // For nodes with multiple children (e.g., Join), we only extract from the first input
    // for now to keep the logic simple. A more sophisticated implementation could handle
    // multiple inputs.
    let input_schema = Arc::clone(plan.inputs()[0].schema());
    let mut extractor =
        LeafExpressionExtractor::new(input_schema.as_ref(), alias_generator);

    // Transform expressions using map_expressions
    let transformed = plan.map_expressions(|expr| extractor.extract(expr))?;

    if !extractor.has_extractions() {
        return Ok(transformed);
    }

    // For non-Projection nodes (like Filter, Sort, etc.), we need to pass through
    // ALL columns from the input schema, not just those referenced in expressions.
    // This is because these nodes don't change the schema - they pass through all columns.
    for col in input_schema.columns() {
        extractor.columns_needed.insert(col);
    }

    // Build projection with extracted expressions + pass-through columns
    // Clone the first input to wrap in Arc
    let first_input = transformed.data.inputs()[0].clone();
    let inner_projection = extractor.build_projection(Arc::new(first_input))?;

    // Update plan to use new projection as input
    let new_inputs: Vec<LogicalPlan> =
        std::iter::once(LogicalPlan::Projection(inner_projection))
            .chain(
                transformed
                    .data
                    .inputs()
                    .iter()
                    .skip(1)
                    .map(|p| (*p).clone()),
            )
            .collect();

    let new_plan = transformed
        .data
        .with_new_exprs(transformed.data.expressions(), new_inputs)?;

    // Add an outer projection to restore the original schema
    // This ensures the optimized plan has the same output schema
    let original_schema_exprs: Vec<Expr> = input_schema
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();

    let outer_projection =
        Projection::try_new(original_schema_exprs, Arc::new(new_plan))?;

    Ok(Transformed::yes(LogicalPlan::Projection(outer_projection)))
}

/// Extracts `PlaceAtLeaves` sub-expressions from Aggregate nodes.
///
/// For Aggregates, we extract from:
/// - Group-by expressions (full expressions or sub-expressions)
/// - Arguments inside aggregate functions (NOT the aggregate function itself)
fn extract_from_aggregate(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Aggregate(agg) = plan else {
        return Ok(Transformed::no(plan));
    };

    // Capture original output schema for restoration
    let original_schema = Arc::clone(&agg.schema);

    let input_schema = agg.input.schema();
    let mut extractor =
        LeafExpressionExtractor::new(input_schema.as_ref(), alias_generator);

    // Extract from group-by expressions
    let mut new_group_by = Vec::with_capacity(agg.group_expr.len());
    let mut has_extractions = false;

    for expr in &agg.group_expr {
        let transformed = extractor.extract(expr.clone())?;
        if transformed.transformed {
            has_extractions = true;
        }
        new_group_by.push(transformed.data);
    }

    // Extract from aggregate function arguments (not the function itself)
    let mut new_aggr = Vec::with_capacity(agg.aggr_expr.len());

    for expr in &agg.aggr_expr {
        let transformed = extract_from_aggregate_args(expr.clone(), &mut extractor)?;
        if transformed.transformed {
            has_extractions = true;
        }
        new_aggr.push(transformed.data);
    }

    if !has_extractions {
        return Ok(Transformed::no(LogicalPlan::Aggregate(agg)));
    }

    // Track columns needed by the aggregate (for pass-through)
    for expr in new_group_by.iter().chain(new_aggr.iter()) {
        for col in expr.column_refs() {
            extractor.columns_needed.insert(col.clone());
        }
    }

    // Build inner projection with extracted expressions + pass-through columns
    let inner_projection = extractor.build_projection(Arc::clone(&agg.input))?;

    // Create new Aggregate with transformed expressions
    let new_agg = datafusion_expr::logical_plan::Aggregate::try_new(
        Arc::new(LogicalPlan::Projection(inner_projection)),
        new_group_by,
        new_aggr,
    )?;

    // Create outer projection to restore original schema names
    let outer_exprs: Vec<Expr> = original_schema
        .iter()
        .zip(new_agg.schema.columns())
        .map(|((original_qual, original_field), new_col)| {
            // Map from new schema column to original schema name, preserving qualifier
            Expr::Column(new_col)
                .alias_qualified(original_qual.cloned(), original_field.name())
        })
        .collect();

    let outer_projection =
        Projection::try_new(outer_exprs, Arc::new(LogicalPlan::Aggregate(new_agg)))?;

    Ok(Transformed::yes(LogicalPlan::Projection(outer_projection)))
}

/// Extracts `PlaceAtLeaves` sub-expressions from aggregate function arguments.
///
/// This extracts from inside the aggregate (e.g., from `sum(get_field(x, 'y'))`
/// we extract `get_field(x, 'y')`), but NOT the aggregate function itself.
fn extract_from_aggregate_args(
    expr: Expr,
    extractor: &mut LeafExpressionExtractor,
) -> Result<Transformed<Expr>> {
    match expr {
        Expr::AggregateFunction(mut agg_func) => {
            // Extract from arguments, not the function itself
            let mut any_changed = false;
            let mut new_args = Vec::with_capacity(agg_func.params.args.len());

            for arg in agg_func.params.args {
                let transformed = extractor.extract(arg)?;
                if transformed.transformed {
                    any_changed = true;
                }
                new_args.push(transformed.data);
            }

            if any_changed {
                agg_func.params.args = new_args;
                Ok(Transformed::yes(Expr::AggregateFunction(agg_func)))
            } else {
                agg_func.params.args = new_args;
                Ok(Transformed::no(Expr::AggregateFunction(agg_func)))
            }
        }
        // For aliased aggregates, process the inner expression
        Expr::Alias(alias) => {
            let transformed = extract_from_aggregate_args(*alias.expr, extractor)?;
            Ok(
                transformed
                    .update_data(|e| e.alias_qualified(alias.relation, alias.name)),
            )
        }
        // For other expressions, use regular extraction
        other => extractor.extract(other),
    }
}

/// Extracts `PlaceAtLeaves` sub-expressions from larger expressions.
struct LeafExpressionExtractor<'a> {
    /// Extracted expressions: maps schema_name -> (original_expr, alias)
    extracted: IndexMap<String, (Expr, String)>,
    /// Columns needed for pass-through
    columns_needed: IndexSet<Column>,
    /// Input schema
    input_schema: &'a DFSchema,
    /// Alias generator
    alias_generator: &'a Arc<AliasGenerator>,
}

impl<'a> LeafExpressionExtractor<'a> {
    fn new(input_schema: &'a DFSchema, alias_generator: &'a Arc<AliasGenerator>) -> Self {
        Self {
            extracted: IndexMap::new(),
            columns_needed: IndexSet::new(),
            input_schema,
            alias_generator,
        }
    }

    /// Extracts `PlaceAtLeaves` sub-expressions, returning rewritten expression.
    fn extract(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // Walk top-down to find PlaceAtLeaves sub-expressions
        expr.transform_down(|e| {
            match e.placement() {
                ExpressionPlacement::PlaceAtLeaves => {
                    // Extract this entire sub-tree
                    let col_ref = self.add_extracted(e)?;
                    Ok(Transformed::yes(col_ref))
                }
                ExpressionPlacement::Column => {
                    // Track columns for pass-through
                    if let Expr::Column(col) = &e {
                        self.columns_needed.insert(col.clone());
                    }
                    Ok(Transformed::no(e))
                }
                _ => {
                    // Continue recursing into children
                    Ok(Transformed::no(e))
                }
            }
        })
    }

    /// Adds an expression to extracted set, returns column reference.
    fn add_extracted(&mut self, expr: Expr) -> Result<Expr> {
        let schema_name = expr.schema_name().to_string();

        // Deduplication: reuse existing alias if same expression
        if let Some((_, alias)) = self.extracted.get(&schema_name) {
            return Ok(Expr::Column(Column::new_unqualified(alias)));
        }

        // Track columns referenced by this expression
        for col in expr.column_refs() {
            self.columns_needed.insert(col.clone());
        }

        // Generate unique alias
        let alias = self.alias_generator.next("__leaf");
        self.extracted.insert(schema_name, (expr, alias.clone()));

        Ok(Expr::Column(Column::new_unqualified(&alias)))
    }

    fn has_extractions(&self) -> bool {
        !self.extracted.is_empty()
    }

    /// Builds projection with extracted expressions + pass-through columns.
    fn build_projection(&self, input: Arc<LogicalPlan>) -> Result<Projection> {
        let mut proj_exprs = Vec::new();

        // Add extracted expressions with their aliases
        for (_, (expr, alias)) in &self.extracted {
            proj_exprs.push(expr.clone().alias(alias));
        }

        // Add pass-through columns that are in the input schema
        for col in &self.columns_needed {
            // Only add if the column exists in the input schema
            if self.input_schema.has_column(col) {
                proj_exprs.push(Expr::Column(col.clone()));
            }
        }

        Projection::try_new(proj_exprs, input)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::test::*;
    use crate::{OptimizerContext, assert_optimized_plan_eq_snapshot};
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, col, lit, logical_plan::builder::LogicalPlanBuilder,
    };

    /// A mock UDF that simulates a leaf-pushable function like `get_field`.
    /// It returns `PlaceAtLeaves` when its first argument is Column or PlaceAtLeaves.
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
            // Return PlaceAtLeaves if first arg is Column or PlaceAtLeaves
            // (like get_field does)
            match args.first() {
                Some(ExpressionPlacement::Column)
                | Some(ExpressionPlacement::PlaceAtLeaves) => {
                    ExpressionPlacement::PlaceAtLeaves
                }
                _ => ExpressionPlacement::PlaceAtRoot,
            }
        }
    }

    fn mock_leaf(expr: Expr, name: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(MockLeafFunc::new())),
            vec![expr, lit(name)],
        ))
    }

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(ExtractLeafExpressions::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan, @ $expected,)
        }};
    }

    #[test]
    fn test_extract_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .build()?;

        // Note: An outer projection is added to preserve the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __leaf_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __leaf_1, test.user
              TableScan: test
        "#)
    }

    #[test]
    fn test_no_extraction_for_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1)))?
            .build()?;

        // No extraction should happen for simple columns
        assert_optimized_plan_equal!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test
        ")
    }

    #[test]
    fn test_no_extraction_for_projection() -> Result<()> {
        // Projections are skipped - they're handled by OptimizeProjections
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                mock_leaf(col("user"), "name")
                    .is_not_null()
                    .alias("has_name"),
            ])?
            .build()?;

        // No extraction from Projections - they're schema-transforming
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test
        "#)
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

        // Same expression should be extracted only once
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __leaf_1 IS NOT NULL AND __leaf_1 IS NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __leaf_1, test.user
              TableScan: test
        "#)
    }

    #[test]
    fn test_already_leaf_expression_in_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // A bare mock_leaf expression is already PlaceAtLeaves
        // When compared to a literal, the comparison is PlaceAtRoot so extraction happens
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "name").eq(lit("test")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __leaf_1 = Utf8("test")
            Projection: mock_leaf(test.user, Utf8("name")) AS __leaf_1, test.user
              TableScan: test
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_group_by() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![mock_leaf(col("user"), "status")], vec![count(lit(1))])?
            .build()?;

        // Group-by expression is PlaceAtLeaves, so it gets extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __leaf_1 AS mock_leaf(test.user,Utf8("status")), COUNT(Int32(1)) AS COUNT(Int32(1))
          Aggregate: groupBy=[[__leaf_1]], aggr=[[COUNT(Int32(1))]]
            Projection: mock_leaf(test.user, Utf8("status")) AS __leaf_1, test.user
              TableScan: test
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

        // Aggregate argument is PlaceAtLeaves, so it gets extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user AS user, COUNT(__leaf_1) AS COUNT(mock_leaf(test.user,Utf8("value")))
          Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__leaf_1)]]
            Projection: mock_leaf(test.user, Utf8("value")) AS __leaf_1, test.user
              TableScan: test
        "#)
    }
}

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

//! Eliminate common sub-expression.

use crate::{utils, OptimizerConfig, OptimizerRule};
use arrow::datatypes::DataType;
use datafusion_common::{DFField, DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{
    col,
    expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion},
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
    logical_plan::{Aggregate, Filter, LogicalPlan, Projection, Sort, Window},
    Expr, ExprSchemable,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// A map from expression's identifier to tuple including
/// - the expression itself (cloned)
/// - counter
/// - DataType of this expression.
type ExprSet = HashMap<Identifier, (Expr, usize, DataType)>;

/// Identifier type. Current implementation use describe of a expression (type String) as
/// Identifier.
///
/// A Identifier should (ideally) be able to "hash", "accumulate", "equal" and "have no
/// collision (as low as possible)"
///
/// Since a identifier is likely to be copied many times, it is better that a identifier
/// is small or "copy". otherwise some kinds of reference count is needed. String description
/// here is not such a good choose.
type Identifier = String;

/// Perform Common Sub-expression Elimination optimization.
///
/// Currently only common sub-expressions within one logical plan will
/// be eliminated.
pub struct CommonSubexprEliminate {}

impl CommonSubexprEliminate {
    fn rewrite_expr(
        &self,
        exprs_list: &[&[Expr]],
        arrays_list: &[&[Vec<(usize, String)>]],
        input: &LogicalPlan,
        expr_set: &mut ExprSet,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<(Vec<Vec<Expr>>, LogicalPlan)> {
        let mut affected_id = BTreeSet::<Identifier>::new();

        let rewrite_exprs = exprs_list
            .iter()
            .zip(arrays_list.iter())
            .map(|(exprs, arrays)| {
                exprs
                    .iter()
                    .cloned()
                    .zip(arrays.iter())
                    .map(|(expr, id_array)| {
                        replace_common_expr(expr, id_array, expr_set, &mut affected_id)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        let mut new_input = self.optimize(input, optimizer_config)?;
        if !affected_id.is_empty() {
            new_input = build_project_plan(new_input, affected_id, expr_set)?;
        }

        Ok((rewrite_exprs, new_input))
    }
}

impl OptimizerRule for CommonSubexprEliminate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut expr_set = ExprSet::new();

        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => {
                let input_schema = Arc::clone(input.schema());
                let arrays = to_arrays(expr, input_schema, &mut expr_set)?;

                let (mut new_expr, new_input) = self.rewrite_expr(
                    &[expr],
                    &[&arrays],
                    input,
                    &mut expr_set,
                    optimizer_config,
                )?;

                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    pop_expr(&mut new_expr)?,
                    Arc::new(new_input),
                    schema.clone(),
                )?))
            }
            LogicalPlan::Filter(filter) => {
                let input = filter.input();
                let predicate = filter.predicate();
                let input_schema = Arc::clone(input.schema());
                let mut id_array = vec![];
                expr_to_identifier(
                    predicate,
                    &mut expr_set,
                    &mut id_array,
                    input_schema,
                )?;

                let (mut new_expr, new_input) = self.rewrite_expr(
                    &[&[predicate.clone()]],
                    &[&[id_array]],
                    filter.input(),
                    &mut expr_set,
                    optimizer_config,
                )?;

                if let Some(predicate) = pop_expr(&mut new_expr)?.pop() {
                    Ok(LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(new_input),
                    )?))
                } else {
                    Err(DataFusionError::Internal(
                        "Failed to pop predicate expr".to_string(),
                    ))
                }
            }
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => {
                let input_schema = Arc::clone(input.schema());
                let arrays = to_arrays(window_expr, input_schema, &mut expr_set)?;

                let (mut new_expr, new_input) = self.rewrite_expr(
                    &[window_expr],
                    &[&arrays],
                    input,
                    &mut expr_set,
                    optimizer_config,
                )?;

                Ok(LogicalPlan::Window(Window {
                    input: Arc::new(new_input),
                    window_expr: pop_expr(&mut new_expr)?,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                input,
                schema,
            }) => {
                let input_schema = Arc::clone(input.schema());
                let group_arrays =
                    to_arrays(group_expr, Arc::clone(&input_schema), &mut expr_set)?;
                let aggr_arrays = to_arrays(aggr_expr, input_schema, &mut expr_set)?;

                let (mut new_expr, new_input) = self.rewrite_expr(
                    &[group_expr, aggr_expr],
                    &[&group_arrays, &aggr_arrays],
                    input,
                    &mut expr_set,
                    optimizer_config,
                )?;
                // note the reversed pop order.
                let new_aggr_expr = pop_expr(&mut new_expr)?;
                let new_group_expr = pop_expr(&mut new_expr)?;

                Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    Arc::new(new_input),
                    new_group_expr,
                    new_aggr_expr,
                    schema.clone(),
                )?))
            }
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                let input_schema = Arc::clone(input.schema());
                let arrays = to_arrays(expr, input_schema, &mut expr_set)?;

                let (mut new_expr, new_input) = self.rewrite_expr(
                    &[expr],
                    &[&arrays],
                    input,
                    &mut expr_set,
                    optimizer_config,
                )?;

                Ok(LogicalPlan::Sort(Sort {
                    expr: pop_expr(&mut new_expr)?,
                    input: Arc::new(new_input),
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropView(_)
            | LogicalPlan::SetVariable(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Extension(_) => {
                // apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl Default for CommonSubexprEliminate {
    fn default() -> Self {
        Self::new()
    }
}

impl CommonSubexprEliminate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn pop_expr(new_expr: &mut Vec<Vec<Expr>>) -> Result<Vec<Expr>> {
    new_expr
        .pop()
        .ok_or_else(|| DataFusionError::Internal("Failed to pop expression".to_string()))
}

fn to_arrays(
    expr: &[Expr],
    input_schema: DFSchemaRef,
    expr_set: &mut ExprSet,
) -> Result<Vec<Vec<(usize, String)>>> {
    expr.iter()
        .map(|e| {
            let mut id_array = vec![];
            expr_to_identifier(e, expr_set, &mut id_array, Arc::clone(&input_schema))?;

            Ok(id_array)
        })
        .collect::<Result<Vec<_>>>()
}

/// Build the "intermediate" projection plan that evaluates the extracted common expressions.
fn build_project_plan(
    input: LogicalPlan,
    affected_id: BTreeSet<Identifier>,
    expr_set: &ExprSet,
) -> Result<LogicalPlan> {
    let mut project_exprs = vec![];
    let mut fields = vec![];
    let mut fields_set = BTreeSet::new();

    for id in affected_id {
        match expr_set.get(&id) {
            Some((expr, _, data_type)) => {
                // todo: check `nullable`
                let field = DFField::new(None, &id, data_type.clone(), true);
                fields_set.insert(field.name().to_owned());
                fields.push(field);
                project_exprs.push(expr.clone().alias(&id));
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "expr_set invalid state".to_string(),
                ));
            }
        }
    }

    for field in input.schema().fields() {
        if fields_set.insert(field.qualified_name()) {
            fields.push(field.clone());
            project_exprs.push(Expr::Column(field.qualified_column()));
        }
    }

    let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        project_exprs,
        Arc::new(input),
        Arc::new(schema),
    )?))
}

/// Go through an expression tree and generate identifier.
///
/// An identifier contains information of the expression itself and its sub-expression.
/// This visitor implementation use a stack `visit_stack` to track traversal, which
/// lets us know when a sub-tree's visiting is finished. When `pre_visit` is called
/// (traversing to a new node), an `EnterMark` and an `ExprItem` will be pushed into stack.
/// And try to pop out a `EnterMark` on leaving a node (`post_visit()`). All `ExprItem`
/// before the first `EnterMark` is considered to be sub-tree of the leaving node.
///
/// This visitor also records identifier in `id_array`. Makes the following traverse
/// pass can get the identifier of a node without recalculate it. We assign each node
/// in the expr tree a series number, start from 1, maintained by `series_number`.
/// Series number represents the order we left (`post_visit`) a node. Has the property
/// that child node's series number always smaller than parent's. While `id_array` is
/// organized in the order we enter (`pre_visit`) a node. `node_count` helps us to
/// get the index of `id_array` for each node.
///
/// `Expr` without sub-expr (column, literal etc.) will not have identifier
/// because they should not be recognized as common sub-expr.
struct ExprIdentifierVisitor<'a> {
    // param
    expr_set: &'a mut ExprSet,
    /// series number (usize) and identifier.
    id_array: &'a mut Vec<(usize, Identifier)>,
    /// input schema for the node that we're optimizing, so we can determine the correct datatype
    /// for each subexpression
    input_schema: DFSchemaRef,
    // inner states
    visit_stack: Vec<VisitRecord>,
    /// increased in pre_visit, start from 0.
    node_count: usize,
    /// increased in post_visit, start from 1.
    series_number: usize,
}

/// Record item that used when traversing a expression tree.
enum VisitRecord {
    /// `usize` is the monotone increasing series number assigned in pre_visit().
    /// Starts from 0. Is used to index the identifier array `id_array` in post_visit().
    EnterMark(usize),
    /// Accumulated identifier of sub expression.
    ExprItem(Identifier),
}

impl ExprIdentifierVisitor<'_> {
    fn desc_expr(expr: &Expr) -> String {
        format!("{}", expr)
    }

    /// Find the first `EnterMark` in the stack, and accumulates every `ExprItem`
    /// before it.
    fn pop_enter_mark(&mut self) -> (usize, Identifier) {
        let mut desc = String::new();

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(idx) => {
                    return (idx, desc);
                }
                VisitRecord::ExprItem(s) => {
                    desc.push_str(&s);
                }
            }
        }

        unreachable!("Enter mark should paired with node number");
    }
}

impl ExpressionVisitor for ExprIdentifierVisitor<'_> {
    fn pre_visit(mut self, _expr: &Expr) -> Result<Recursion<Self>> {
        self.visit_stack
            .push(VisitRecord::EnterMark(self.node_count));
        self.node_count += 1;
        // put placeholder
        self.id_array.push((0, "".to_string()));
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        self.series_number += 1;

        let (idx, sub_expr_desc) = self.pop_enter_mark();
        // skip exprs should not be recognize.
        if matches!(
            expr,
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Alias(..)
                | Expr::Sort { .. }
                | Expr::Wildcard
        ) {
            self.id_array[idx].0 = self.series_number;
            let desc = Self::desc_expr(expr);
            self.visit_stack.push(VisitRecord::ExprItem(desc));
            return Ok(self);
        }
        let mut desc = Self::desc_expr(expr);
        desc.push_str(&sub_expr_desc);

        self.id_array[idx] = (self.series_number, desc.clone());
        self.visit_stack.push(VisitRecord::ExprItem(desc.clone()));

        let data_type = expr.get_type(&self.input_schema)?;

        self.expr_set
            .entry(desc)
            .or_insert_with(|| (expr.clone(), 0, data_type))
            .1 += 1;
        Ok(self)
    }
}

/// Go through an expression tree and generate identifier for every node in this tree.
fn expr_to_identifier(
    expr: &Expr,
    expr_set: &mut ExprSet,
    id_array: &mut Vec<(usize, Identifier)>,
    input_schema: DFSchemaRef,
) -> Result<()> {
    expr.accept(ExprIdentifierVisitor {
        expr_set,
        id_array,
        input_schema,
        visit_stack: vec![],
        node_count: 0,
        series_number: 0,
    })?;

    Ok(())
}

/// Rewrite expression by replacing detected common sub-expression with
/// the corresponding temporary column name. That column contains the
/// evaluate result of replaced expression.
struct CommonSubexprRewriter<'a> {
    expr_set: &'a mut ExprSet,
    id_array: &'a [(usize, Identifier)],
    /// Which identifier is replaced.
    affected_id: &'a mut BTreeSet<Identifier>,

    /// the max series number we have rewritten. Other expression nodes
    /// with smaller series number is already replaced and shouldn't
    /// do anything with them.
    max_series_number: usize,
    /// current node's information's index in `id_array`.
    curr_index: usize,
}

impl ExprRewriter for CommonSubexprRewriter<'_> {
    fn pre_visit(&mut self, _: &Expr) -> Result<RewriteRecursion> {
        if self.curr_index >= self.id_array.len()
            || self.max_series_number > self.id_array[self.curr_index].0
        {
            return Ok(RewriteRecursion::Stop);
        }

        let curr_id = &self.id_array[self.curr_index].1;
        // skip `Expr`s without identifier (empty identifier).
        if curr_id.is_empty() {
            self.curr_index += 1;
            return Ok(RewriteRecursion::Skip);
        }
        match self.expr_set.get(curr_id) {
            Some((_, counter, _)) => {
                if *counter > 1 {
                    self.affected_id.insert(curr_id.clone());
                    Ok(RewriteRecursion::Mutate)
                } else {
                    self.curr_index += 1;
                    Ok(RewriteRecursion::Skip)
                }
            }
            _ => Err(DataFusionError::Internal(
                "expr_set invalid state".to_string(),
            )),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        // This expr tree is finished.
        if self.curr_index >= self.id_array.len() {
            return Ok(expr);
        }

        let (series_number, id) = &self.id_array[self.curr_index];
        self.curr_index += 1;
        // Skip sub-node of a replaced tree, or without identifier, or is not repeated expr.
        let expr_set_item = self.expr_set.get(id).ok_or_else(|| {
            DataFusionError::Internal("expr_set invalid state".to_string())
        })?;
        if *series_number < self.max_series_number
            || id.is_empty()
            || expr_set_item.1 <= 1
        {
            return Ok(expr);
        }

        self.max_series_number = *series_number;
        // step index to skip all sub-node (which has smaller series number).
        while self.curr_index < self.id_array.len()
            && *series_number > self.id_array[self.curr_index].0
        {
            self.curr_index += 1;
        }

        let expr_name = expr.display_name()?;
        // Alias this `Column` expr to it original "expr name",
        // `projection_push_down` optimizer use "expr name" to eliminate useless
        // projections.
        Ok(col(id).alias(expr_name))
    }
}

fn replace_common_expr(
    expr: Expr,
    id_array: &[(usize, Identifier)],
    expr_set: &mut ExprSet,
    affected_id: &mut BTreeSet<Identifier>,
) -> Result<Expr> {
    expr.rewrite(&mut CommonSubexprRewriter {
        expr_set,
        id_array,
        affected_id,
        max_series_number: 0,
        curr_index: 0,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_expr::logical_plan::{table_scan, JoinType};
    use datafusion_expr::{
        avg, binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, sum,
        Operator,
    };
    use std::iter;

    fn assert_optimized_plan_eq(expected: &str, plan: &LogicalPlan) {
        let optimizer = CommonSubexprEliminate {};
        let optimized_plan = optimizer
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(expected, formatted_plan);
    }

    #[test]
    fn id_array_visitor() -> Result<()> {
        let expr = binary_expr(
            binary_expr(
                sum(binary_expr(col("a"), Operator::Plus, lit(1))),
                Operator::Minus,
                avg(col("c")),
            ),
            Operator::Multiply,
            lit(2),
        );

        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![
                DFField::new(None, "a", DataType::Int64, false),
                DFField::new(None, "c", DataType::Int64, false),
            ],
            Default::default(),
        )?);

        let mut id_array = vec![];
        expr_to_identifier(
            &expr,
            &mut HashMap::new(),
            &mut id_array,
            Arc::clone(&schema),
        )?;

        let expected = vec![
            (9, "(SUM(a + Int32(1)) - AVG(c)) * Int32(2)Int32(2)SUM(a + Int32(1)) - AVG(c)AVG(c)cSUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (7, "SUM(a + Int32(1)) - AVG(c)AVG(c)cSUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (4, "SUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (3, "a + Int32(1)Int32(1)a"),
            (1, ""),
            (2, ""),
            (6, "AVG(c)c"),
            (5, ""),
            (8, "")
        ]
        .into_iter()
        .map(|(number, id)| (number, id.into()))
        .collect::<Vec<_>>();
        assert_eq!(expected, id_array);

        Ok(())
    }

    #[test]
    fn tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T;
        //
        // The manual assembled logical plan don't contains the outermost `Projection`.

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(binary_expr(
                        col("a"),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Minus, col("b")),
                    )),
                    sum(binary_expr(
                        binary_expr(
                            col("a"),
                            Operator::Multiply,
                            binary_expr(lit(1), Operator::Minus, col("b")),
                        ),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Plus, col("c")),
                    )),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[SUM(test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a AS test.a * Int32(1) - test.b), SUM(test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a AS test.a * Int32(1) - test.b * (Int32(1) + test.c))]]\
        \n  Projection: test.a * (Int32(1) - test.b) AS test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    binary_expr(lit(1), Operator::Plus, avg(col("a"))),
                    binary_expr(lit(1), Operator::Minus, avg(col("a"))),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[Int32(1) + AVG(test.a)test.a AS AVG(test.a), Int32(1) - AVG(test.a)test.a AS AVG(test.a)]]\
        \n  Projection: AVG(test.a) AS AVG(test.a)test.a, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn subexpr_in_same_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                binary_expr(lit(1), Operator::Plus, col("a")).alias("first"),
                binary_expr(lit(1), Operator::Plus, col("a")).alias("second"),
            ])?
            .build()?;

        let expected = "Projection: Int32(1) + test.atest.aInt32(1) AS Int32(1) + test.a AS first, Int32(1) + test.atest.aInt32(1) AS Int32(1) + test.a AS second\
        \n  Projection: Int32(1) + test.a AS Int32(1) + test.atest.aInt32(1), test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn subexpr_in_different_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                binary_expr(lit(1), Operator::Plus, col("a")),
                binary_expr(col("a"), Operator::Plus, lit(1)),
            ])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a, test.a + Int32(1)\
        \n  TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn cross_plans_subexpr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  Projection: Int32(1) + test.a\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn redundant_project_fields() {
        let table_scan = test_table_scan().unwrap();
        let affected_id: BTreeSet<Identifier> =
            ["c+a".to_string(), "d+a".to_string()].into_iter().collect();
        let expr_set = [
            ("c+a".to_string(), (col("c+a"), 1, DataType::UInt32)),
            ("d+a".to_string(), (col("d+a"), 1, DataType::UInt32)),
        ]
        .into_iter()
        .collect();
        let project =
            build_project_plan(table_scan, affected_id.clone(), &expr_set).unwrap();
        let project_2 = build_project_plan(project, affected_id, &expr_set).unwrap();

        let mut field_set = BTreeSet::new();
        for field in project_2.schema().fields() {
            assert!(field_set.insert(field.qualified_name()));
        }
    }

    #[test]
    fn redundant_project_fields_join_input() {
        let table_scan_1 = test_table_scan_with_name("test1").unwrap();
        let table_scan_2 = test_table_scan_with_name("test2").unwrap();
        let join = LogicalPlanBuilder::from(table_scan_1)
            .join(&table_scan_2, JoinType::Inner, (vec!["a"], vec!["a"]), None)
            .unwrap()
            .build()
            .unwrap();
        let affected_id: BTreeSet<Identifier> =
            ["c+a".to_string(), "d+a".to_string()].into_iter().collect();
        let expr_set = [
            ("c+a".to_string(), (col("c+a"), 1, DataType::UInt32)),
            ("d+a".to_string(), (col("d+a"), 1, DataType::UInt32)),
        ]
        .into_iter()
        .collect();
        let project = build_project_plan(join, affected_id.clone(), &expr_set).unwrap();
        let project_2 = build_project_plan(project, affected_id, &expr_set).unwrap();

        let mut field_set = BTreeSet::new();
        for field in project_2.schema().fields() {
            assert!(field_set.insert(field.qualified_name()));
        }
    }

    #[test]
    fn eliminated_subexpr_datatype() {
        use datafusion_expr::cast;

        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]);

        let plan = table_scan(Some("table"), &schema, None)
            .unwrap()
            .filter(
                cast(col("a"), DataType::Int64)
                    .lt(lit(1_i64))
                    .and(cast(col("a"), DataType::Int64).not_eq(lit(1_i64))),
            )
            .unwrap()
            .build()
            .unwrap();
        let rule = CommonSubexprEliminate {};
        let optimized_plan = rule.optimize(&plan, &mut OptimizerConfig::new()).unwrap();

        let schema = optimized_plan.schema();
        let fields_with_datatypes: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type()))
            .collect();
        let formatted_fields_with_datatype = format!("{fields_with_datatypes:#?}");
        let expected = r###"[
    (
        "CAST(table.a AS Int64)table.a",
        Int64,
    ),
    (
        "a",
        UInt64,
    ),
    (
        "b",
        UInt64,
    ),
    (
        "c",
        UInt64,
    ),
]"###;
        assert_eq!(expected, formatted_fields_with_datatype);
    }
}

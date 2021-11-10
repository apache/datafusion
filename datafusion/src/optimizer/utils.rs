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

//! Collection of utility functions that are leveraged by the query optimizer rules

use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::optimizer::OptimizerRule;
use crate::execution::context::{ExecutionContextState, ExecutionProps};
use crate::logical_plan::{
    build_join_schema, Column, DFSchema, DFSchemaRef, Expr, ExprRewriter, LogicalPlan,
    LogicalPlanBuilder, Operator, Partitioning, Recursion, RewriteRecursion,
};
use crate::physical_plan::functions::Volatility;
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::prelude::lit;
use crate::scalar::ScalarValue;
use crate::{
    error::{DataFusionError, Result},
    logical_plan::ExpressionVisitor,
};
use std::{collections::HashSet, sync::Arc};

const CASE_EXPR_MARKER: &str = "__DATAFUSION_CASE_EXPR__";
const CASE_ELSE_MARKER: &str = "__DATAFUSION_CASE_ELSE__";
const WINDOW_PARTITION_MARKER: &str = "__DATAFUSION_WINDOW_PARTITION__";
const WINDOW_SORT_MARKER: &str = "__DATAFUSION_WINDOW_SORT__";

/// Recursively walk a list of expression trees, collecting the unique set of columns
/// referenced in the expression
pub fn exprlist_to_columns(expr: &[Expr], accum: &mut HashSet<Column>) -> Result<()> {
    for e in expr {
        expr_to_columns(e, accum)?;
    }
    Ok(())
}

/// Recursively walk an expression tree, collecting the unique set of column names
/// referenced in the expression
struct ColumnNameVisitor<'a> {
    accum: &'a mut HashSet<Column>,
}

impl ExpressionVisitor for ColumnNameVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(qc) => {
                self.accum.insert(qc.clone());
            }
            Expr::ScalarVariable(var_names) => {
                self.accum.insert(Column::from_name(var_names.join(".")));
            }
            Expr::Alias(_, _) => {}
            Expr::Literal(_) => {}
            Expr::BinaryExpr { .. } => {}
            Expr::Not(_) => {}
            Expr::IsNotNull(_) => {}
            Expr::IsNull(_) => {}
            Expr::Negative(_) => {}
            Expr::Between { .. } => {}
            Expr::Case { .. } => {}
            Expr::Cast { .. } => {}
            Expr::TryCast { .. } => {}
            Expr::Sort { .. } => {}
            Expr::ScalarFunction { .. } => {}
            Expr::ScalarUDF { .. } => {}
            Expr::WindowFunction { .. } => {}
            Expr::AggregateFunction { .. } => {}
            Expr::AggregateUDF { .. } => {}
            Expr::InList { .. } => {}
            Expr::Wildcard => {}
            Expr::GetIndexedField { .. } => {}
        }
        Ok(Recursion::Continue(self))
    }
}

/// Recursively walk an expression tree, collecting the unique set of columns
/// referenced in the expression
pub fn expr_to_columns(expr: &Expr, accum: &mut HashSet<Column>) -> Result<()> {
    expr.accept(ColumnNameVisitor { accum })?;
    Ok(())
}

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    let new_exprs = plan.expressions();
    let new_inputs = plan
        .inputs()
        .into_iter()
        .map(|plan| optimizer.optimize(plan, execution_props))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_exprs, &new_inputs)
}

/// Returns a new logical plan based on the original one with inputs
/// and expressions replaced.
///
/// The exprs correspond to the same order of expressions returned by
/// `LogicalPlan::expressions`. This function is used in optimizers in
/// the following way:
///
/// ```text
/// let new_inputs = optimize_children(..., plan, props);
///
/// // get the plans expressions to optimize
/// let exprs = plan.expressions();
///
/// // potentially rewrite plan expressions
/// let rewritten_exprs = rewrite_exprs(exprs);
///
/// // create new plan using rewritten_exprs in same position
/// let new_plan = from_plan(&plan, rewritten_exprs, new_inputs);
/// ```
pub fn from_plan(
    plan: &LogicalPlan,
    expr: &[Expr],
    inputs: &[LogicalPlan],
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection { schema, alias, .. } => Ok(LogicalPlan::Projection {
            expr: expr.to_vec(),
            input: Arc::new(inputs[0].clone()),
            schema: schema.clone(),
            alias: alias.clone(),
        }),
        LogicalPlan::Values { schema, .. } => Ok(LogicalPlan::Values {
            schema: schema.clone(),
            values: expr
                .chunks_exact(schema.fields().len())
                .map(|s| s.to_vec())
                .collect::<Vec<_>>(),
        }),
        LogicalPlan::Filter { .. } => Ok(LogicalPlan::Filter {
            predicate: expr[0].clone(),
            input: Arc::new(inputs[0].clone()),
        }),
        LogicalPlan::Repartition {
            partitioning_scheme,
            ..
        } => match partitioning_scheme {
            Partitioning::RoundRobinBatch(n) => Ok(LogicalPlan::Repartition {
                partitioning_scheme: Partitioning::RoundRobinBatch(*n),
                input: Arc::new(inputs[0].clone()),
            }),
            Partitioning::Hash(_, n) => Ok(LogicalPlan::Repartition {
                partitioning_scheme: Partitioning::Hash(expr.to_owned(), *n),
                input: Arc::new(inputs[0].clone()),
            }),
        },
        LogicalPlan::Window {
            window_expr,
            schema,
            ..
        } => Ok(LogicalPlan::Window {
            input: Arc::new(inputs[0].clone()),
            window_expr: expr[0..window_expr.len()].to_vec(),
            schema: schema.clone(),
        }),
        LogicalPlan::Aggregate {
            group_expr, schema, ..
        } => Ok(LogicalPlan::Aggregate {
            group_expr: expr[0..group_expr.len()].to_vec(),
            aggr_expr: expr[group_expr.len()..].to_vec(),
            input: Arc::new(inputs[0].clone()),
            schema: schema.clone(),
        }),
        LogicalPlan::Sort { .. } => Ok(LogicalPlan::Sort {
            expr: expr.to_vec(),
            input: Arc::new(inputs[0].clone()),
        }),
        LogicalPlan::Join {
            join_type,
            join_constraint,
            on,
            null_equals_null,
            ..
        } => {
            let schema =
                build_join_schema(inputs[0].schema(), inputs[1].schema(), join_type)?;
            Ok(LogicalPlan::Join {
                left: Arc::new(inputs[0].clone()),
                right: Arc::new(inputs[1].clone()),
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: on.clone(),
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            })
        }
        LogicalPlan::CrossJoin { .. } => {
            let left = inputs[0].clone();
            let right = &inputs[1];
            LogicalPlanBuilder::from(left).cross_join(right)?.build()
        }
        LogicalPlan::Limit { n, .. } => Ok(LogicalPlan::Limit {
            n: *n,
            input: Arc::new(inputs[0].clone()),
        }),
        LogicalPlan::CreateMemoryTable { name, .. } => {
            Ok(LogicalPlan::CreateMemoryTable {
                input: Arc::new(inputs[0].clone()),
                name: name.clone(),
            })
        }
        LogicalPlan::Extension { node } => Ok(LogicalPlan::Extension {
            node: node.from_template(expr, inputs),
        }),
        LogicalPlan::Union { schema, alias, .. } => Ok(LogicalPlan::Union {
            inputs: inputs.to_vec(),
            schema: schema.clone(),
            alias: alias.clone(),
        }),
        LogicalPlan::Analyze {
            verbose, schema, ..
        } => {
            assert!(expr.is_empty());
            assert_eq!(inputs.len(), 1);
            Ok(LogicalPlan::Analyze {
                verbose: *verbose,
                schema: schema.clone(),
                input: Arc::new(inputs[0].clone()),
            })
        }
        LogicalPlan::Explain { .. } => {
            // Explain should be handled specially in the optimizers;
            // If this assert fails it means some optimizer pass is
            // trying to optimize Explain directly
            assert!(
                expr.is_empty(),
                "Explain can not be created from utils::from_expr"
            );
            assert!(
                inputs.is_empty(),
                "Explain can not be created from utils::from_expr"
            );
            Ok(plan.clone())
        }
        LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::TableScan { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::DropTable { .. } => {
            // All of these plan types have no inputs / exprs so should not be called
            assert!(expr.is_empty(), "{:?} should have no exprs", plan);
            assert!(inputs.is_empty(), "{:?}  should have no inputs", plan);
            Ok(plan.clone())
        }
    }
}

/// Returns all direct children `Expression`s of `expr`.
/// E.g. if the expression is "(a + 1) + 1", it returns ["a + 1", "1"] (as Expr objects)
pub fn expr_sub_expressions(expr: &Expr) -> Result<Vec<Expr>> {
    match expr {
        Expr::BinaryExpr { left, right, .. } => {
            Ok(vec![left.as_ref().to_owned(), right.as_ref().to_owned()])
        }
        Expr::IsNull(e) => Ok(vec![e.as_ref().to_owned()]),
        Expr::IsNotNull(e) => Ok(vec![e.as_ref().to_owned()]),
        Expr::ScalarFunction { args, .. } => Ok(args.clone()),
        Expr::ScalarUDF { args, .. } => Ok(args.clone()),
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            let mut expr_list: Vec<Expr> = vec![];
            expr_list.extend(args.clone());
            expr_list.push(lit(WINDOW_PARTITION_MARKER));
            expr_list.extend(partition_by.clone());
            expr_list.push(lit(WINDOW_SORT_MARKER));
            expr_list.extend(order_by.clone());
            Ok(expr_list)
        }
        Expr::AggregateFunction { args, .. } => Ok(args.clone()),
        Expr::AggregateUDF { args, .. } => Ok(args.clone()),
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
            ..
        } => {
            let mut expr_list: Vec<Expr> = vec![];
            if let Some(e) = expr {
                expr_list.push(lit(CASE_EXPR_MARKER));
                expr_list.push(e.as_ref().to_owned());
            }
            for (w, t) in when_then_expr {
                expr_list.push(w.as_ref().to_owned());
                expr_list.push(t.as_ref().to_owned());
            }
            if let Some(e) = else_expr {
                expr_list.push(lit(CASE_ELSE_MARKER));
                expr_list.push(e.as_ref().to_owned());
            }
            Ok(expr_list)
        }
        Expr::Cast { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
        Expr::TryCast { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
        Expr::Column(_) => Ok(vec![]),
        Expr::Alias(expr, ..) => Ok(vec![expr.as_ref().to_owned()]),
        Expr::Literal(_) => Ok(vec![]),
        Expr::ScalarVariable(_) => Ok(vec![]),
        Expr::Not(expr) => Ok(vec![expr.as_ref().to_owned()]),
        Expr::Negative(expr) => Ok(vec![expr.as_ref().to_owned()]),
        Expr::Sort { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
        Expr::Between {
            expr, low, high, ..
        } => Ok(vec![
            expr.as_ref().to_owned(),
            low.as_ref().to_owned(),
            high.as_ref().to_owned(),
        ]),
        Expr::InList { expr, list, .. } => {
            let mut expr_list: Vec<Expr> = vec![expr.as_ref().to_owned()];
            for list_expr in list {
                expr_list.push(list_expr.to_owned());
            }
            Ok(expr_list)
        }
        Expr::Wildcard { .. } => Err(DataFusionError::Internal(
            "Wildcard expressions are not valid in a logical query plan".to_owned(),
        )),
        Expr::GetIndexedField { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
    }
}

/// returns a new expression where the expressions in `expr` are replaced by the ones in
/// `expressions`.
/// This is used in conjunction with ``expr_expressions`` to re-write expressions.
pub fn rewrite_expression(expr: &Expr, expressions: &[Expr]) -> Result<Expr> {
    match expr {
        Expr::BinaryExpr { op, .. } => Ok(Expr::BinaryExpr {
            left: Box::new(expressions[0].clone()),
            op: *op,
            right: Box::new(expressions[1].clone()),
        }),
        Expr::IsNull(_) => Ok(Expr::IsNull(Box::new(expressions[0].clone()))),
        Expr::IsNotNull(_) => Ok(Expr::IsNotNull(Box::new(expressions[0].clone()))),
        Expr::ScalarFunction { fun, .. } => Ok(Expr::ScalarFunction {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::ScalarUDF { fun, .. } => Ok(Expr::ScalarUDF {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::WindowFunction {
            fun, window_frame, ..
        } => {
            let partition_index = expressions
                .iter()
                .position(|expr| {
                    matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(str)))
            if str == WINDOW_PARTITION_MARKER)
                })
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Ill-formed window function expressions: unexpected marker"
                            .to_owned(),
                    )
                })?;

            let sort_index = expressions
                .iter()
                .position(|expr| {
                    matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(str)))
            if str == WINDOW_SORT_MARKER)
                })
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Ill-formed window function expressions".to_owned(),
                    )
                })?;

            if partition_index >= sort_index {
                Err(DataFusionError::Internal(
                    "Ill-formed window function expressions: partition index too large"
                        .to_owned(),
                ))
            } else {
                Ok(Expr::WindowFunction {
                    fun: fun.clone(),
                    args: expressions[..partition_index].to_vec(),
                    partition_by: expressions[partition_index + 1..sort_index].to_vec(),
                    order_by: expressions[sort_index + 1..].to_vec(),
                    window_frame: *window_frame,
                })
            }
        }
        Expr::AggregateFunction { fun, distinct, .. } => Ok(Expr::AggregateFunction {
            fun: fun.clone(),
            args: expressions.to_vec(),
            distinct: *distinct,
        }),
        Expr::AggregateUDF { fun, .. } => Ok(Expr::AggregateUDF {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::Case { .. } => {
            let mut base_expr: Option<Box<Expr>> = None;
            let mut when_then: Vec<(Box<Expr>, Box<Expr>)> = vec![];
            let mut else_expr: Option<Box<Expr>> = None;
            let mut i = 0;

            while i < expressions.len() {
                match &expressions[i] {
                    Expr::Literal(ScalarValue::Utf8(Some(str)))
                        if str == CASE_EXPR_MARKER =>
                    {
                        base_expr = Some(Box::new(expressions[i + 1].clone()));
                        i += 2;
                    }
                    Expr::Literal(ScalarValue::Utf8(Some(str)))
                        if str == CASE_ELSE_MARKER =>
                    {
                        else_expr = Some(Box::new(expressions[i + 1].clone()));
                        i += 2;
                    }
                    _ => {
                        when_then.push((
                            Box::new(expressions[i].clone()),
                            Box::new(expressions[i + 1].clone()),
                        ));
                        i += 2;
                    }
                }
            }

            Ok(Expr::Case {
                expr: base_expr,
                when_then_expr: when_then,
                else_expr,
            })
        }
        Expr::Cast { data_type, .. } => Ok(Expr::Cast {
            expr: Box::new(expressions[0].clone()),
            data_type: data_type.clone(),
        }),
        Expr::TryCast { data_type, .. } => Ok(Expr::TryCast {
            expr: Box::new(expressions[0].clone()),
            data_type: data_type.clone(),
        }),
        Expr::Alias(_, alias) => {
            Ok(Expr::Alias(Box::new(expressions[0].clone()), alias.clone()))
        }
        Expr::Not(_) => Ok(Expr::Not(Box::new(expressions[0].clone()))),
        Expr::Negative(_) => Ok(Expr::Negative(Box::new(expressions[0].clone()))),
        Expr::Column(_) => Ok(expr.clone()),
        Expr::Literal(_) => Ok(expr.clone()),
        Expr::ScalarVariable(_) => Ok(expr.clone()),
        Expr::Sort {
            asc, nulls_first, ..
        } => Ok(Expr::Sort {
            expr: Box::new(expressions[0].clone()),
            asc: *asc,
            nulls_first: *nulls_first,
        }),
        Expr::Between { negated, .. } => {
            let expr = Expr::BinaryExpr {
                left: Box::new(Expr::BinaryExpr {
                    left: Box::new(expressions[0].clone()),
                    op: Operator::GtEq,
                    right: Box::new(expressions[1].clone()),
                }),
                op: Operator::And,
                right: Box::new(Expr::BinaryExpr {
                    left: Box::new(expressions[0].clone()),
                    op: Operator::LtEq,
                    right: Box::new(expressions[2].clone()),
                }),
            };

            if *negated {
                Ok(Expr::Not(Box::new(expr)))
            } else {
                Ok(expr)
            }
        }
        Expr::InList { .. } => Ok(expr.clone()),
        Expr::Wildcard { .. } => Err(DataFusionError::Internal(
            "Wildcard expressions are not valid in a logical query plan".to_owned(),
        )),
        Expr::GetIndexedField { expr: _, key } => Ok(Expr::GetIndexedField {
            expr: Box::new(expressions[0].clone()),
            key: key.clone(),
        }),
    }
}

/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle other algebriac rewrites such as `(a and false)` --> `a`
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::optimizer::utils::ConstEvaluator;
/// # use datafusion::execution::context::ExecutionProps;
///
/// let execution_props = ExecutionProps::new();
/// let mut const_evaluator = ConstEvaluator::new(&execution_props);
///
/// // (1 + 2) + a
/// let expr = (lit(1) + lit(2)) + col("a");
///
/// // is rewritten to (3 + a);
/// let rewritten = expr.rewrite(&mut const_evaluator).unwrap();
/// assert_eq!(rewritten, lit(3) + col("a"));
/// ```
pub struct ConstEvaluator {
    /// can_evaluate is used during the depth-first-search of the
    /// Expr tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, can_evaluate[N] represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if can_evauate.top() is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this Expr can be evaluated
    can_evaluate: Vec<bool>,

    ctx_state: ExecutionContextState,
    planner: DefaultPhysicalPlanner,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

impl ExprRewriter for ConstEvaluator {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that is non evaluateable

        if !Self::can_evaluate(expr) {
            // walk back up stack, marking first parent that is not mutable
            let parent_iter = self.can_evaluate.iter_mut().rev();
            for p in parent_iter {
                if !*p {
                    // optimization: if we find an element on the
                    // stack already marked, know all elements above are also marked
                    break;
                }
                *p = false;
            }
        }

        // NB: do not short circuit recursion even if we find a non
        // evaluatable node (so we can fold other children, args to
        // functions, etc)
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.can_evaluate.pop().unwrap() {
            let scalar = self.evaluate_to_scalar(expr)?;
            Ok(Expr::Literal(scalar))
        } else {
            Ok(expr)
        }
    }
}

impl ConstEvaluator {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn new(execution_props: &ExecutionProps) -> Self {
        let planner = DefaultPhysicalPlanner::default();
        let ctx_state = ExecutionContextState {
            execution_props: execution_props.clone(),
            ..ExecutionContextState::new()
        };
        let input_schema = DFSchema::empty();

        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);

        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch =
            RecordBatch::try_new(std::sync::Arc::new(schema), vec![col]).unwrap();

        Self {
            can_evaluate: vec![],
            ctx_state,
            planner,
            input_schema,
            input_batch,
        }
    }

    /// Can a function of the specified volatility be evaluated?
    fn volatility_ok(volatility: Volatility) -> bool {
        match volatility {
            Volatility::Immutable => true,
            // Values for functions such as now() are taken from ExecutionProps
            Volatility::Stable => true,
            Volatility::Volatile => false,
        }
    }

    /// Can the expression be evaluated at plan time, (assuming all of
    /// its children can also be evaluated)?
    fn can_evaluate(expr: &Expr) -> bool {
        // check for reasons we can't evaluate this node
        //
        // NOTE all expr types are listed here so when new ones are
        // added they can be checked for their ability to be evaluated
        // at plan time
        match expr {
            // Has no runtime cost, but needed during planning
            Expr::Alias(..) => false,
            Expr::AggregateFunction { .. } => false,
            Expr::AggregateUDF { .. } => false,
            Expr::ScalarVariable(_) => false,
            Expr::Column(_) => false,
            Expr::ScalarFunction { fun, .. } => Self::volatility_ok(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => Self::volatility_ok(fun.signature.volatility),
            Expr::WindowFunction { .. } => false,
            Expr::Sort { .. } => false,
            Expr::Wildcard => false,

            Expr::Literal(_) => true,
            Expr::BinaryExpr { .. } => true,
            Expr::Not(_) => true,
            Expr::IsNotNull(_) => true,
            Expr::IsNull(_) => true,
            Expr::Negative(_) => true,
            Expr::Between { .. } => true,
            Expr::Case { .. } => true,
            Expr::Cast { .. } => true,
            Expr::TryCast { .. } => true,
            Expr::InList { .. } => true,
            Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    fn evaluate_to_scalar(&self, expr: Expr) -> Result<ScalarValue> {
        if let Expr::Literal(s) = expr {
            return Ok(s);
        }

        let phys_expr = self.planner.create_physical_expr(
            &expr,
            &self.input_schema,
            &self.input_batch.schema(),
            &self.ctx_state,
        )?;
        let col_val = phys_expr.evaluate(&self.input_batch)?;
        match col_val {
            crate::physical_plan::ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    Err(DataFusionError::Execution(format!(
                        "Could not evaluate the expressison, found a result of length {}",
                        a.len()
                    )))
                } else {
                    Ok(ScalarValue::try_from_array(&a, 0)?)
                }
            }
            crate::physical_plan::ColumnarValue::Scalar(s) => Ok(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        logical_plan::{col, create_udf, lit_timestamp_nano},
        physical_plan::{
            functions::{make_scalar_function, BuiltinScalarFunction},
            udf::ScalarUDF,
        },
    };
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::DataType,
    };
    use chrono::{DateTime, TimeZone, Utc};
    use std::collections::HashSet;

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }

    #[test]
    fn test_const_evaluator() {
        // true --> true
        test_evaluate(lit(true), lit(true));
        // true or true --> true
        test_evaluate(lit(true).or(lit(true)), lit(true));
        // true or false --> true
        test_evaluate(lit(true).or(lit(false)), lit(true));

        // "foo" == "foo" --> true
        test_evaluate(lit("foo").eq(lit("foo")), lit(true));
        // "foo" != "foo" --> false
        test_evaluate(lit("foo").not_eq(lit("foo")), lit(false));

        // c = 1 --> c = 1
        test_evaluate(col("c").eq(lit(1)), col("c").eq(lit(1)));
        // c = 1 + 2 --> c + 3
        test_evaluate(col("c").eq(lit(1) + lit(2)), col("c").eq(lit(3)));
        // (foo != foo) OR (c = 1) --> false OR (c = 1)
        test_evaluate(
            (lit("foo").not_eq(lit("foo"))).or(col("c").eq(lit(1))),
            lit(false).or(col("c").eq(lit(1))),
        );
    }

    #[test]
    fn test_const_evaluator_scalar_functions() {
        // concat("foo", "bar") --> "foobar"
        let expr = Expr::ScalarFunction {
            args: vec![lit("foo"), lit("bar")],
            fun: BuiltinScalarFunction::Concat,
        };
        test_evaluate(expr, lit("foobar"));

        // ensure arguments are also constant folded
        // concat("foo", concat("bar", "baz")) --> "foobarbaz"
        let concat1 = Expr::ScalarFunction {
            args: vec![lit("bar"), lit("baz")],
            fun: BuiltinScalarFunction::Concat,
        };
        let expr = Expr::ScalarFunction {
            args: vec![lit("foo"), concat1],
            fun: BuiltinScalarFunction::Concat,
        };
        test_evaluate(expr, lit("foobarbaz"));

        // Check non string arguments
        // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400000000000i64)
        let expr = Expr::ScalarFunction {
            args: vec![lit("2020-09-08T12:00:00+00:00")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = Expr::ScalarFunction {
            args: vec![col("a")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr.clone(), expr);

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = Expr::ScalarFunction {
            args: vec![col("a")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr.clone(), expr);

        // volatile / stable functions should not be evaluated
        // rand() + (1 + 2) --> rand() + 3
        let fun = BuiltinScalarFunction::Random;
        assert_eq!(fun.volatility(), Volatility::Volatile);
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = rand.clone() + (lit(1) + lit(2));
        let expected = rand + lit(3);
        test_evaluate(expr, expected);

        // parenthesization matters: can't rewrite
        // (rand() + 1) + 2 --> (rand() + 1) + 2)
        let fun = BuiltinScalarFunction::Random;
        assert_eq!(fun.volatility(), Volatility::Volatile);
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = (rand + lit(1)) + lit(2);
        test_evaluate(expr.clone(), expr);
    }

    #[test]
    fn test_const_evaluator_now() {
        let ts_nanos = 1599566400000000000i64;
        let time = chrono::Utc.timestamp_nanos(ts_nanos);
        let ts_string = "2020-09-08T12:05:00+00:00";

        // now() --> ts
        test_evaluate_with_start_time(now_expr(), lit_timestamp_nano(ts_nanos), &time);

        // CAST(now() as int64) + 100 --> ts + 100
        let expr = cast_to_int64_expr(now_expr()) + lit(100);
        test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

        //  now() < cast(to_timestamp(...) as int) + 50000 ---> true
        let expr = cast_to_int64_expr(now_expr())
            .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000));
        test_evaluate_with_start_time(expr, lit(true), &time);
    }

    fn now_expr() -> Expr {
        Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }
    }

    fn cast_to_int64_expr(expr: Expr) -> Expr {
        Expr::Cast {
            expr: expr.into(),
            data_type: DataType::Int64,
        }
    }

    fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
        Expr::ScalarFunction {
            args: vec![lit(arg.into())],
            fun: BuiltinScalarFunction::ToTimestamp,
        }
    }

    #[test]
    fn test_evaluator_udfs() {
        let args = vec![lit(1) + lit(2), lit(30) + lit(40)];
        let folded_args = vec![lit(3), lit(70)];

        // immutable UDF should get folded
        // udf_add(1+2, 30+40) --> 73
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: make_udf_add(Volatility::Immutable),
        };
        test_evaluate(expr, lit(73));

        // stable UDF should be entirely folded
        // udf_add(1+2, 30+40) --> 73
        let fun = make_udf_add(Volatility::Stable);
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, lit(73));

        // volatile UDF should have args folded
        // udf_add(1+2, 30+40) --> udf_add(3, 70)
        let fun = make_udf_add(Volatility::Volatile);
        let expr = Expr::ScalarUDF {
            args,
            fun: Arc::clone(&fun),
        };
        let expected_expr = Expr::ScalarUDF {
            args: folded_args,
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, expected_expr);
    }

    // Make a UDF that adds its two values together, with the specified volatility
    fn make_udf_add(volatility: Volatility) -> Arc<ScalarUDF> {
        let input_types = vec![DataType::Int32, DataType::Int32];
        let return_type = Arc::new(DataType::Int32);

        let fun = |args: &[ArrayRef]| {
            let arg0 = &args[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");
            let arg1 = &args[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");

            // 2. perform the computation
            let array = arg0
                .iter()
                .zip(arg1.iter())
                .map(|args| {
                    if let (Some(arg0), Some(arg1)) = args {
                        Some(arg0 + arg1)
                    } else {
                        // one or both args were Null
                        None
                    }
                })
                .collect::<Int32Array>();

            Ok(Arc::new(array) as ArrayRef)
        };

        let fun = make_scalar_function(fun);
        Arc::new(create_udf(
            "udf_add",
            input_types,
            return_type,
            volatility,
            fun,
        ))
    }

    fn test_evaluate_with_start_time(
        input_expr: Expr,
        expected_expr: Expr,
        date_time: &DateTime<Utc>,
    ) {
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
        };

        let mut const_evaluator = ConstEvaluator::new(&execution_props);
        let evaluated_expr = input_expr
            .clone()
            .rewrite(&mut const_evaluator)
            .expect("successfully evaluated");

        assert_eq!(
            evaluated_expr, expected_expr,
            "Mismatch evaluating {}\n  Expected:{}\n  Got:{}",
            input_expr, expected_expr, evaluated_expr
        );
    }

    fn test_evaluate(input_expr: Expr, expected_expr: Expr) {
        test_evaluate_with_start_time(input_expr, expected_expr, &Utc::now())
    }
}

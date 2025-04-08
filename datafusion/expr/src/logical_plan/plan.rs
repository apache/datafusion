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

//! Logical plan types

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use super::dml::CopyTo;
use super::invariants::{
    assert_always_invariants_at_current_node, assert_executable_invariants,
    InvariantLevel,
};
use super::DdlStatement;
use crate::builder::{change_redundant_column, unnest_with_options};
use crate::expr::{Placeholder, Sort as SortExpr, WindowFunction, WindowFunctionParams};
use crate::expr_rewriter::{
    create_col_from_scalar_expr, normalize_cols, normalize_sorts, NamePreserver,
};
use crate::logical_plan::display::{GraphvizVisitor, IndentVisitor};
use crate::logical_plan::extension::UserDefinedLogicalNode;
use crate::logical_plan::{DmlStatement, Statement};
use crate::utils::{
    enumerate_grouping_sets, exprlist_to_fields, find_out_reference_exprs,
    grouping_set_expr_count, grouping_set_to_exprlist, split_conjunction,
};
use crate::{
    build_join_schema, expr_vec_fmt, BinaryExpr, CreateMemoryTable, CreateView, Execute,
    Expr, ExprSchemable, LogicalPlanBuilder, Operator, Prepare,
    TableProviderFilterPushDown, TableSource, WindowFunctionDefinition,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::cse::{NormalizeEq, Normalizeable};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};
use datafusion_common::{
    aggregate_functional_dependencies, internal_err, plan_err, Column, Constraints,
    DFSchema, DFSchemaRef, DataFusionError, Dependency, FunctionalDependence,
    FunctionalDependencies, ParamValues, Result, ScalarValue, Spans, TableReference,
    UnnestOptions,
};
use indexmap::IndexSet;

// backwards compatibility
use crate::display::PgJsonVisitor;
pub use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
pub use datafusion_common::{JoinConstraint, JoinType};

/// A `LogicalPlan` is a node in a tree of relational operators (such as
/// Projection or Filter).
///
/// Represents transforming an input relation (table) to an output relation
/// (table) with a potentially different schema. Plans form a dataflow tree
/// where data flows from leaves up to the root to produce the query result.
///
/// `LogicalPlan`s can be created by the SQL query planner, the DataFrame API,
/// or programmatically (for example custom query languages).
///
/// # See also:
/// * [`Expr`]: For the expressions that are evaluated by the plan
/// * [`LogicalPlanBuilder`]: For building `LogicalPlan`s
/// * [`tree_node`]: To inspect and rewrite `LogicalPlan`s
///
/// [`tree_node`]: crate::logical_plan::tree_node
///
/// # Examples
///
/// ## Creating a LogicalPlan from SQL:
///
/// See [`SessionContext::sql`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql)
///
/// ## Creating a LogicalPlan from the DataFrame API:
///
/// See [`DataFrame::logical_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan)
///
/// ## Creating a LogicalPlan programmatically:
///
/// See [`LogicalPlanBuilder`]
///
/// # Visiting and Rewriting `LogicalPlan`s
///
/// Using the [`tree_node`] API, you can recursively walk all nodes in a
/// `LogicalPlan`. For example, to find all column references in a plan:
///
/// ```
/// # use std::collections::HashSet;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
/// # use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
/// # use datafusion_common::{Column, Result};
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("name", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// // Projection(name, salary)
/// //   Filter(salary > 1000)
/// //     TableScan(employee)
/// # fn main() -> Result<()> {
/// let plan = table_scan(Some("employee"), &employee_schema(), None)?
///  .filter(col("salary").gt(lit(1000)))?
///  .project(vec![col("name")])?
///  .build()?;
///
/// // use apply to walk the plan and collect all expressions
/// let mut expressions = HashSet::new();
/// plan.apply(|node| {
///   // collect all expressions in the plan
///   node.apply_expressions(|expr| {
///    expressions.insert(expr.clone());
///    Ok(TreeNodeRecursion::Continue) // control walk of expressions
///   })?;
///   Ok(TreeNodeRecursion::Continue) // control walk of plan nodes
/// }).unwrap();
///
/// // we found the expression in projection and filter
/// assert_eq!(expressions.len(), 2);
/// println!("Found expressions: {:?}", expressions);
/// // found predicate in the Filter: employee.salary > 1000
/// let salary = Expr::Column(Column::new(Some("employee"), "salary"));
/// assert!(expressions.contains(&salary.gt(lit(1000))));
/// // found projection in the Projection: employee.name
/// let name = Expr::Column(Column::new(Some("employee"), "name"));
/// assert!(expressions.contains(&name));
/// # Ok(())
/// # }
/// ```
///
/// You can also rewrite plans using the [`tree_node`] API. For example, to
/// replace the filter predicate in a plan:
///
/// ```
/// # use std::collections::HashSet;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
/// # use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
/// # use datafusion_common::{Column, Result};
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("name", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// // Projection(name, salary)
/// //   Filter(salary > 1000)
/// //     TableScan(employee)
/// # fn main() -> Result<()> {
/// use datafusion_common::tree_node::Transformed;
/// let plan = table_scan(Some("employee"), &employee_schema(), None)?
///  .filter(col("salary").gt(lit(1000)))?
///  .project(vec![col("name")])?
///  .build()?;
///
/// // use transform to rewrite the plan
/// let transformed_result = plan.transform(|node| {
///   // when we see the filter node
///   if let LogicalPlan::Filter(mut filter) = node {
///     // replace predicate with salary < 2000
///     filter.predicate = Expr::Column(Column::new(Some("employee"), "salary")).lt(lit(2000));
///     let new_plan = LogicalPlan::Filter(filter);
///     return Ok(Transformed::yes(new_plan)); // communicate the node was changed
///   }
///   // return the node unchanged
///   Ok(Transformed::no(node))
/// }).unwrap();
///
/// // Transformed result contains rewritten plan and information about
/// // whether the plan was changed
/// assert!(transformed_result.transformed);
/// let rewritten_plan = transformed_result.data;
///
/// // we found the filter
/// assert_eq!(rewritten_plan.display_indent().to_string(),
/// "Projection: employee.name\
/// \n  Filter: employee.salary < Int32(2000)\
/// \n    TableScan: employee");
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection(Projection),
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the
    /// input; If the value of `<predicate>` is true, the input row is
    /// passed to the output. If the value of `<predicate>` is false
    /// (or null), the row is discarded.
    Filter(Filter),
    /// Windows input based on a set of window spec and window
    /// function (e.g. SUM or RANK).  This is used to implement SQL
    /// window functions, and the `OVER` clause.
    ///
    /// See [`Window`] for more details
    Window(Window),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM). This is used to implement SQL aggregates
    /// and `GROUP BY`.
    ///
    /// See [`Aggregate`] for more details
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions. This
    /// is used to implement SQL `ORDER BY`
    Sort(Sort),
    /// Join two logical plans on one or more join columns.
    /// This is used to implement SQL `JOIN`
    Join(Join),
    /// Repartitions the input based on a partitioning scheme. This is
    /// used to add parallelism and is sometimes referred to as an
    /// "exchange" operator in other systems
    Repartition(Repartition),
    /// Union multiple inputs with the same schema into a single
    /// output stream. This is used to implement SQL `UNION [ALL]` and
    /// `INTERSECT [ALL]`.
    Union(Union),
    /// Produces rows from a [`TableSource`], used to implement SQL
    /// `FROM` tables or views.
    TableScan(TableScan),
    /// Produces no rows: An empty relation with an empty schema that
    /// produces 0 or 1 row. This is used to implement SQL `SELECT`
    /// that has no values in the `FROM` clause.
    EmptyRelation(EmptyRelation),
    /// Produces the output of running another query.  This is used to
    /// implement SQL subqueries
    Subquery(Subquery),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
    /// A DataFusion [`Statement`] such as `SET VARIABLE` or `START TRANSACTION`
    Statement(Statement),
    /// Values expression. See
    /// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details. This is used to implement SQL such as
    /// `VALUES (1, 2), (3, 4)`
    Values(Values),
    /// Produces a relation with string representations of
    /// various parts of the plan. This is used to implement SQL `EXPLAIN`.
    Explain(Explain),
    /// Runs the input, and prints annotated physical plan as a string
    /// with execution metric. This is used to implement SQL
    /// `EXPLAIN ANALYZE`.
    Analyze(Analyze),
    /// Extension operator defined outside of DataFusion. This is used
    /// to extend DataFusion with custom relational operations that
    Extension(Extension),
    /// Remove duplicate rows from the input. This is used to
    /// implement SQL `SELECT DISTINCT ...`.
    Distinct(Distinct),
    /// Data Manipulation Language (DML): Insert / Update / Delete
    Dml(DmlStatement),
    /// Data Definition Language (DDL): CREATE / DROP TABLES / VIEWS / SCHEMAS
    Ddl(DdlStatement),
    /// `COPY TO` for writing plan results to files
    Copy(CopyTo),
    /// Describe the schema of the table. This is used to implement the
    /// SQL `DESCRIBE` command from MySQL.
    DescribeTable(DescribeTable),
    /// Unnest a column that contains a nested list type such as an
    /// ARRAY. This is used to implement SQL `UNNEST`
    Unnest(Unnest),
    /// A variadic query (e.g. "Recursive CTEs")
    RecursiveQuery(RecursiveQuery),
}

impl Default for LogicalPlan {
    fn default() -> Self {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }
}

impl<'a> TreeNodeContainer<'a, Self> for LogicalPlan {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema,
            LogicalPlan::Values(Values { schema, .. }) => schema,
            LogicalPlan::TableScan(TableScan {
                projected_schema, ..
            }) => projected_schema,
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Distinct(Distinct::All(input)) => input.schema(),
            LogicalPlan::Distinct(Distinct::On(DistinctOn { schema, .. })) => schema,
            LogicalPlan::Window(Window { schema, .. }) => schema,
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Repartition(Repartition { input, .. }) => input.schema(),
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Statement(statement) => statement.schema(),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => subquery.schema(),
            LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. }) => schema,
            LogicalPlan::Explain(explain) => &explain.schema,
            LogicalPlan::Analyze(analyze) => &analyze.schema,
            LogicalPlan::Extension(extension) => extension.node.schema(),
            LogicalPlan::Union(Union { schema, .. }) => schema,
            LogicalPlan::DescribeTable(DescribeTable { output_schema, .. }) => {
                output_schema
            }
            LogicalPlan::Dml(DmlStatement { output_schema, .. }) => output_schema,
            LogicalPlan::Copy(CopyTo { input, .. }) => input.schema(),
            LogicalPlan::Ddl(ddl) => ddl.schema(),
            LogicalPlan::Unnest(Unnest { schema, .. }) => schema,
            LogicalPlan::RecursiveQuery(RecursiveQuery { static_term, .. }) => {
                // we take the schema of the static term as the schema of the entire recursive query
                static_term.schema()
            }
        }
    }

    /// Used for normalizing columns, as the fallback schemas to the main schema
    /// of the plan.
    pub fn fallback_normalize_schemas(&self) -> Vec<&DFSchema> {
        match self {
            LogicalPlan::Window(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Join(_) => self
                .inputs()
                .iter()
                .map(|input| input.schema().as_ref())
                .collect(),
            _ => vec![],
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }

    /// Returns the (fixed) output schema for `DESCRIBE` plans
    pub fn describe_schema() -> Schema {
        Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ])
    }

    /// Returns all expressions (non-recursively) evaluated by the current
    /// logical plan node. This does not include expressions in any children.
    ///
    /// Note this method `clone`s all the expressions. When possible, the
    /// [`tree_node`] API should be used instead of this API.
    ///
    /// The returned expressions do not necessarily represent or even
    /// contributed to the output schema of this node. For example,
    /// `LogicalPlan::Filter` returns the filter expression even though the
    /// output of a Filter has the same columns as the input.
    ///
    /// The expressions do contain all the columns that are used by this plan,
    /// so if there are columns not referenced by these expressions then
    /// DataFusion's optimizer attempts to optimize them away.
    ///
    /// [`tree_node`]: crate::logical_plan::tree_node
    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.apply_expressions(|e| {
            exprs.push(e.clone());
            Ok(TreeNodeRecursion::Continue)
        })
        // closure always returns OK
        .unwrap();
        exprs
    }

    /// Returns all the out reference(correlated) expressions (recursively) in the current
    /// logical plan nodes and all its descendant nodes.
    pub fn all_out_ref_exprs(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.apply_expressions(|e| {
            find_out_reference_exprs(e).into_iter().for_each(|e| {
                if !exprs.contains(&e) {
                    exprs.push(e)
                }
            });
            Ok(TreeNodeRecursion::Continue)
        })
        // closure always returns OK
        .unwrap();
        self.inputs()
            .into_iter()
            .flat_map(|child| child.all_out_ref_exprs())
            .for_each(|e| {
                if !exprs.contains(&e) {
                    exprs.push(e)
                }
            });
        exprs
    }

    /// Returns all inputs / children of this `LogicalPlan` node.
    ///
    /// Note does not include inputs to inputs, or subqueries.
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input],
            LogicalPlan::Repartition(Repartition { input, .. }) => vec![input],
            LogicalPlan::Window(Window { input, .. }) => vec![input],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input],
            LogicalPlan::Sort(Sort { input, .. }) => vec![input],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::Subquery(Subquery { subquery, .. }) => vec![subquery],
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => vec![input],
            LogicalPlan::Extension(extension) => extension.node.inputs(),
            LogicalPlan::Union(Union { inputs, .. }) => {
                inputs.iter().map(|arc| arc.as_ref()).collect()
            }
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => vec![input],
            LogicalPlan::Explain(explain) => vec![&explain.plan],
            LogicalPlan::Analyze(analyze) => vec![&analyze.input],
            LogicalPlan::Dml(write) => vec![&write.input],
            LogicalPlan::Copy(copy) => vec![&copy.input],
            LogicalPlan::Ddl(ddl) => ddl.inputs(),
            LogicalPlan::Unnest(Unnest { input, .. }) => vec![input],
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                static_term,
                recursive_term,
                ..
            }) => vec![static_term, recursive_term],
            LogicalPlan::Statement(stmt) => stmt.inputs(),
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => vec![],
        }
    }

    /// returns all `Using` join columns in a logical plan
    pub fn using_columns(&self) -> Result<Vec<HashSet<Column>>, DataFusionError> {
        let mut using_columns: Vec<HashSet<Column>> = vec![];

        self.apply_with_subqueries(|plan| {
            if let LogicalPlan::Join(Join {
                join_constraint: JoinConstraint::Using,
                on,
                ..
            }) = plan
            {
                // The join keys in using-join must be columns.
                let columns =
                    on.iter().try_fold(HashSet::new(), |mut accumu, (l, r)| {
                        let Some(l) = l.get_as_join_column() else {
                            return internal_err!(
                                "Invalid join key. Expected column, found {l:?}"
                            );
                        };
                        let Some(r) = r.get_as_join_column() else {
                            return internal_err!(
                                "Invalid join key. Expected column, found {r:?}"
                            );
                        };
                        accumu.insert(l.to_owned());
                        accumu.insert(r.to_owned());
                        Result::<_, DataFusionError>::Ok(accumu)
                    })?;
                using_columns.push(columns);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(using_columns)
    }

    /// returns the first output expression of this `LogicalPlan` node.
    pub fn head_output_expr(&self) -> Result<Option<Expr>> {
        match self {
            LogicalPlan::Projection(projection) => {
                Ok(Some(projection.expr.as_slice()[0].clone()))
            }
            LogicalPlan::Aggregate(agg) => {
                if agg.group_expr.is_empty() {
                    Ok(Some(agg.aggr_expr.as_slice()[0].clone()))
                } else {
                    Ok(Some(agg.group_expr.as_slice()[0].clone()))
                }
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn { select_expr, .. })) => {
                Ok(Some(select_expr[0].clone()))
            }
            LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::Distinct(Distinct::All(input))
            | LogicalPlan::Sort(Sort { input, .. })
            | LogicalPlan::Limit(Limit { input, .. })
            | LogicalPlan::Repartition(Repartition { input, .. })
            | LogicalPlan::Window(Window { input, .. }) => input.head_output_expr(),
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                ..
            }) => match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if left.schema().fields().is_empty() {
                        right.head_output_expr()
                    } else {
                        left.head_output_expr()
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                    left.head_output_expr()
                }
                JoinType::RightSemi | JoinType::RightAnti => right.head_output_expr(),
            },
            LogicalPlan::RecursiveQuery(RecursiveQuery { static_term, .. }) => {
                static_term.head_output_expr()
            }
            LogicalPlan::Union(union) => Ok(Some(Expr::Column(Column::from(
                union.schema.qualified_field(0),
            )))),
            LogicalPlan::TableScan(table) => Ok(Some(Expr::Column(Column::from(
                table.projected_schema.qualified_field(0),
            )))),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let expr_opt = subquery_alias.input.head_output_expr()?;
                expr_opt
                    .map(|expr| {
                        Ok(Expr::Column(create_col_from_scalar_expr(
                            &expr,
                            subquery_alias.alias.to_string(),
                        )?))
                    })
                    .map_or(Ok(None), |v| v.map(Some))
            }
            LogicalPlan::Subquery(_) => Ok(None),
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_) => Ok(None),
        }
    }

    /// Recomputes schema and type information for this LogicalPlan if needed.
    ///
    /// Some `LogicalPlan`s may need to recompute their schema if the number or
    /// type of expressions have been changed (for example due to type
    /// coercion). For example [`LogicalPlan::Projection`]s schema depends on
    /// its expressions.
    ///
    /// Some `LogicalPlan`s schema is unaffected by any changes to their
    /// expressions. For example [`LogicalPlan::Filter`] schema is always the
    /// same as its input schema.
    ///
    /// This is useful after modifying a plans `Expr`s (or input plans) via
    /// methods such as [Self::map_children] and [Self::map_expressions]. Unlike
    /// [Self::with_new_exprs], this method does not require a new set of
    /// expressions or inputs plans.
    ///
    /// # Return value
    /// Returns an error if there is some issue recomputing the schema.
    ///
    /// # Notes
    ///
    /// * Does not recursively recompute schema for input (child) plans.
    pub fn recompute_schema(self) -> Result<Self> {
        match self {
            // Since expr may be different than the previous expr, schema of the projection
            // may change. We need to use try_new method instead of try_new_with_schema method.
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema: _,
            }) => Projection::try_new(expr, input).map(LogicalPlan::Projection),
            LogicalPlan::Dml(_) => Ok(self),
            LogicalPlan::Copy(_) => Ok(self),
            LogicalPlan::Values(Values { schema, values }) => {
                // todo it isn't clear why the schema is not recomputed here
                Ok(LogicalPlan::Values(Values { schema, values }))
            }
            LogicalPlan::Filter(Filter {
                predicate,
                input,
                having,
            }) => Filter::try_new_internal(predicate, input, having)
                .map(LogicalPlan::Filter),
            LogicalPlan::Repartition(_) => Ok(self),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema: _,
            }) => Window::try_new(window_expr, input).map(LogicalPlan::Window),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema: _,
            }) => Aggregate::try_new(input, group_expr, aggr_expr)
                .map(LogicalPlan::Aggregate),
            LogicalPlan::Sort(_) => Ok(self),
            LogicalPlan::Join(Join {
                left,
                right,
                filter,
                join_type,
                join_constraint,
                on,
                schema: _,
                null_equals_null,
            }) => {
                let schema =
                    build_join_schema(left.schema(), right.schema(), &join_type)?;

                let new_on: Vec<_> = on
                    .into_iter()
                    .map(|equi_expr| {
                        // SimplifyExpression rule may add alias to the equi_expr.
                        (equi_expr.0.unalias(), equi_expr.1.unalias())
                    })
                    .collect();

                Ok(LogicalPlan::Join(Join {
                    left,
                    right,
                    join_type,
                    join_constraint,
                    on: new_on,
                    filter,
                    schema: DFSchemaRef::new(schema),
                    null_equals_null,
                }))
            }
            LogicalPlan::Subquery(_) => Ok(self),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema: _,
            }) => SubqueryAlias::try_new(input, alias).map(LogicalPlan::SubqueryAlias),
            LogicalPlan::Limit(_) => Ok(self),
            LogicalPlan::Ddl(_) => Ok(self),
            LogicalPlan::Extension(Extension { node }) => {
                // todo make an API that does not require cloning
                // This requires a copy of the extension nodes expressions and inputs
                let expr = node.expressions();
                let inputs: Vec<_> = node.inputs().into_iter().cloned().collect();
                Ok(LogicalPlan::Extension(Extension {
                    node: node.with_exprs_and_inputs(expr, inputs)?,
                }))
            }
            LogicalPlan::Union(Union { inputs, schema }) => {
                let first_input_schema = inputs[0].schema();
                if schema.fields().len() == first_input_schema.fields().len() {
                    // If inputs are not pruned do not change schema
                    Ok(LogicalPlan::Union(Union { inputs, schema }))
                } else {
                    // A note on `Union`s constructed via `try_new_by_name`:
                    //
                    // At this point, the schema for each input should have
                    // the same width. Thus, we do not need to save whether a
                    // `Union` was created `BY NAME`, and can safely rely on the
                    // `try_new` initializer to derive the new schema based on
                    // column positions.
                    Ok(LogicalPlan::Union(Union::try_new(inputs)?))
                }
            }
            LogicalPlan::Distinct(distinct) => {
                let distinct = match distinct {
                    Distinct::All(input) => Distinct::All(input),
                    Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema: _,
                    }) => Distinct::On(DistinctOn::try_new(
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                    )?),
                };
                Ok(LogicalPlan::Distinct(distinct))
            }
            LogicalPlan::RecursiveQuery(_) => Ok(self),
            LogicalPlan::Analyze(_) => Ok(self),
            LogicalPlan::Explain(_) => Ok(self),
            LogicalPlan::TableScan(_) => Ok(self),
            LogicalPlan::EmptyRelation(_) => Ok(self),
            LogicalPlan::Statement(_) => Ok(self),
            LogicalPlan::DescribeTable(_) => Ok(self),
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns,
                options,
                ..
            }) => {
                // Update schema with unnested column type.
                unnest_with_options(Arc::unwrap_or_clone(input), exec_columns, options)
            }
        }
    }

    /// Returns a new `LogicalPlan` based on `self` with inputs and
    /// expressions replaced.
    ///
    /// Note this method creates an entirely new node, which requires a large
    /// amount of clone'ing. When possible, the [`tree_node`] API should be used
    /// instead of this API.
    ///
    /// The exprs correspond to the same order of expressions returned
    /// by [`Self::expressions`]. This function is used by optimizers
    /// to rewrite plans using the following pattern:
    ///
    /// [`tree_node`]: crate::logical_plan::tree_node
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
    /// let new_plan = plan.new_with_exprs(rewritten_exprs, new_inputs);
    /// ```
    pub fn with_new_exprs(
        &self,
        mut expr: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match self {
            // Since expr may be different than the previous expr, schema of the projection
            // may change. We need to use try_new method instead of try_new_with_schema method.
            LogicalPlan::Projection(Projection { .. }) => {
                let input = self.only_input(inputs)?;
                Projection::try_new(expr, Arc::new(input)).map(LogicalPlan::Projection)
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                target,
                op,
                ..
            }) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Dml(DmlStatement::new(
                    table_name.clone(),
                    Arc::clone(target),
                    op.clone(),
                    Arc::new(input),
                )))
            }
            LogicalPlan::Copy(CopyTo {
                input: _,
                output_url,
                file_type,
                options,
                partition_by,
            }) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Copy(CopyTo {
                    input: Arc::new(input),
                    output_url: output_url.clone(),
                    file_type: Arc::clone(file_type),
                    options: options.clone(),
                    partition_by: partition_by.clone(),
                }))
            }
            LogicalPlan::Values(Values { schema, .. }) => {
                self.assert_no_inputs(inputs)?;
                Ok(LogicalPlan::Values(Values {
                    schema: Arc::clone(schema),
                    values: expr
                        .chunks_exact(schema.fields().len())
                        .map(|s| s.to_vec())
                        .collect(),
                }))
            }
            LogicalPlan::Filter { .. } => {
                let predicate = self.only_expr(expr)?;
                let input = self.only_input(inputs)?;

                Filter::try_new(predicate, Arc::new(input)).map(LogicalPlan::Filter)
            }
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::RoundRobinBatch(n) => {
                    self.assert_no_expressions(expr)?;
                    let input = self.only_input(inputs)?;
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::RoundRobinBatch(*n),
                        input: Arc::new(input),
                    }))
                }
                Partitioning::Hash(_, n) => {
                    let input = self.only_input(inputs)?;
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::Hash(expr, *n),
                        input: Arc::new(input),
                    }))
                }
                Partitioning::DistributeBy(_) => {
                    let input = self.only_input(inputs)?;
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::DistributeBy(expr),
                        input: Arc::new(input),
                    }))
                }
            },
            LogicalPlan::Window(Window { window_expr, .. }) => {
                assert_eq!(window_expr.len(), expr.len());
                let input = self.only_input(inputs)?;
                Window::try_new(expr, Arc::new(input)).map(LogicalPlan::Window)
            }
            LogicalPlan::Aggregate(Aggregate { group_expr, .. }) => {
                let input = self.only_input(inputs)?;
                // group exprs are the first expressions
                let agg_expr = expr.split_off(group_expr.len());

                Aggregate::try_new(Arc::new(input), expr, agg_expr)
                    .map(LogicalPlan::Aggregate)
            }
            LogicalPlan::Sort(Sort {
                expr: sort_expr,
                fetch,
                ..
            }) => {
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Sort(Sort {
                    expr: expr
                        .into_iter()
                        .zip(sort_expr.iter())
                        .map(|(expr, sort)| sort.with_expr(expr))
                        .collect(),
                    input: Arc::new(input),
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Join(Join {
                join_type,
                join_constraint,
                on,
                null_equals_null,
                ..
            }) => {
                let (left, right) = self.only_two_inputs(inputs)?;
                let schema = build_join_schema(left.schema(), right.schema(), join_type)?;

                let equi_expr_count = on.len() * 2;
                assert!(expr.len() >= equi_expr_count);

                // Assume that the last expr, if any,
                // is the filter_expr (non equality predicate from ON clause)
                let filter_expr = if expr.len() > equi_expr_count {
                    expr.pop()
                } else {
                    None
                };

                // The first part of expr is equi-exprs,
                // and the struct of each equi-expr is like `left-expr = right-expr`.
                assert_eq!(expr.len(), equi_expr_count);
                let mut new_on = Vec::with_capacity(on.len());
                let mut iter = expr.into_iter();
                while let Some(left) = iter.next() {
                    let Some(right) = iter.next() else {
                        internal_err!("Expected a pair of expressions to construct the join on expression")?
                    };

                    // SimplifyExpression rule may add alias to the equi_expr.
                    new_on.push((left.unalias(), right.unalias()));
                }

                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    on: new_on,
                    filter: filter_expr,
                    schema: DFSchemaRef::new(schema),
                    null_equals_null: *null_equals_null,
                }))
            }
            LogicalPlan::Subquery(Subquery {
                outer_ref_columns,
                spans,
                ..
            }) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                let subquery = LogicalPlanBuilder::from(input).build()?;
                Ok(LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(subquery),
                    outer_ref_columns: outer_ref_columns.clone(),
                    spans: spans.clone(),
                }))
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                SubqueryAlias::try_new(Arc::new(input), alias.clone())
                    .map(LogicalPlan::SubqueryAlias)
            }
            LogicalPlan::Limit(Limit { skip, fetch, .. }) => {
                let old_expr_len = skip.iter().chain(fetch.iter()).count();
                if old_expr_len != expr.len() {
                    return internal_err!(
                        "Invalid number of new Limit expressions: expected {}, got {}",
                        old_expr_len,
                        expr.len()
                    );
                }
                // `LogicalPlan::expressions()` returns in [skip, fetch] order, so we can pop from the end.
                let new_fetch = fetch.as_ref().and_then(|_| expr.pop());
                let new_skip = skip.as_ref().and_then(|_| expr.pop());
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Limit(Limit {
                    skip: new_skip.map(Box::new),
                    fetch: new_fetch.map(Box::new),
                    input: Arc::new(input),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                if_not_exists,
                or_replace,
                column_defaults,
                temporary,
                ..
            })) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                    CreateMemoryTable {
                        input: Arc::new(input),
                        constraints: Constraints::empty(),
                        name: name.clone(),
                        if_not_exists: *if_not_exists,
                        or_replace: *or_replace,
                        column_defaults: column_defaults.clone(),
                        temporary: *temporary,
                    },
                )))
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                name,
                or_replace,
                definition,
                temporary,
                ..
            })) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    input: Arc::new(input),
                    name: name.clone(),
                    or_replace: *or_replace,
                    temporary: *temporary,
                    definition: definition.clone(),
                })))
            }
            LogicalPlan::Extension(e) => Ok(LogicalPlan::Extension(Extension {
                node: e.node.with_exprs_and_inputs(expr, inputs)?,
            })),
            LogicalPlan::Union(Union { schema, .. }) => {
                self.assert_no_expressions(expr)?;
                let input_schema = inputs[0].schema();
                // If inputs are not pruned do not change schema.
                let schema = if schema.fields().len() == input_schema.fields().len() {
                    Arc::clone(schema)
                } else {
                    Arc::clone(input_schema)
                };
                Ok(LogicalPlan::Union(Union {
                    inputs: inputs.into_iter().map(Arc::new).collect(),
                    schema,
                }))
            }
            LogicalPlan::Distinct(distinct) => {
                let distinct = match distinct {
                    Distinct::All(_) => {
                        self.assert_no_expressions(expr)?;
                        let input = self.only_input(inputs)?;
                        Distinct::All(Arc::new(input))
                    }
                    Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        ..
                    }) => {
                        let input = self.only_input(inputs)?;
                        let sort_expr = expr.split_off(on_expr.len() + select_expr.len());
                        let select_expr = expr.split_off(on_expr.len());
                        assert!(sort_expr.is_empty(), "with_new_exprs for Distinct does not support sort expressions");
                        Distinct::On(DistinctOn::try_new(
                            expr,
                            select_expr,
                            None, // no sort expressions accepted
                            Arc::new(input),
                        )?)
                    }
                };
                Ok(LogicalPlan::Distinct(distinct))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name, is_distinct, ..
            }) => {
                self.assert_no_expressions(expr)?;
                let (static_term, recursive_term) = self.only_two_inputs(inputs)?;
                Ok(LogicalPlan::RecursiveQuery(RecursiveQuery {
                    name: name.clone(),
                    static_term: Arc::new(static_term),
                    recursive_term: Arc::new(recursive_term),
                    is_distinct: *is_distinct,
                }))
            }
            LogicalPlan::Analyze(a) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Analyze(Analyze {
                    verbose: a.verbose,
                    schema: Arc::clone(&a.schema),
                    input: Arc::new(input),
                }))
            }
            LogicalPlan::Explain(e) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Explain(Explain {
                    verbose: e.verbose,
                    plan: Arc::new(input),
                    explain_format: e.explain_format.clone(),
                    stringified_plans: e.stringified_plans.clone(),
                    schema: Arc::clone(&e.schema),
                    logical_optimization_succeeded: e.logical_optimization_succeeded,
                }))
            }
            LogicalPlan::Statement(Statement::Prepare(Prepare {
                name,
                data_types,
                ..
            })) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                Ok(LogicalPlan::Statement(Statement::Prepare(Prepare {
                    name: name.clone(),
                    data_types: data_types.clone(),
                    input: Arc::new(input),
                })))
            }
            LogicalPlan::Statement(Statement::Execute(Execute { name, .. })) => {
                self.assert_no_inputs(inputs)?;
                Ok(LogicalPlan::Statement(Statement::Execute(Execute {
                    name: name.clone(),
                    parameters: expr,
                })))
            }
            LogicalPlan::TableScan(ts) => {
                self.assert_no_inputs(inputs)?;
                Ok(LogicalPlan::TableScan(TableScan {
                    filters: expr,
                    ..ts.clone()
                }))
            }
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::DescribeTable(_) => {
                // All of these plan types have no inputs / exprs so should not be called
                self.assert_no_expressions(expr)?;
                self.assert_no_inputs(inputs)?;
                Ok(self.clone())
            }
            LogicalPlan::Unnest(Unnest {
                exec_columns: columns,
                options,
                ..
            }) => {
                self.assert_no_expressions(expr)?;
                let input = self.only_input(inputs)?;
                // Update schema with unnested column type.
                let new_plan =
                    unnest_with_options(input, columns.clone(), options.clone())?;
                Ok(new_plan)
            }
        }
    }

    /// checks that the plan conforms to the listed invariant level, returning an Error if not
    pub fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        match check {
            InvariantLevel::Always => assert_always_invariants_at_current_node(self),
            InvariantLevel::Executable => assert_executable_invariants(self),
        }
    }

    /// Helper for [Self::with_new_exprs] to use when no expressions are expected.
    #[inline]
    #[allow(clippy::needless_pass_by_value)] // expr is moved intentionally to ensure it's not used again
    fn assert_no_expressions(&self, expr: Vec<Expr>) -> Result<()> {
        if !expr.is_empty() {
            return internal_err!("{self:?} should have no exprs, got {:?}", expr);
        }
        Ok(())
    }

    /// Helper for [Self::with_new_exprs] to use when no inputs are expected.
    #[inline]
    #[allow(clippy::needless_pass_by_value)] // inputs is moved intentionally to ensure it's not used again
    fn assert_no_inputs(&self, inputs: Vec<LogicalPlan>) -> Result<()> {
        if !inputs.is_empty() {
            return internal_err!("{self:?} should have no inputs, got: {:?}", inputs);
        }
        Ok(())
    }

    /// Helper for [Self::with_new_exprs] to use when exactly one expression is expected.
    #[inline]
    fn only_expr(&self, mut expr: Vec<Expr>) -> Result<Expr> {
        if expr.len() != 1 {
            return internal_err!(
                "{self:?} should have exactly one expr, got {:?}",
                expr
            );
        }
        Ok(expr.remove(0))
    }

    /// Helper for [Self::with_new_exprs] to use when exactly one input is expected.
    #[inline]
    fn only_input(&self, mut inputs: Vec<LogicalPlan>) -> Result<LogicalPlan> {
        if inputs.len() != 1 {
            return internal_err!(
                "{self:?} should have exactly one input, got {:?}",
                inputs
            );
        }
        Ok(inputs.remove(0))
    }

    /// Helper for [Self::with_new_exprs] to use when exactly two inputs are expected.
    #[inline]
    fn only_two_inputs(
        &self,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<(LogicalPlan, LogicalPlan)> {
        if inputs.len() != 2 {
            return internal_err!(
                "{self:?} should have exactly two inputs, got {:?}",
                inputs
            );
        }
        let right = inputs.remove(1);
        let left = inputs.remove(0);
        Ok((left, right))
    }

    /// Replaces placeholder param values (like `$1`, `$2`) in [`LogicalPlan`]
    /// with the specified `param_values`.
    ///
    /// [`Prepare`] statements are converted to
    /// their inner logical plan for execution.
    ///
    /// # Example
    /// ```
    /// # use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_common::ScalarValue;
    /// # use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan, placeholder};
    /// # let schema = Schema::new(vec![
    /// #     Field::new("id", DataType::Int32, false),
    /// # ]);
    /// // Build SELECT * FROM t1 WHERE id = $1
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(placeholder("$1"))).unwrap()
    ///     .build().unwrap();
    ///
    /// assert_eq!(
    ///   "Filter: t1.id = $1\
    ///   \n  TableScan: t1",
    ///   plan.display_indent().to_string()
    /// );
    ///
    /// // Fill in the parameter $1 with a literal 3
    /// let plan = plan.with_param_values(vec![
    ///   ScalarValue::from(3i32) // value at index 0 --> $1
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///    "Filter: t1.id = Int32(3)\
    ///    \n  TableScan: t1",
    ///    plan.display_indent().to_string()
    ///  );
    ///
    /// // Note you can also used named parameters
    /// // Build SELECT * FROM t1 WHERE id = $my_param
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(placeholder("$my_param"))).unwrap()
    ///     .build().unwrap()
    ///     // Fill in the parameter $my_param with a literal 3
    ///     .with_param_values(vec![
    ///       ("my_param", ScalarValue::from(3i32)),
    ///     ]).unwrap();
    ///
    /// assert_eq!(
    ///    "Filter: t1.id = Int32(3)\
    ///    \n  TableScan: t1",
    ///    plan.display_indent().to_string()
    ///  );
    ///
    /// ```
    pub fn with_param_values(
        self,
        param_values: impl Into<ParamValues>,
    ) -> Result<LogicalPlan> {
        let param_values = param_values.into();
        let plan_with_values = self.replace_params_with_values(&param_values)?;

        // unwrap Prepare
        Ok(
            if let LogicalPlan::Statement(Statement::Prepare(prepare_lp)) =
                plan_with_values
            {
                param_values.verify(&prepare_lp.data_types)?;
                // try and take ownership of the input if is not shared, clone otherwise
                Arc::unwrap_or_clone(prepare_lp.input)
            } else {
                plan_with_values
            },
        )
    }

    /// Returns the maximum number of rows that this plan can output, if known.
    ///
    /// If `None`, the plan can return any number of rows.
    /// If `Some(n)` then the plan can return at most `n` rows but may return fewer.
    pub fn max_rows(self: &LogicalPlan) -> Option<usize> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => input.max_rows(),
            LogicalPlan::Filter(filter) => {
                if filter.is_scalar() {
                    Some(1)
                } else {
                    filter.input.max_rows()
                }
            }
            LogicalPlan::Window(Window { input, .. }) => input.max_rows(),
            LogicalPlan::Aggregate(Aggregate {
                input, group_expr, ..
            }) => {
                // Empty group_expr will return Some(1)
                if group_expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Literal(_)))
                {
                    Some(1)
                } else {
                    input.max_rows()
                }
            }
            LogicalPlan::Sort(Sort { input, fetch, .. }) => {
                match (fetch, input.max_rows()) {
                    (Some(fetch_limit), Some(input_max)) => {
                        Some(input_max.min(*fetch_limit))
                    }
                    (Some(fetch_limit), None) => Some(*fetch_limit),
                    (None, Some(input_max)) => Some(input_max),
                    (None, None) => None,
                }
            }
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                ..
            }) => match join_type {
                JoinType::Inner => Some(left.max_rows()? * right.max_rows()?),
                JoinType::Left | JoinType::Right | JoinType::Full => {
                    match (left.max_rows()?, right.max_rows()?, join_type) {
                        (0, 0, _) => Some(0),
                        (max_rows, 0, JoinType::Left | JoinType::Full) => Some(max_rows),
                        (0, max_rows, JoinType::Right | JoinType::Full) => Some(max_rows),
                        (left_max, right_max, _) => Some(left_max * right_max),
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                    left.max_rows()
                }
                JoinType::RightSemi | JoinType::RightAnti => right.max_rows(),
            },
            LogicalPlan::Repartition(Repartition { input, .. }) => input.max_rows(),
            LogicalPlan::Union(Union { inputs, .. }) => {
                inputs.iter().try_fold(0usize, |mut acc, plan| {
                    acc += plan.max_rows()?;
                    Some(acc)
                })
            }
            LogicalPlan::TableScan(TableScan { fetch, .. }) => *fetch,
            LogicalPlan::EmptyRelation(_) => Some(0),
            LogicalPlan::RecursiveQuery(_) => None,
            LogicalPlan::Subquery(_) => None,
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => input.max_rows(),
            LogicalPlan::Limit(limit) => match limit.get_fetch_type() {
                Ok(FetchType::Literal(s)) => s,
                _ => None,
            },
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => input.max_rows(),
            LogicalPlan::Values(v) => Some(v.values.len()),
            LogicalPlan::Unnest(_) => None,
            LogicalPlan::Ddl(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Extension(_) => None,
        }
    }

    /// If this node's expressions contains any references to an outer subquery
    pub fn contains_outer_reference(&self) -> bool {
        let mut contains = false;
        self.apply_expressions(|expr| {
            Ok(if expr.contains_outer() {
                contains = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
        contains
    }

    /// Get the output expressions and their corresponding columns.
    ///
    /// The parent node may reference the output columns of the plan by expressions, such as
    /// projection over aggregate or window functions. This method helps to convert the
    /// referenced expressions into columns.
    ///
    /// See also: [`crate::utils::columnize_expr`]
    pub fn columnized_output_exprs(&self) -> Result<Vec<(&Expr, Column)>> {
        match self {
            LogicalPlan::Aggregate(aggregate) => Ok(aggregate
                .output_expressions()?
                .into_iter()
                .zip(self.schema().columns())
                .collect()),
            LogicalPlan::Window(Window {
                window_expr,
                input,
                schema,
            }) => {
                // The input could be another Window, so the result should also include the input's. For Example:
                // `EXPLAIN SELECT RANK() OVER (PARTITION BY a ORDER BY b), SUM(b) OVER (PARTITION BY a) FROM t`
                // Its plan is:
                // Projection: RANK() PARTITION BY [t.a] ORDER BY [t.b ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(t.b) PARTITION BY [t.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                //   WindowAggr: windowExpr=[[SUM(CAST(t.b AS Int64)) PARTITION BY [t.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
                //     WindowAggr: windowExpr=[[RANK() PARTITION BY [t.a] ORDER BY [t.b ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]/
                //       TableScan: t projection=[a, b]
                let mut output_exprs = input.columnized_output_exprs()?;
                let input_len = input.schema().fields().len();
                output_exprs.extend(
                    window_expr
                        .iter()
                        .zip(schema.columns().into_iter().skip(input_len)),
                );
                Ok(output_exprs)
            }
            _ => Ok(vec![]),
        }
    }
}

impl LogicalPlan {
    /// Return a `LogicalPlan` with all placeholders (e.g $1 $2,
    /// ...) replaced with corresponding values provided in
    /// `params_values`
    ///
    /// See [`Self::with_param_values`] for examples and usage with an owned
    /// `ParamValues`
    pub fn replace_params_with_values(
        self,
        param_values: &ParamValues,
    ) -> Result<LogicalPlan> {
        self.transform_up_with_subqueries(|plan| {
            let schema = Arc::clone(plan.schema());
            let name_preserver = NamePreserver::new(&plan);
            plan.map_expressions(|e| {
                let (e, has_placeholder) = e.infer_placeholder_types(&schema)?;
                if !has_placeholder {
                    // Performance optimization:
                    // avoid NamePreserver copy and second pass over expression
                    // if no placeholders.
                    Ok(Transformed::no(e))
                } else {
                    let original_name = name_preserver.save(&e);
                    let transformed_expr = e.transform_up(|e| {
                        if let Expr::Placeholder(Placeholder { id, .. }) = e {
                            let value = param_values.get_placeholders_with_values(&id)?;
                            Ok(Transformed::yes(Expr::Literal(value)))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })?;
                    // Preserve name to avoid breaking column references to this expression
                    Ok(transformed_expr.update_data(|expr| original_name.restore(expr)))
                }
            })
        })
        .map(|res| res.data)
    }

    /// Walk the logical plan, find any `Placeholder` tokens, and return a set of their names.
    pub fn get_parameter_names(&self) -> Result<HashSet<String>> {
        let mut param_names = HashSet::new();
        self.apply_with_subqueries(|plan| {
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::Placeholder(Placeholder { id, .. }) = expr {
                        param_names.insert(id.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })
        .map(|_| param_names)
    }

    /// Walk the logical plan, find any `Placeholder` tokens, and return a map of their IDs and DataTypes
    pub fn get_parameter_types(
        &self,
    ) -> Result<HashMap<String, Option<DataType>>, DataFusionError> {
        let mut param_types: HashMap<String, Option<DataType>> = HashMap::new();

        self.apply_with_subqueries(|plan| {
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::Placeholder(Placeholder { id, data_type }) = expr {
                        let prev = param_types.get(id);
                        match (prev, data_type) {
                            (Some(Some(prev)), Some(dt)) => {
                                if prev != dt {
                                    plan_err!("Conflicting types for {id}")?;
                                }
                            }
                            (_, Some(dt)) => {
                                param_types.insert(id.clone(), Some(dt.clone()));
                            }
                            _ => {
                                param_types.insert(id.clone(), None);
                            }
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })
        .map(|_| param_types)
    }

    // ------------
    // Various implementations for printing out LogicalPlans
    // ------------

    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// # Example
    ///
    /// ```text
    /// Projection: employee.id
    ///    Filter: employee.state Eq Utf8(\"CO\")\
    ///       CsvScan: employee projection=Some([0, 3])
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: t1.id = Int32(5)\n  TableScan: t1",
    ///             display_string);
    /// ```
    pub fn display_indent(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                match self.0.visit_with_subqueries(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces a single line
    /// per node that includes the output schema. For example:
    ///
    /// ```text
    /// Projection: employee.id [id:Int32]\
    ///    Filter: employee.state = Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee projection=[0, 3] [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: t1.id = Int32(5) [id:Int32]\
    ///             \n  TableScan: t1 [id:Int32]",
    ///             display_string);
    /// ```
    pub fn display_indent_schema(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let with_schema = true;
                let mut visitor = IndentVisitor::new(f, with_schema);
                match self.0.visit_with_subqueries(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a displayable structure that produces plan in postgresql JSON format.
    ///
    /// Users can use this format to visualize the plan in existing plan visualization tools, for example [dalibo](https://explain.dalibo.com/)
    pub fn display_pg_json(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = PgJsonVisitor::new(f);
                visitor.with_schema(true);
                match self.0.visit_with_subqueries(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces lines meant for
    /// graphical display using the `DOT` language. This format can be
    /// visualized using software from
    /// [`graphviz`](https://graphviz.org/)
    ///
    /// This currently produces two graphs -- one with the basic
    /// structure, and one with additional details such as schema.
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_graphviz
    /// let graphviz_string = format!("{}", plan.display_graphviz());
    /// ```
    ///
    /// If graphviz string is saved to a file such as `/tmp/example.dot`, the following
    /// commands can be used to render it as a pdf:
    ///
    /// ```bash
    ///   dot -Tpdf < /tmp/example.dot  > /tmp/example.pdf
    /// ```
    ///
    pub fn display_graphviz(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = GraphvizVisitor::new(f);

                visitor.start_graph()?;

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0
                    .visit_with_subqueries(&mut visitor)
                    .map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0
                    .visit_with_subqueries(&mut visitor)
                    .map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.end_graph()?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: id
    /// ```
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: t1", display_string);
    /// ```
    pub fn display(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self.0 {
                    LogicalPlan::EmptyRelation(_) => write!(f, "EmptyRelation"),
                    LogicalPlan::RecursiveQuery(RecursiveQuery {
                        is_distinct, ..
                    }) => {
                        write!(f, "RecursiveQuery: is_distinct={}", is_distinct)
                    }
                    LogicalPlan::Values(Values { ref values, .. }) => {
                        let str_values: Vec<_> = values
                            .iter()
                            // limit to only 5 values to avoid horrible display
                            .take(5)
                            .map(|row| {
                                let item = row
                                    .iter()
                                    .map(|expr| expr.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({item})")
                            })
                            .collect();

                        let eclipse = if values.len() > 5 { "..." } else { "" };
                        write!(f, "Values: {}{}", str_values.join(", "), eclipse)
                    }

                    LogicalPlan::TableScan(TableScan {
                        ref source,
                        ref table_name,
                        ref projection,
                        ref filters,
                        ref fetch,
                        ..
                    }) => {
                        let projected_fields = match projection {
                            Some(indices) => {
                                let schema = source.schema();
                                let names: Vec<&str> = indices
                                    .iter()
                                    .map(|i| schema.field(*i).name().as_str())
                                    .collect();
                                format!(" projection=[{}]", names.join(", "))
                            }
                            _ => "".to_string(),
                        };

                        write!(f, "TableScan: {table_name}{projected_fields}")?;

                        if !filters.is_empty() {
                            let mut full_filter = vec![];
                            let mut partial_filter = vec![];
                            let mut unsupported_filters = vec![];
                            let filters: Vec<&Expr> = filters.iter().collect();

                            if let Ok(results) =
                                source.supports_filters_pushdown(&filters)
                            {
                                filters.iter().zip(results.iter()).for_each(
                                    |(x, res)| match res {
                                        TableProviderFilterPushDown::Exact => {
                                            full_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Inexact => {
                                            partial_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Unsupported => {
                                            unsupported_filters.push(x)
                                        }
                                    },
                                );
                            }

                            if !full_filter.is_empty() {
                                write!(
                                    f,
                                    ", full_filters=[{}]",
                                    expr_vec_fmt!(full_filter)
                                )?;
                            };
                            if !partial_filter.is_empty() {
                                write!(
                                    f,
                                    ", partial_filters=[{}]",
                                    expr_vec_fmt!(partial_filter)
                                )?;
                            }
                            if !unsupported_filters.is_empty() {
                                write!(
                                    f,
                                    ", unsupported_filters=[{}]",
                                    expr_vec_fmt!(unsupported_filters)
                                )?;
                            }
                        }

                        if let Some(n) = fetch {
                            write!(f, ", fetch={n}")?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Projection(Projection { ref expr, .. }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Dml(DmlStatement { table_name, op, .. }) => {
                        write!(f, "Dml: op=[{op}] table=[{table_name}]")
                    }
                    LogicalPlan::Copy(CopyTo {
                        input: _,
                        output_url,
                        file_type,
                        options,
                        ..
                    }) => {
                        let op_str = options
                            .iter()
                            .map(|(k, v)| format!("{k} {v}"))
                            .collect::<Vec<String>>()
                            .join(", ");

                        write!(f, "CopyTo: format={} output_url={output_url} options: ({op_str})", file_type.get_ext())
                    }
                    LogicalPlan::Ddl(ddl) => {
                        write!(f, "{}", ddl.display())
                    }
                    LogicalPlan::Filter(Filter {
                        predicate: ref expr,
                        ..
                    }) => write!(f, "Filter: {expr}"),
                    LogicalPlan::Window(Window {
                        ref window_expr, ..
                    }) => {
                        write!(
                            f,
                            "WindowAggr: windowExpr=[[{}]]",
                            expr_vec_fmt!(window_expr)
                        )
                    }
                    LogicalPlan::Aggregate(Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    }) => write!(
                        f,
                        "Aggregate: groupBy=[[{}]], aggr=[[{}]]",
                        expr_vec_fmt!(group_expr),
                        expr_vec_fmt!(aggr_expr)
                    ),
                    LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        if let Some(a) = fetch {
                            write!(f, ", fetch={a}")?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Join(Join {
                        on: ref keys,
                        filter,
                        join_constraint,
                        join_type,
                        ..
                    }) => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{l} = {r}")).collect();
                        let filter_expr = filter
                            .as_ref()
                            .map(|expr| format!(" Filter: {expr}"))
                            .unwrap_or_else(|| "".to_string());
                        let join_type = if filter.is_none() && keys.is_empty() && matches!(join_type, JoinType::Inner) {
                            "Cross".to_string()
                        } else {
                            join_type.to_string()
                        };
                        match join_constraint {
                            JoinConstraint::On => {
                                write!(
                                    f,
                                    "{} Join: {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                            JoinConstraint::Using => {
                                write!(
                                    f,
                                    "{} Join: Using {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr,
                                )
                            }
                        }
                    }
                    LogicalPlan::Repartition(Repartition {
                        partitioning_scheme,
                        ..
                    }) => match partitioning_scheme {
                        Partitioning::RoundRobinBatch(n) => {
                            write!(f, "Repartition: RoundRobinBatch partition_count={n}")
                        }
                        Partitioning::Hash(expr, n) => {
                            let hash_expr: Vec<String> =
                                expr.iter().map(|e| format!("{e}")).collect();
                            write!(
                                f,
                                "Repartition: Hash({}) partition_count={}",
                                hash_expr.join(", "),
                                n
                            )
                        }
                        Partitioning::DistributeBy(expr) => {
                            let dist_by_expr: Vec<String> =
                                expr.iter().map(|e| format!("{e}")).collect();
                            write!(
                                f,
                                "Repartition: DistributeBy({})",
                                dist_by_expr.join(", "),
                            )
                        }
                    },
                    LogicalPlan::Limit(limit) => {
                        // Attempt to display `skip` and `fetch` as literals if possible, otherwise as expressions.
                        let skip_str = match limit.get_skip_type() {
                            Ok(SkipType::Literal(n)) => n.to_string(),
                            _ => limit.skip.as_ref().map_or_else(|| "None".to_string(), |x| x.to_string()),
                        };
                        let fetch_str = match limit.get_fetch_type() {
                            Ok(FetchType::Literal(Some(n))) => n.to_string(),
                            Ok(FetchType::Literal(None)) => "None".to_string(),
                            _ => limit.fetch.as_ref().map_or_else(|| "None".to_string(), |x| x.to_string())
                        };
                        write!(
                            f,
                            "Limit: skip={}, fetch={}", skip_str,fetch_str,
                        )
                    }
                    LogicalPlan::Subquery(Subquery { .. }) => {
                        write!(f, "Subquery:")
                    }
                    LogicalPlan::SubqueryAlias(SubqueryAlias { ref alias, .. }) => {
                        write!(f, "SubqueryAlias: {alias}")
                    }
                    LogicalPlan::Statement(statement) => {
                        write!(f, "{}", statement.display())
                    }
                    LogicalPlan::Distinct(distinct) => match distinct {
                        Distinct::All(_) => write!(f, "Distinct:"),
                        Distinct::On(DistinctOn {
                            on_expr,
                            select_expr,
                            sort_expr,
                            ..
                        }) => write!(
                            f,
                            "DistinctOn: on_expr=[[{}]], select_expr=[[{}]], sort_expr=[[{}]]",
                            expr_vec_fmt!(on_expr),
                            expr_vec_fmt!(select_expr),
                            if let Some(sort_expr) = sort_expr { expr_vec_fmt!(sort_expr) } else { "".to_string() },
                        ),
                    },
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Analyze { .. } => write!(f, "Analyze"),
                    LogicalPlan::Union(_) => write!(f, "Union"),
                    LogicalPlan::Extension(e) => e.node.fmt_for_explain(f),
                    LogicalPlan::DescribeTable(DescribeTable { .. }) => {
                        write!(f, "DescribeTable")
                    }
                    LogicalPlan::Unnest(Unnest {
                        input: plan,
                        list_type_columns: list_col_indices,
                        struct_type_columns: struct_col_indices, .. }) => {
                        let input_columns = plan.schema().columns();
                        let list_type_columns = list_col_indices
                            .iter()
                            .map(|(i,unnest_info)|
                                format!("{}|depth={}", &input_columns[*i].to_string(),
                                unnest_info.depth))
                            .collect::<Vec<String>>();
                        let struct_type_columns = struct_col_indices
                            .iter()
                            .map(|i| &input_columns[*i])
                            .collect::<Vec<&Column>>();
                        // get items from input_columns indexed by list_col_indices
                        write!(f, "Unnest: lists[{}] structs[{}]",
                        expr_vec_fmt!(list_type_columns),
                        expr_vec_fmt!(struct_type_columns))
                    }
                }
            }
        }
        Wrapper(self)
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

impl ToStringifiedPlan for LogicalPlan {
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.display_indent().to_string())
    }
}

/// Produces no rows: An empty relation with an empty schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmptyRelation {
    /// Whether to produce a placeholder row
    pub produce_one_row: bool,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for EmptyRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.produce_one_row.partial_cmp(&other.produce_one_row)
    }
}

/// A variadic query operation, Recursive CTE.
///
/// # Recursive Query Evaluation
///
/// From the [Postgres Docs]:
///
/// 1. Evaluate the non-recursive term. For `UNION` (but not `UNION ALL`),
///    discard duplicate rows. Include all remaining rows in the result of the
///    recursive query, and also place them in a temporary working table.
///
/// 2. So long as the working table is not empty, repeat these steps:
///
/// * Evaluate the recursive term, substituting the current contents of the
///   working table for the recursive self-reference. For `UNION` (but not `UNION
///   ALL`), discard duplicate rows and rows that duplicate any previous result
///   row. Include all remaining rows in the result of the recursive query, and
///   also place them in a temporary intermediate table.
///
/// * Replace the contents of the working table with the contents of the
///   intermediate table, then empty the intermediate table.
///
/// [Postgres Docs]: https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-RECURSIVE
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct RecursiveQuery {
    /// Name of the query
    pub name: String,
    /// The static term (initial contents of the working table)
    pub static_term: Arc<LogicalPlan>,
    /// The recursive term (evaluated on the contents of the working table until
    /// it returns an empty set)
    pub recursive_term: Arc<LogicalPlan>,
    /// Should the output of the recursive term be deduplicated (`UNION`) or
    /// not (`UNION ALL`).
    pub is_distinct: bool,
}

/// Values expression. See
/// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
/// documentation for more details.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values {
    /// The table schema
    pub schema: DFSchemaRef,
    /// Values
    pub values: Vec<Vec<Expr>>,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Values {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.values.partial_cmp(&other.values)
    }
}

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct Projection {
    /// The list of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Projection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.expr.partial_cmp(&other.expr) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            cmp => cmp,
        }
    }
}

impl Projection {
    /// Create a new Projection
    pub fn try_new(expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self> {
        let projection_schema = projection_schema(&input, &expr)?;
        Self::try_new_with_schema(expr, input, projection_schema)
    }

    /// Create a new Projection using the specified output schema
    pub fn try_new_with_schema(
        expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        #[expect(deprecated)]
        if !expr.iter().any(|e| matches!(e, Expr::Wildcard { .. }))
            && expr.len() != schema.fields().len()
        {
            return plan_err!("Projection has mismatch between number of expressions ({}) and number of fields in schema ({})", expr.len(), schema.fields().len());
        }
        Ok(Self {
            expr,
            input,
            schema,
        })
    }

    /// Create a new Projection using the specified output schema
    pub fn new_from_schema(input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Self {
        let expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
        Self {
            expr,
            input,
            schema,
        }
    }
}

/// Computes the schema of the result produced by applying a projection to the input logical plan.
///
/// # Arguments
///
/// * `input`: A reference to the input `LogicalPlan` for which the projection schema
///   will be computed.
/// * `exprs`: A slice of `Expr` expressions representing the projection operation to apply.
///
/// # Returns
///
/// A `Result` containing an `Arc<DFSchema>` representing the schema of the result
/// produced by the projection operation. If the schema computation is successful,
/// the `Result` will contain the schema; otherwise, it will contain an error.
pub fn projection_schema(input: &LogicalPlan, exprs: &[Expr]) -> Result<Arc<DFSchema>> {
    let metadata = input.schema().metadata().clone();

    let schema =
        DFSchema::new_with_metadata(exprlist_to_fields(exprs, input)?, metadata)?
            .with_functional_dependencies(calc_func_dependencies_for_project(
                exprs, input,
            )?)?;

    Ok(Arc::new(schema))
}

/// Aliased subquery
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The alias for the input relation
    pub alias: TableReference,
    /// The schema with qualified field names
    pub schema: DFSchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(
        plan: Arc<LogicalPlan>,
        alias: impl Into<TableReference>,
    ) -> Result<Self> {
        let alias = alias.into();
        let fields = change_redundant_column(plan.schema().fields());
        let meta_data = plan.schema().as_ref().metadata().clone();
        let schema: Schema =
            DFSchema::from_unqualified_fields(fields.into(), meta_data)?.into();
        // Since schema is the same, other than qualifier, we can use existing
        // functional dependencies:
        let func_dependencies = plan.schema().functional_dependencies().clone();
        let schema = DFSchemaRef::new(
            DFSchema::try_from_qualified_schema(alias.clone(), &schema)?
                .with_functional_dependencies(func_dependencies)?,
        );
        Ok(SubqueryAlias {
            input: plan,
            alias,
            schema,
        })
    }
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for SubqueryAlias {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => self.alias.partial_cmp(&other.alias),
            cmp => cmp,
        }
    }
}

/// Filters rows from its input that do not match an
/// expression (essentially a WHERE clause with a predicate
/// expression).
///
/// Semantically, `<predicate>` is evaluated for each row of the input;
/// If the value of `<predicate>` is true, the input row is passed to
/// the output. If the value of `<predicate>` is false, the row is
/// discarded.
///
/// Filter should not be created directly but instead use `try_new()`
/// and that these fields are only pub to support pattern matching
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
#[non_exhaustive]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The flag to indicate if the filter is a having clause
    pub having: bool,
}

impl Filter {
    /// Create a new filter operator.
    ///
    /// Notes: as Aliases have no effect on the output of a filter operator,
    /// they are removed from the predicate expression.
    pub fn try_new(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self> {
        Self::try_new_internal(predicate, input, false)
    }

    /// Create a new filter operator for a having clause.
    /// This is similar to a filter, but its having flag is set to true.
    pub fn try_new_with_having(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self> {
        Self::try_new_internal(predicate, input, true)
    }

    fn is_allowed_filter_type(data_type: &DataType) -> bool {
        match data_type {
            // Interpret NULL as a missing boolean value.
            DataType::Boolean | DataType::Null => true,
            DataType::Dictionary(_, value_type) => {
                Filter::is_allowed_filter_type(value_type.as_ref())
            }
            _ => false,
        }
    }

    fn try_new_internal(
        predicate: Expr,
        input: Arc<LogicalPlan>,
        having: bool,
    ) -> Result<Self> {
        // Filter predicates must return a boolean value so we try and validate that here.
        // Note that it is not always possible to resolve the predicate expression during plan
        // construction (such as with correlated subqueries) so we make a best effort here and
        // ignore errors resolving the expression against the schema.
        if let Ok(predicate_type) = predicate.get_type(input.schema()) {
            if !Filter::is_allowed_filter_type(&predicate_type) {
                return plan_err!(
                    "Cannot create filter with non-boolean predicate '{predicate}' returning {predicate_type}"
                );
            }
        }

        Ok(Self {
            predicate: predicate.unalias_nested().data,
            input,
            having,
        })
    }

    /// Is this filter guaranteed to return 0 or 1 row in a given instantiation?
    ///
    /// This function will return `true` if its predicate contains a conjunction of
    /// `col(a) = <expr>`, where its schema has a unique filter that is covered
    /// by this conjunction.
    ///
    /// For example, for the table:
    /// ```sql
    /// CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER);
    /// ```
    /// `Filter(a = 2).is_scalar() == true`
    /// , whereas
    /// `Filter(b = 2).is_scalar() == false`
    /// and
    /// `Filter(a = 2 OR b = 2).is_scalar() == false`
    fn is_scalar(&self) -> bool {
        let schema = self.input.schema();

        let functional_dependencies = self.input.schema().functional_dependencies();
        let unique_keys = functional_dependencies.iter().filter(|dep| {
            let nullable = dep.nullable
                && dep
                    .source_indices
                    .iter()
                    .any(|&source| schema.field(source).is_nullable());
            !nullable
                && dep.mode == Dependency::Single
                && dep.target_indices.len() == schema.fields().len()
        });

        let exprs = split_conjunction(&self.predicate);
        let eq_pred_cols: HashSet<_> = exprs
            .iter()
            .filter_map(|expr| {
                let Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) = expr
                else {
                    return None;
                };
                // This is a no-op filter expression
                if left == right {
                    return None;
                }

                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(_), Expr::Column(_)) => None,
                    (Expr::Column(c), _) | (_, Expr::Column(c)) => {
                        Some(schema.index_of_column(c).unwrap())
                    }
                    _ => None,
                }
            })
            .collect();

        // If we have a functional dependence that is a subset of our predicate,
        // this filter is scalar
        for key in unique_keys {
            if key.source_indices.iter().all(|c| eq_pred_cols.contains(c)) {
                return true;
            }
        }
        false
    }
}

/// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
///
/// # Output Schema
///
/// The output schema is the input schema followed by the window function
/// expressions, in order.
///
/// For example, given the input schema `"A", "B", "C"` and the window function
/// `SUM(A) OVER (PARTITION BY B+1 ORDER BY C)`, the output schema will be `"A",
/// "B", "C", "SUM(A) OVER ..."` where `"SUM(A) OVER ..."` is the name of the
/// output column.
///
/// Note that the `PARTITION BY` expression "B+1" is not produced in the output
/// schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Window {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The window function expression
    pub window_expr: Vec<Expr>,
    /// The schema description of the window output
    pub schema: DFSchemaRef,
}

impl Window {
    /// Create a new window operator.
    pub fn try_new(window_expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self> {
        let fields: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        let input_len = fields.len();
        let mut window_fields = fields;
        let expr_fields = exprlist_to_fields(window_expr.as_slice(), &input)?;
        window_fields.extend_from_slice(expr_fields.as_slice());
        let metadata = input.schema().metadata().clone();

        // Update functional dependencies for window:
        let mut window_func_dependencies =
            input.schema().functional_dependencies().clone();
        window_func_dependencies.extend_target_indices(window_fields.len());

        // Since we know that ROW_NUMBER outputs will be unique (i.e. it consists
        // of consecutive numbers per partition), we can represent this fact with
        // functional dependencies.
        let mut new_dependencies = window_expr
            .iter()
            .enumerate()
            .filter_map(|(idx, expr)| {
                if let Expr::WindowFunction(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(udwf),
                    params: WindowFunctionParams { partition_by, .. },
                }) = expr
                {
                    // When there is no PARTITION BY, row number will be unique
                    // across the entire table.
                    if udwf.name() == "row_number" && partition_by.is_empty() {
                        return Some(idx + input_len);
                    }
                }
                None
            })
            .map(|idx| {
                FunctionalDependence::new(vec![idx], vec![], false)
                    .with_mode(Dependency::Single)
            })
            .collect::<Vec<_>>();

        if !new_dependencies.is_empty() {
            for dependence in new_dependencies.iter_mut() {
                dependence.target_indices = (0..window_fields.len()).collect();
            }
            // Add the dependency introduced because of ROW_NUMBER window function to the functional dependency
            let new_deps = FunctionalDependencies::new(new_dependencies);
            window_func_dependencies.extend(new_deps);
        }

        Self::try_new_with_schema(
            window_expr,
            input,
            Arc::new(
                DFSchema::new_with_metadata(window_fields, metadata)?
                    .with_functional_dependencies(window_func_dependencies)?,
            ),
        )
    }

    pub fn try_new_with_schema(
        window_expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        if window_expr.len() != schema.fields().len() - input.schema().fields().len() {
            return plan_err!(
                "Window has mismatch between number of expressions ({}) and number of fields in schema ({})",
                window_expr.len(),
                schema.fields().len() - input.schema().fields().len()
            );
        }

        Ok(Window {
            input,
            window_expr,
            schema,
        })
    }
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Window {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => self.window_expr.partial_cmp(&other.window_expr),
            cmp => cmp,
        }
    }
}

/// Produces rows from a table provider by reference or from the context
#[derive(Clone)]
pub struct TableScan {
    /// The name of the table
    pub table_name: TableReference,
    /// The source of the table
    pub source: Arc<dyn TableSource>,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DFSchemaRef,
    /// Optional expressions to be used as filters by the table provider
    pub filters: Vec<Expr>,
    /// Optional number of rows to read
    pub fetch: Option<usize>,
}

impl Debug for TableScan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("TableScan")
            .field("table_name", &self.table_name)
            .field("source", &"...")
            .field("projection", &self.projection)
            .field("projected_schema", &self.projected_schema)
            .field("filters", &self.filters)
            .field("fetch", &self.fetch)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.projection == other.projection
            && self.projected_schema == other.projected_schema
            && self.filters == other.filters
            && self.fetch == other.fetch
    }
}

impl Eq for TableScan {}

// Manual implementation needed because of `source` and `projected_schema` fields.
// Comparison excludes these field.
impl PartialOrd for TableScan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableTableScan<'a> {
            /// The name of the table
            pub table_name: &'a TableReference,
            /// Optional column indices to use as a projection
            pub projection: &'a Option<Vec<usize>>,
            /// Optional expressions to be used as filters by the table provider
            pub filters: &'a Vec<Expr>,
            /// Optional number of rows to read
            pub fetch: &'a Option<usize>,
        }
        let comparable_self = ComparableTableScan {
            table_name: &self.table_name,
            projection: &self.projection,
            filters: &self.filters,
            fetch: &self.fetch,
        };
        let comparable_other = ComparableTableScan {
            table_name: &other.table_name,
            projection: &other.projection,
            filters: &other.filters,
            fetch: &other.fetch,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

impl Hash for TableScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.projection.hash(state);
        self.projected_schema.hash(state);
        self.filters.hash(state);
        self.fetch.hash(state);
    }
}

impl TableScan {
    /// Initialize TableScan with appropriate schema from the given
    /// arguments.
    pub fn try_new(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.table().is_empty() {
            return plan_err!("table_name cannot be empty");
        }
        let schema = table_source.schema();
        let func_dependencies = FunctionalDependencies::new_from_constraints(
            table_source.constraints(),
            schema.fields.len(),
        );
        let projected_schema = projection
            .as_ref()
            .map(|p| {
                let projected_func_dependencies =
                    func_dependencies.project_functional_dependencies(p, p.len());

                let df_schema = DFSchema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            (Some(table_name.clone()), Arc::new(schema.field(*i).clone()))
                        })
                        .collect(),
                    schema.metadata.clone(),
                )?;
                df_schema.with_functional_dependencies(projected_func_dependencies)
            })
            .unwrap_or_else(|| {
                let df_schema =
                    DFSchema::try_from_qualified_schema(table_name.clone(), &schema)?;
                df_schema.with_functional_dependencies(func_dependencies)
            })?;
        let projected_schema = Arc::new(projected_schema);

        Ok(Self {
            table_name,
            source: table_source,
            projection,
            projected_schema,
            filters,
            fetch,
        })
    }
}

// Repartition the plan based on a partitioning scheme.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Repartition {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The partitioning scheme
    pub partitioning_scheme: Partitioning,
}

/// Union multiple inputs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Union {
    /// Inputs to merge
    pub inputs: Vec<Arc<LogicalPlan>>,
    /// Union schema. Should be the same for all inputs.
    pub schema: DFSchemaRef,
}

impl Union {
    /// Constructs new Union instance deriving schema from inputs.
    fn try_new(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self> {
        let schema = Self::derive_schema_from_inputs(&inputs, false, false)?;
        Ok(Union { inputs, schema })
    }

    /// Constructs new Union instance deriving schema from inputs.
    /// Inputs do not have to have matching types and produced schema will
    /// take type from the first input.
    // TODO (https://github.com/apache/datafusion/issues/14380): Avoid creating uncoerced union at all.
    pub fn try_new_with_loose_types(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self> {
        let schema = Self::derive_schema_from_inputs(&inputs, true, false)?;
        Ok(Union { inputs, schema })
    }

    /// Constructs a new Union instance that combines rows from different tables by name,
    /// instead of by position. This means that the specified inputs need not have schemas
    /// that are all the same width.
    pub fn try_new_by_name(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self> {
        let schema = Self::derive_schema_from_inputs(&inputs, true, true)?;
        let inputs = Self::rewrite_inputs_from_schema(&schema, inputs)?;

        Ok(Union { inputs, schema })
    }

    /// When constructing a `UNION BY NAME`, we need to wrap inputs
    /// in an additional `Projection` to account for absence of columns
    /// in input schemas or differing projection orders.
    fn rewrite_inputs_from_schema(
        schema: &Arc<DFSchema>,
        inputs: Vec<Arc<LogicalPlan>>,
    ) -> Result<Vec<Arc<LogicalPlan>>> {
        let schema_width = schema.iter().count();
        let mut wrapped_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            // Any columns that exist within the derived schema but do not exist
            // within an input's schema should be replaced with `NULL` aliased
            // to the appropriate column in the derived schema.
            let mut expr = Vec::with_capacity(schema_width);
            for column in schema.columns() {
                if input
                    .schema()
                    .has_column_with_unqualified_name(column.name())
                {
                    expr.push(Expr::Column(column));
                } else {
                    expr.push(Expr::Literal(ScalarValue::Null).alias(column.name()));
                }
            }
            wrapped_inputs.push(Arc::new(LogicalPlan::Projection(
                Projection::try_new_with_schema(expr, input, Arc::clone(schema))?,
            )));
        }

        Ok(wrapped_inputs)
    }

    /// Constructs new Union instance deriving schema from inputs.
    ///
    /// If `loose_types` is true, inputs do not need to have matching types and
    /// the produced schema will use the type from the first input.
    /// TODO (<https://github.com/apache/datafusion/issues/14380>): This is not necessarily reasonable behavior.
    ///
    /// If `by_name` is `true`, input schemas need not be the same width. That is,
    /// the constructed schema follows `UNION BY NAME` semantics.
    fn derive_schema_from_inputs(
        inputs: &[Arc<LogicalPlan>],
        loose_types: bool,
        by_name: bool,
    ) -> Result<DFSchemaRef> {
        if inputs.len() < 2 {
            return plan_err!("UNION requires at least two inputs");
        }

        if by_name {
            Self::derive_schema_from_inputs_by_name(inputs, loose_types)
        } else {
            Self::derive_schema_from_inputs_by_position(inputs, loose_types)
        }
    }

    fn derive_schema_from_inputs_by_name(
        inputs: &[Arc<LogicalPlan>],
        loose_types: bool,
    ) -> Result<DFSchemaRef> {
        type FieldData<'a> =
            (&'a DataType, bool, Vec<&'a HashMap<String, String>>, usize);
        let mut cols: Vec<(&str, FieldData)> = Vec::new();
        for input in inputs.iter() {
            for field in input.schema().fields() {
                if let Some((_, (data_type, is_nullable, metadata, occurrences))) =
                    cols.iter_mut().find(|(name, _)| name == field.name())
                {
                    if !loose_types && *data_type != field.data_type() {
                        return plan_err!(
                            "Found different types for field {}",
                            field.name()
                        );
                    }

                    metadata.push(field.metadata());
                    // If the field is nullable in any one of the inputs,
                    // then the field in the final schema is also nullable.
                    *is_nullable |= field.is_nullable();
                    *occurrences += 1;
                } else {
                    cols.push((
                        field.name(),
                        (
                            field.data_type(),
                            field.is_nullable(),
                            vec![field.metadata()],
                            1,
                        ),
                    ));
                }
            }
        }

        let union_fields = cols
            .into_iter()
            .map(
                |(name, (data_type, is_nullable, unmerged_metadata, occurrences))| {
                    // If the final number of occurrences of the field is less
                    // than the number of inputs (i.e. the field is missing from
                    // one or more inputs), then it must be treated as nullable.
                    let final_is_nullable = if occurrences == inputs.len() {
                        is_nullable
                    } else {
                        true
                    };

                    let mut field =
                        Field::new(name, data_type.clone(), final_is_nullable);
                    field.set_metadata(intersect_maps(unmerged_metadata));

                    (None, Arc::new(field))
                },
            )
            .collect::<Vec<(Option<TableReference>, _)>>();

        let union_schema_metadata =
            intersect_maps(inputs.iter().map(|input| input.schema().metadata()));

        // Functional Dependencies are not preserved after UNION operation
        let schema = DFSchema::new_with_metadata(union_fields, union_schema_metadata)?;
        let schema = Arc::new(schema);

        Ok(schema)
    }

    fn derive_schema_from_inputs_by_position(
        inputs: &[Arc<LogicalPlan>],
        loose_types: bool,
    ) -> Result<DFSchemaRef> {
        let first_schema = inputs[0].schema();
        let fields_count = first_schema.fields().len();
        for input in inputs.iter().skip(1) {
            if fields_count != input.schema().fields().len() {
                return plan_err!(
                    "UNION queries have different number of columns: \
                    left has {} columns whereas right has {} columns",
                    fields_count,
                    input.schema().fields().len()
                );
            }
        }

        let mut name_counts: HashMap<String, usize> = HashMap::new();
        let union_fields = (0..fields_count)
            .map(|i| {
                let fields = inputs
                    .iter()
                    .map(|input| input.schema().field(i))
                    .collect::<Vec<_>>();
                let first_field = fields[0];
                let base_name = first_field.name().to_string();

                let data_type = if loose_types {
                    // TODO apply type coercion here, or document why it's better to defer
                    // temporarily use the data type from the left input and later rely on the analyzer to
                    // coerce the two schemas into a common one.
                    first_field.data_type()
                } else {
                    fields.iter().skip(1).try_fold(
                        first_field.data_type(),
                        |acc, field| {
                            if acc != field.data_type() {
                                return plan_err!(
                                    "UNION field {i} have different type in inputs: \
                                    left has {} whereas right has {}",
                                    first_field.data_type(),
                                    field.data_type()
                                );
                            }
                            Ok(acc)
                        },
                    )?
                };
                let nullable = fields.iter().any(|field| field.is_nullable());

                // Generate unique field name
                let name = if let Some(count) = name_counts.get_mut(&base_name) {
                    *count += 1;
                    format!("{}_{}", base_name, count)
                } else {
                    name_counts.insert(base_name.clone(), 0);
                    base_name
                };

                let mut field = Field::new(&name, data_type.clone(), nullable);
                let field_metadata =
                    intersect_maps(fields.iter().map(|field| field.metadata()));
                field.set_metadata(field_metadata);
                Ok((None, Arc::new(field)))
            })
            .collect::<Result<_>>()?;
        let union_schema_metadata =
            intersect_maps(inputs.iter().map(|input| input.schema().metadata()));

        // Functional Dependencies are not preserved after UNION operation
        let schema = DFSchema::new_with_metadata(union_fields, union_schema_metadata)?;
        let schema = Arc::new(schema);

        Ok(schema)
    }
}

fn intersect_maps<'a>(
    inputs: impl IntoIterator<Item = &'a HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut inputs = inputs.into_iter();
    let mut merged: HashMap<String, String> = inputs.next().cloned().unwrap_or_default();
    for input in inputs {
        // The extra dereference below (`&*v`) is a workaround for https://github.com/rkyv/rkyv/issues/434.
        // When this crate is used in a workspace that enables the `rkyv-64` feature in the `chrono` crate,
        // this triggers a Rust compilation error:
        // error[E0277]: can't compare `Option<&std::string::String>` with `Option<&mut std::string::String>`.
        merged.retain(|k, v| input.get(k) == Some(&*v));
    }
    merged
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Union {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inputs.partial_cmp(&other.inputs)
    }
}

/// Describe the schema of table
///
/// # Example output:
///
/// ```sql
/// > describe traces;
/// +--------------------+-----------------------------+-------------+
/// | column_name        | data_type                   | is_nullable |
/// +--------------------+-----------------------------+-------------+
/// | attributes         | Utf8                        | YES         |
/// | duration_nano      | Int64                       | YES         |
/// | end_time_unix_nano | Int64                       | YES         |
/// | service.name       | Dictionary(Int32, Utf8)     | YES         |
/// | span.kind          | Utf8                        | YES         |
/// | span.name          | Utf8                        | YES         |
/// | span_id            | Dictionary(Int32, Utf8)     | YES         |
/// | time               | Timestamp(Nanosecond, None) | NO          |
/// | trace_id           | Dictionary(Int32, Utf8)     | YES         |
/// | otel.status_code   | Utf8                        | YES         |
/// | parent_span_id     | Utf8                        | YES         |
/// +--------------------+-----------------------------+-------------+
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DescribeTable {
    /// Table schema
    pub schema: Arc<Schema>,
    /// schema of describe table output
    pub output_schema: DFSchemaRef,
}

// Manual implementation of `PartialOrd`, returning none since there are no comparable types in
// `DescribeTable`. This allows `LogicalPlan` to derive `PartialOrd`.
impl PartialOrd for DescribeTable {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        // There is no relevant comparison for schemas
        None
    }
}

/// Output formats for controlling for Explain plans
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    /// Indent mode
    ///
    /// Example:
    /// ```text
    /// > explain format indent select x from values (1) t(x);
    /// +---------------+-----------------------------------------------------+
    /// | plan_type     | plan                                                |
    /// +---------------+-----------------------------------------------------+
    /// | logical_plan  | SubqueryAlias: t                                    |
    /// |               |   Projection: column1 AS x                          |
    /// |               |     Values: (Int64(1))                              |
    /// | physical_plan | ProjectionExec: expr=[column1@0 as x]               |
    /// |               |   DataSourceExec: partitions=1, partition_sizes=[1] |
    /// |               |                                                     |
    /// +---------------+-----------------------------------------------------+
    /// ```
    Indent,
    /// Tree mode
    ///
    /// Example:
    /// ```text
    /// > explain format tree select x from values (1) t(x);
    /// +---------------+-------------------------------+
    /// | plan_type     | plan                          |
    /// +---------------+-------------------------------+
    /// | physical_plan | ┌───────────────────────────┐ |
    /// |               | │       ProjectionExec      │ |
    /// |               | │    --------------------   │ |
    /// |               | │        x: column1@0       │ |
    /// |               | └─────────────┬─────────────┘ |
    /// |               | ┌─────────────┴─────────────┐ |
    /// |               | │       DataSourceExec      │ |
    /// |               | │    --------------------   │ |
    /// |               | │         bytes: 128        │ |
    /// |               | │       format: memory      │ |
    /// |               | │          rows: 1          │ |
    /// |               | └───────────────────────────┘ |
    /// |               |                               |
    /// +---------------+-------------------------------+
    /// ```
    Tree,
    /// Postgres Json mode
    ///
    /// A displayable structure that produces plan in postgresql JSON format.
    ///
    /// Users can use this format to visualize the plan in existing plan
    /// visualization tools, for example [dalibo](https://explain.dalibo.com/)
    ///
    /// Example:
    /// ```text
    /// > explain format pgjson select x from values (1) t(x);
    /// +--------------+--------------------------------------+
    /// | plan_type    | plan                                 |
    /// +--------------+--------------------------------------+
    /// | logical_plan | [                                    |
    /// |              |   {                                  |
    /// |              |     "Plan": {                        |
    /// |              |       "Alias": "t",                  |
    /// |              |       "Node Type": "Subquery",       |
    /// |              |       "Output": [                    |
    /// |              |         "x"                          |
    /// |              |       ],                             |
    /// |              |       "Plans": [                     |
    /// |              |         {                            |
    /// |              |           "Expressions": [           |
    /// |              |             "column1 AS x"           |
    /// |              |           ],                         |
    /// |              |           "Node Type": "Projection", |
    /// |              |           "Output": [                |
    /// |              |             "x"                      |
    /// |              |           ],                         |
    /// |              |           "Plans": [                 |
    /// |              |             {                        |
    /// |              |               "Node Type": "Values", |
    /// |              |               "Output": [            |
    /// |              |                 "column1"            |
    /// |              |               ],                     |
    /// |              |               "Plans": [],           |
    /// |              |               "Values": "(Int64(1))" |
    /// |              |             }                        |
    /// |              |           ]                          |
    /// |              |         }                            |
    /// |              |       ]                              |
    /// |              |     }                                |
    /// |              |   }                                  |
    /// |              | ]                                    |
    /// +--------------+--------------------------------------+
    /// ```
    PostgresJSON,
    /// Graphviz mode
    ///
    /// Example:
    /// ```text
    /// > explain format graphviz select x from values (1) t(x);
    /// +--------------+------------------------------------------------------------------------+
    /// | plan_type    | plan                                                                   |
    /// +--------------+------------------------------------------------------------------------+
    /// | logical_plan |                                                                        |
    /// |              | // Begin DataFusion GraphViz Plan,                                     |
    /// |              | // display it online here: https://dreampuf.github.io/GraphvizOnline   |
    /// |              |                                                                        |
    /// |              | digraph {                                                              |
    /// |              |   subgraph cluster_1                                                   |
    /// |              |   {                                                                    |
    /// |              |     graph[label="LogicalPlan"]                                         |
    /// |              |     2[shape=box label="SubqueryAlias: t"]                              |
    /// |              |     3[shape=box label="Projection: column1 AS x"]                      |
    /// |              |     2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |     4[shape=box label="Values: (Int64(1))"]                            |
    /// |              |     3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |   }                                                                    |
    /// |              |   subgraph cluster_5                                                   |
    /// |              |   {                                                                    |
    /// |              |     graph[label="Detailed LogicalPlan"]                                |
    /// |              |     6[shape=box label="SubqueryAlias: t\nSchema: [x:Int64;N]"]         |
    /// |              |     7[shape=box label="Projection: column1 AS x\nSchema: [x:Int64;N]"] |
    /// |              |     6 -> 7 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |     8[shape=box label="Values: (Int64(1))\nSchema: [column1:Int64;N]"] |
    /// |              |     7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |   }                                                                    |
    /// |              | }                                                                      |
    /// |              | // End DataFusion GraphViz Plan                                        |
    /// |              |                                                                        |
    /// +--------------+------------------------------------------------------------------------+
    /// ```
    Graphviz,
}

/// Implement  parsing strings to `ExplainFormat`
impl FromStr for ExplainFormat {
    type Err = DataFusionError;

    fn from_str(format: &str) -> std::result::Result<Self, Self::Err> {
        match format.to_lowercase().as_str() {
            "indent" => Ok(ExplainFormat::Indent),
            "tree" => Ok(ExplainFormat::Tree),
            "pgjson" => Ok(ExplainFormat::PostgresJSON),
            "graphviz" => Ok(ExplainFormat::Graphviz),
            _ => {
                plan_err!("Invalid explain format. Expected 'indent', 'tree', 'pgjson' or 'graphviz'. Got '{format}'")
            }
        }
    }
}

/// Produces a relation with string representations of
/// various parts of the plan
///
/// See [the documentation] for more information
///
/// [the documentation]: https://datafusion.apache.org/user-guide/sql/explain.html
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Explain {
    /// Should extra (detailed, intermediate plans) be included?
    pub verbose: bool,
    /// Output format for explain, if specified.
    /// If none, defaults to `text`
    pub explain_format: ExplainFormat,
    /// The logical plan that is being EXPLAIN'd
    pub plan: Arc<LogicalPlan>,
    /// Represent the various stages plans have gone through
    pub stringified_plans: Vec<StringifiedPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
    /// Used by physical planner to check if should proceed with planning
    pub logical_optimization_succeeded: bool,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Explain {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableExplain<'a> {
            /// Should extra (detailed, intermediate plans) be included?
            pub verbose: &'a bool,
            /// The logical plan that is being EXPLAIN'd
            pub plan: &'a Arc<LogicalPlan>,
            /// Represent the various stages plans have gone through
            pub stringified_plans: &'a Vec<StringifiedPlan>,
            /// Used by physical planner to check if should proceed with planning
            pub logical_optimization_succeeded: &'a bool,
        }
        let comparable_self = ComparableExplain {
            verbose: &self.verbose,
            plan: &self.plan,
            stringified_plans: &self.stringified_plans,
            logical_optimization_succeeded: &self.logical_optimization_succeeded,
        };
        let comparable_other = ComparableExplain {
            verbose: &other.verbose,
            plan: &other.plan,
            stringified_plans: &other.stringified_plans,
            logical_optimization_succeeded: &other.logical_optimization_succeeded,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

/// Runs the actual plan, and then prints the physical plan with
/// with execution metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Analyze {
    /// Should extra detail be included?
    pub verbose: bool,
    /// The logical plan that is being EXPLAIN ANALYZE'd
    pub input: Arc<LogicalPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Analyze {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.verbose.partial_cmp(&other.verbose) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            cmp => cmp,
        }
    }
}

/// Extension operator defined outside of DataFusion
// TODO(clippy): This clippy `allow` should be removed if
// the manual `PartialEq` is removed in favor of a derive.
// (see `PartialEq` the impl for details.)
#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Clone, Eq, Hash)]
pub struct Extension {
    /// The runtime extension operator
    pub node: Arc<dyn UserDefinedLogicalNode>,
}

// `PartialEq` cannot be derived for types containing `Arc<dyn Trait>`.
// This manual implementation should be removed if
// https://github.com/rust-lang/rust/issues/39128 is fixed.
impl PartialEq for Extension {
    fn eq(&self, other: &Self) -> bool {
        self.node.eq(&other.node)
    }
}

impl PartialOrd for Extension {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.node.partial_cmp(&other.node)
    }
}

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Limit {
    /// Number of rows to skip before fetch
    pub skip: Option<Box<Expr>>,
    /// Maximum number of rows to fetch,
    /// None means fetching all rows
    pub fetch: Option<Box<Expr>>,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

/// Different types of skip expression in Limit plan.
pub enum SkipType {
    /// The skip expression is a literal value.
    Literal(usize),
    /// Currently only supports expressions that can be folded into constants.
    UnsupportedExpr,
}

/// Different types of fetch expression in Limit plan.
pub enum FetchType {
    /// The fetch expression is a literal value.
    /// `Literal(None)` means the fetch expression is not provided.
    Literal(Option<usize>),
    /// Currently only supports expressions that can be folded into constants.
    UnsupportedExpr,
}

impl Limit {
    /// Get the skip type from the limit plan.
    pub fn get_skip_type(&self) -> Result<SkipType> {
        match self.skip.as_deref() {
            Some(expr) => match *expr {
                Expr::Literal(ScalarValue::Int64(s)) => {
                    // `skip = NULL` is equivalent to `skip = 0`
                    let s = s.unwrap_or(0);
                    if s >= 0 {
                        Ok(SkipType::Literal(s as usize))
                    } else {
                        plan_err!("OFFSET must be >=0, '{}' was provided", s)
                    }
                }
                _ => Ok(SkipType::UnsupportedExpr),
            },
            // `skip = None` is equivalent to `skip = 0`
            None => Ok(SkipType::Literal(0)),
        }
    }

    /// Get the fetch type from the limit plan.
    pub fn get_fetch_type(&self) -> Result<FetchType> {
        match self.fetch.as_deref() {
            Some(expr) => match *expr {
                Expr::Literal(ScalarValue::Int64(Some(s))) => {
                    if s >= 0 {
                        Ok(FetchType::Literal(Some(s as usize)))
                    } else {
                        plan_err!("LIMIT must be >= 0, '{}' was provided", s)
                    }
                }
                Expr::Literal(ScalarValue::Int64(None)) => Ok(FetchType::Literal(None)),
                _ => Ok(FetchType::UnsupportedExpr),
            },
            None => Ok(FetchType::Literal(None)),
        }
    }
}

/// Removes duplicate rows from the input
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Distinct {
    /// Plain `DISTINCT` referencing all selection expressions
    All(Arc<LogicalPlan>),
    /// The `Postgres` addition, allowing separate control over DISTINCT'd and selected columns
    On(DistinctOn),
}

impl Distinct {
    /// return a reference to the nodes input
    pub fn input(&self) -> &Arc<LogicalPlan> {
        match self {
            Distinct::All(input) => input,
            Distinct::On(DistinctOn { input, .. }) => input,
        }
    }
}

/// Removes duplicate rows from the input
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DistinctOn {
    /// The `DISTINCT ON` clause expression list
    pub on_expr: Vec<Expr>,
    /// The selected projection expression list
    pub select_expr: Vec<Expr>,
    /// The `ORDER BY` clause, whose initial expressions must match those of the `ON` clause when
    /// present. Note that those matching expressions actually wrap the `ON` expressions with
    /// additional info pertaining to the sorting procedure (i.e. ASC/DESC, and NULLS FIRST/LAST).
    pub sort_expr: Option<Vec<SortExpr>>,
    /// The logical plan that is being DISTINCT'd
    pub input: Arc<LogicalPlan>,
    /// The schema description of the DISTINCT ON output
    pub schema: DFSchemaRef,
}

impl DistinctOn {
    /// Create a new `DistinctOn` struct.
    pub fn try_new(
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<SortExpr>>,
        input: Arc<LogicalPlan>,
    ) -> Result<Self> {
        if on_expr.is_empty() {
            return plan_err!("No `ON` expressions provided");
        }

        let on_expr = normalize_cols(on_expr, input.as_ref())?;
        let qualified_fields = exprlist_to_fields(select_expr.as_slice(), &input)?
            .into_iter()
            .collect();

        let dfschema = DFSchema::new_with_metadata(
            qualified_fields,
            input.schema().metadata().clone(),
        )?;

        let mut distinct_on = DistinctOn {
            on_expr,
            select_expr,
            sort_expr: None,
            input,
            schema: Arc::new(dfschema),
        };

        if let Some(sort_expr) = sort_expr {
            distinct_on = distinct_on.with_sort_expr(sort_expr)?;
        }

        Ok(distinct_on)
    }

    /// Try to update `self` with a new sort expressions.
    ///
    /// Validates that the sort expressions are a super-set of the `ON` expressions.
    pub fn with_sort_expr(mut self, sort_expr: Vec<SortExpr>) -> Result<Self> {
        let sort_expr = normalize_sorts(sort_expr, self.input.as_ref())?;

        // Check that the left-most sort expressions are the same as the `ON` expressions.
        let mut matched = true;
        for (on, sort) in self.on_expr.iter().zip(sort_expr.iter()) {
            if on != &sort.expr {
                matched = false;
                break;
            }
        }

        if self.on_expr.len() > sort_expr.len() || !matched {
            return plan_err!(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
            );
        }

        self.sort_expr = Some(sort_expr);
        Ok(self)
    }
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DistinctOn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableDistinctOn<'a> {
            /// The `DISTINCT ON` clause expression list
            pub on_expr: &'a Vec<Expr>,
            /// The selected projection expression list
            pub select_expr: &'a Vec<Expr>,
            /// The `ORDER BY` clause, whose initial expressions must match those of the `ON` clause when
            /// present. Note that those matching expressions actually wrap the `ON` expressions with
            /// additional info pertaining to the sorting procedure (i.e. ASC/DESC, and NULLS FIRST/LAST).
            pub sort_expr: &'a Option<Vec<SortExpr>>,
            /// The logical plan that is being DISTINCT'd
            pub input: &'a Arc<LogicalPlan>,
        }
        let comparable_self = ComparableDistinctOn {
            on_expr: &self.on_expr,
            select_expr: &self.select_expr,
            sort_expr: &self.sort_expr,
            input: &self.input,
        };
        let comparable_other = ComparableDistinctOn {
            on_expr: &other.on_expr,
            select_expr: &other.select_expr,
            sort_expr: &other.sort_expr,
            input: &other.input,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
///
/// # Output Schema
///
/// The output schema is the group expressions followed by the aggregate
/// expressions in order.
///
/// For example, given the input schema `"A", "B", "C"` and the aggregate
/// `SUM(A) GROUP BY C+B`, the output schema will be `"C+B", "SUM(A)"` where
/// "C+B" and "SUM(A)" are the names of the output columns. Note that "C+B" is a
/// single new column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: DFSchemaRef,
}

impl Aggregate {
    /// Create a new aggregate operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        let group_expr = enumerate_grouping_sets(group_expr)?;

        let is_grouping_set = matches!(group_expr.as_slice(), [Expr::GroupingSet(_)]);

        let grouping_expr: Vec<&Expr> = grouping_set_to_exprlist(group_expr.as_slice())?;

        let mut qualified_fields = exprlist_to_fields(grouping_expr, &input)?;

        // Even columns that cannot be null will become nullable when used in a grouping set.
        if is_grouping_set {
            qualified_fields = qualified_fields
                .into_iter()
                .map(|(q, f)| (q, f.as_ref().clone().with_nullable(true).into()))
                .collect::<Vec<_>>();
            qualified_fields.push((
                None,
                Field::new(
                    Self::INTERNAL_GROUPING_ID,
                    Self::grouping_id_type(qualified_fields.len()),
                    false,
                )
                .into(),
            ));
        }

        qualified_fields.extend(exprlist_to_fields(aggr_expr.as_slice(), &input)?);

        let schema = DFSchema::new_with_metadata(
            qualified_fields,
            input.schema().metadata().clone(),
        )?;

        Self::try_new_with_schema(input, group_expr, aggr_expr, Arc::new(schema))
    }

    /// Create a new aggregate operator using the provided schema to avoid the overhead of
    /// building the schema again when the schema is already known.
    ///
    /// This method should only be called when you are absolutely sure that the schema being
    /// provided is correct for the aggregate. If in doubt, call [try_new](Self::try_new) instead.
    pub fn try_new_with_schema(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        if group_expr.is_empty() && aggr_expr.is_empty() {
            return plan_err!(
                "Aggregate requires at least one grouping or aggregate expression"
            );
        }
        let group_expr_count = grouping_set_expr_count(&group_expr)?;
        if schema.fields().len() != group_expr_count + aggr_expr.len() {
            return plan_err!(
                "Aggregate schema has wrong number of fields. Expected {} got {}",
                group_expr_count + aggr_expr.len(),
                schema.fields().len()
            );
        }

        let aggregate_func_dependencies =
            calc_func_dependencies_for_aggregate(&group_expr, &input, &schema)?;
        let new_schema = schema.as_ref().clone();
        let schema = Arc::new(
            new_schema.with_functional_dependencies(aggregate_func_dependencies)?,
        );
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }

    fn is_grouping_set(&self) -> bool {
        matches!(self.group_expr.as_slice(), [Expr::GroupingSet(_)])
    }

    /// Get the output expressions.
    fn output_expressions(&self) -> Result<Vec<&Expr>> {
        static INTERNAL_ID_EXPR: LazyLock<Expr> = LazyLock::new(|| {
            Expr::Column(Column::from_name(Aggregate::INTERNAL_GROUPING_ID))
        });
        let mut exprs = grouping_set_to_exprlist(self.group_expr.as_slice())?;
        if self.is_grouping_set() {
            exprs.push(&INTERNAL_ID_EXPR);
        }
        exprs.extend(self.aggr_expr.iter());
        debug_assert!(exprs.len() == self.schema.fields().len());
        Ok(exprs)
    }

    /// Get the length of the group by expression in the output schema
    /// This is not simply group by expression length. Expression may be
    /// GroupingSet, etc. In these case we need to get inner expression lengths.
    pub fn group_expr_len(&self) -> Result<usize> {
        grouping_set_expr_count(&self.group_expr)
    }

    /// Returns the data type of the grouping id.
    /// The grouping ID value is a bitmask where each set bit
    /// indicates that the corresponding grouping expression is
    /// null
    pub fn grouping_id_type(group_exprs: usize) -> DataType {
        if group_exprs <= 8 {
            DataType::UInt8
        } else if group_exprs <= 16 {
            DataType::UInt16
        } else if group_exprs <= 32 {
            DataType::UInt32
        } else {
            DataType::UInt64
        }
    }

    /// Internal column used when the aggregation is a grouping set.
    ///
    /// This column contains a bitmask where each bit represents a grouping
    /// expression. The least significant bit corresponds to the rightmost
    /// grouping expression. A bit value of 0 indicates that the corresponding
    /// column is included in the grouping set, while a value of 1 means it is excluded.
    ///
    /// For example, for the grouping expressions CUBE(a, b), the grouping ID
    /// column will have the following values:
    ///     0b00: Both `a` and `b` are included
    ///     0b01: `b` is excluded
    ///     0b10: `a` is excluded
    ///     0b11: Both `a` and `b` are excluded
    ///
    /// This internal column is necessary because excluded columns are replaced
    /// with `NULL` values. To handle these cases correctly, we must distinguish
    /// between an actual `NULL` value in a column and a column being excluded from the set.
    pub const INTERNAL_GROUPING_ID: &'static str = "__grouping_id";
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Aggregate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {
                match self.group_expr.partial_cmp(&other.group_expr) {
                    Some(Ordering::Equal) => self.aggr_expr.partial_cmp(&other.aggr_expr),
                    cmp => cmp,
                }
            }
            cmp => cmp,
        }
    }
}

/// Checks whether any expression in `group_expr` contains `Expr::GroupingSet`.
fn contains_grouping_set(group_expr: &[Expr]) -> bool {
    group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
}

/// Calculates functional dependencies for aggregate expressions.
fn calc_func_dependencies_for_aggregate(
    // Expressions in the GROUP BY clause:
    group_expr: &[Expr],
    // Input plan of the aggregate:
    input: &LogicalPlan,
    // Aggregate schema
    aggr_schema: &DFSchema,
) -> Result<FunctionalDependencies> {
    // We can do a case analysis on how to propagate functional dependencies based on
    // whether the GROUP BY in question contains a grouping set expression:
    // - If so, the functional dependencies will be empty because we cannot guarantee
    //   that GROUP BY expression results will be unique.
    // - Otherwise, it may be possible to propagate functional dependencies.
    if !contains_grouping_set(group_expr) {
        let group_by_expr_names = group_expr
            .iter()
            .map(|item| item.schema_name().to_string())
            .collect::<IndexSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let aggregate_func_dependencies = aggregate_functional_dependencies(
            input.schema(),
            &group_by_expr_names,
            aggr_schema,
        );
        Ok(aggregate_func_dependencies)
    } else {
        Ok(FunctionalDependencies::empty())
    }
}

/// This function projects functional dependencies of the `input` plan according
/// to projection expressions `exprs`.
fn calc_func_dependencies_for_project(
    exprs: &[Expr],
    input: &LogicalPlan,
) -> Result<FunctionalDependencies> {
    let input_fields = input.schema().field_names();
    // Calculate expression indices (if present) in the input schema.
    let proj_indices = exprs
        .iter()
        .map(|expr| match expr {
            #[expect(deprecated)]
            Expr::Wildcard { qualifier, options } => {
                let wildcard_fields = exprlist_to_fields(
                    vec![&Expr::Wildcard {
                        qualifier: qualifier.clone(),
                        options: options.clone(),
                    }],
                    input,
                )?;
                Ok::<_, DataFusionError>(
                    wildcard_fields
                        .into_iter()
                        .filter_map(|(qualifier, f)| {
                            let flat_name = qualifier
                                .map(|t| format!("{}.{}", t, f.name()))
                                .unwrap_or_else(|| f.name().clone());
                            input_fields.iter().position(|item| *item == flat_name)
                        })
                        .collect::<Vec<_>>(),
                )
            }
            Expr::Alias(alias) => {
                let name = format!("{}", alias.expr);
                Ok(input_fields
                    .iter()
                    .position(|item| *item == name)
                    .map(|i| vec![i])
                    .unwrap_or(vec![]))
            }
            _ => {
                let name = format!("{}", expr);
                Ok(input_fields
                    .iter()
                    .position(|item| *item == name)
                    .map(|i| vec![i])
                    .unwrap_or(vec![]))
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(input
        .schema()
        .functional_dependencies()
        .project_functional_dependencies(&proj_indices, exprs.len()))
}

/// Sorts its input according to a list of sort expressions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<SortExpr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Optional fetch limit
    pub fetch: Option<usize>,
}

/// Join two logical plans on one or more join columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join expressions
    pub on: Vec<(Expr, Expr)>,
    /// Filters applied during join (non-equi conditions)
    pub filter: Option<Expr>,
    /// Join type
    pub join_type: JoinType,
    /// Join constraint
    pub join_constraint: JoinConstraint,
    /// The output schema, containing fields from the left and right inputs
    pub schema: DFSchemaRef,
    /// If null_equals_null is true, null == null else null != null
    pub null_equals_null: bool,
}

impl Join {
    /// Create Join with input which wrapped with projection, this method is used to help create physical join.
    pub fn try_new_with_project_input(
        original: &LogicalPlan,
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        column_on: (Vec<Column>, Vec<Column>),
    ) -> Result<Self> {
        let original_join = match original {
            LogicalPlan::Join(join) => join,
            _ => return plan_err!("Could not create join with project input"),
        };

        let on: Vec<(Expr, Expr)> = column_on
            .0
            .into_iter()
            .zip(column_on.1)
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();
        let join_schema =
            build_join_schema(left.schema(), right.schema(), &original_join.join_type)?;

        Ok(Join {
            left,
            right,
            on,
            filter: original_join.filter.clone(),
            join_type: original_join.join_type,
            join_constraint: original_join.join_constraint,
            schema: Arc::new(join_schema),
            null_equals_null: original_join.null_equals_null,
        })
    }
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Join {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableJoin<'a> {
            /// Left input
            pub left: &'a Arc<LogicalPlan>,
            /// Right input
            pub right: &'a Arc<LogicalPlan>,
            /// Equijoin clause expressed as pairs of (left, right) join expressions
            pub on: &'a Vec<(Expr, Expr)>,
            /// Filters applied during join (non-equi conditions)
            pub filter: &'a Option<Expr>,
            /// Join type
            pub join_type: &'a JoinType,
            /// Join constraint
            pub join_constraint: &'a JoinConstraint,
            /// If null_equals_null is true, null == null else null != null
            pub null_equals_null: &'a bool,
        }
        let comparable_self = ComparableJoin {
            left: &self.left,
            right: &self.right,
            on: &self.on,
            filter: &self.filter,
            join_type: &self.join_type,
            join_constraint: &self.join_constraint,
            null_equals_null: &self.null_equals_null,
        };
        let comparable_other = ComparableJoin {
            left: &other.left,
            right: &other.right,
            on: &other.on,
            filter: &other.filter,
            join_type: &other.join_type,
            join_constraint: &other.join_constraint,
            null_equals_null: &other.null_equals_null,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

/// Subquery
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Subquery {
    /// The subquery
    pub subquery: Arc<LogicalPlan>,
    /// The outer references used in the subquery
    pub outer_ref_columns: Vec<Expr>,
    /// Span information for subquery projection columns
    pub spans: Spans,
}

impl Normalizeable for Subquery {
    fn can_normalize(&self) -> bool {
        false
    }
}

impl NormalizeEq for Subquery {
    fn normalize_eq(&self, other: &Self) -> bool {
        // TODO: may be implement NormalizeEq for LogicalPlan?
        *self.subquery == *other.subquery
            && self.outer_ref_columns.len() == other.outer_ref_columns.len()
            && self
                .outer_ref_columns
                .iter()
                .zip(other.outer_ref_columns.iter())
                .all(|(a, b)| a.normalize_eq(b))
    }
}

impl Subquery {
    pub fn try_from_expr(plan: &Expr) -> Result<&Subquery> {
        match plan {
            Expr::ScalarSubquery(it) => Ok(it),
            Expr::Cast(cast) => Subquery::try_from_expr(cast.expr.as_ref()),
            _ => plan_err!("Could not coerce into ScalarSubquery!"),
        }
    }

    pub fn with_plan(&self, plan: Arc<LogicalPlan>) -> Subquery {
        Subquery {
            subquery: plan,
            outer_ref_columns: self.outer_ref_columns.clone(),
            spans: Spans::new(),
        }
    }
}

impl Debug for Subquery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<subquery>")
    }
}

/// Logical partitioning schemes supported by [`LogicalPlan::Repartition`]
///
/// See [`Partitioning`] for more details on partitioning
///
/// [`Partitioning`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/enum.Partitioning.html#
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number
    /// of partitions.
    Hash(Vec<Expr>, usize),
    /// The DISTRIBUTE BY clause is used to repartition the data based on the input expressions
    DistributeBy(Vec<Expr>),
}

/// Represent the unnesting operation on a list column, such as the recursion depth and
/// the output column name after unnesting
///
/// Example: given `ColumnUnnestList { output_column: "output_name", depth: 2 }`
///
/// ```text
///   input             output_name
///  ┌─────────┐      ┌─────────┐
///  │{{1,2}}  │      │ 1       │
///  ├─────────┼─────►├─────────┤
///  │{{3}}    │      │ 2       │
///  ├─────────┤      ├─────────┤
///  │{{4},{5}}│      │ 3       │
///  └─────────┘      ├─────────┤
///                   │ 4       │
///                   ├─────────┤
///                   │ 5       │
///                   └─────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ColumnUnnestList {
    pub output_column: Column,
    pub depth: usize,
}

impl Display for ColumnUnnestList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}|depth={}", self.output_column, self.depth)
    }
}

/// Unnest a column that contains a nested list type. See
/// [`UnnestOptions`] for more details.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unnest {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Columns to run unnest on, can be a list of (List/Struct) columns
    pub exec_columns: Vec<Column>,
    /// refer to the indices(in the input schema) of columns
    /// that have type list to run unnest on
    pub list_type_columns: Vec<(usize, ColumnUnnestList)>,
    /// refer to the indices (in the input schema) of columns
    /// that have type struct to run unnest on
    pub struct_type_columns: Vec<usize>,
    /// Having items aligned with the output columns
    /// representing which column in the input schema each output column depends on
    pub dependency_indices: Vec<usize>,
    /// The output schema, containing the unnested field column.
    pub schema: DFSchemaRef,
    /// Options
    pub options: UnnestOptions,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for Unnest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableUnnest<'a> {
            /// The incoming logical plan
            pub input: &'a Arc<LogicalPlan>,
            /// Columns to run unnest on, can be a list of (List/Struct) columns
            pub exec_columns: &'a Vec<Column>,
            /// refer to the indices(in the input schema) of columns
            /// that have type list to run unnest on
            pub list_type_columns: &'a Vec<(usize, ColumnUnnestList)>,
            /// refer to the indices (in the input schema) of columns
            /// that have type struct to run unnest on
            pub struct_type_columns: &'a Vec<usize>,
            /// Having items aligned with the output columns
            /// representing which column in the input schema each output column depends on
            pub dependency_indices: &'a Vec<usize>,
            /// Options
            pub options: &'a UnnestOptions,
        }
        let comparable_self = ComparableUnnest {
            input: &self.input,
            exec_columns: &self.exec_columns,
            list_type_columns: &self.list_type_columns,
            struct_type_columns: &self.struct_type_columns,
            dependency_indices: &self.dependency_indices,
            options: &self.options,
        };
        let comparable_other = ComparableUnnest {
            input: &other.input,
            exec_columns: &other.exec_columns,
            list_type_columns: &other.list_type_columns,
            struct_type_columns: &other.struct_type_columns,
            dependency_indices: &other.dependency_indices,
            options: &other.options,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::builder::LogicalTableSource;
    use crate::logical_plan::table_scan;
    use crate::{
        binary_expr, col, exists, in_subquery, lit, placeholder, scalar_subquery,
        GroupingSet,
    };

    use datafusion_common::tree_node::{
        TransformedResult, TreeNodeRewriter, TreeNodeVisitor,
    };
    use datafusion_common::{not_impl_err, Constraint, ScalarValue};

    use crate::test::function_stub::count;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> Result<LogicalPlan> {
        let plan1 = table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3]))?
            .build()?;

        table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
            .filter(in_subquery(col("state"), Arc::new(plan1)))?
            .project(vec![col("id")])?
            .build()
    }

    #[test]
    fn test_display_indent() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: employee_csv.id\
        \n  Filter: employee_csv.state IN (<subquery>)\
        \n    Subquery:\
        \n      TableScan: employee_csv projection=[state]\
        \n    TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{}", plan.display_indent()));
        Ok(())
    }

    #[test]
    fn test_display_indent_schema() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: employee_csv.id [id:Int32]\
        \n  Filter: employee_csv.state IN (<subquery>) [id:Int32, state:Utf8]\
        \n    Subquery: [state:Utf8]\
        \n      TableScan: employee_csv projection=[state] [state:Utf8]\
        \n    TableScan: employee_csv projection=[id, state] [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
        Ok(())
    }

    #[test]
    fn test_display_subquery_alias() -> Result<()> {
        let plan1 = table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3]))?
            .build()?;
        let plan1 = Arc::new(plan1);

        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .project(vec![col("id"), exists(plan1).alias("exists")])?
                .build();

        let expected = "Projection: employee_csv.id, EXISTS (<subquery>) AS exists\
        \n  Subquery:\
        \n    TableScan: employee_csv projection=[state]\
        \n  TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{}", plan?.display_indent()));
        Ok(())
    }

    #[test]
    fn test_display_graphviz() -> Result<()> {
        let plan = display_plan()?;

        let expected_graphviz = r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline

digraph {
  subgraph cluster_1
  {
    graph[label="LogicalPlan"]
    2[shape=box label="Projection: employee_csv.id"]
    3[shape=box label="Filter: employee_csv.state IN (<subquery>)"]
    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
    4[shape=box label="Subquery:"]
    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
    5[shape=box label="TableScan: employee_csv projection=[state]"]
    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
    6[shape=box label="TableScan: employee_csv projection=[id, state]"]
    3 -> 6 [arrowhead=none, arrowtail=normal, dir=back]
  }
  subgraph cluster_7
  {
    graph[label="Detailed LogicalPlan"]
    8[shape=box label="Projection: employee_csv.id\nSchema: [id:Int32]"]
    9[shape=box label="Filter: employee_csv.state IN (<subquery>)\nSchema: [id:Int32, state:Utf8]"]
    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
    10[shape=box label="Subquery:\nSchema: [state:Utf8]"]
    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
    11[shape=box label="TableScan: employee_csv projection=[state]\nSchema: [state:Utf8]"]
    10 -> 11 [arrowhead=none, arrowtail=normal, dir=back]
    12[shape=box label="TableScan: employee_csv projection=[id, state]\nSchema: [id:Int32, state:Utf8]"]
    9 -> 12 [arrowhead=none, arrowtail=normal, dir=back]
  }
}
// End DataFusion GraphViz Plan
"#;

        // just test for a few key lines in the output rather than the
        // whole thing to make test maintenance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert_eq!(expected_graphviz, graphviz);
        Ok(())
    }

    #[test]
    fn test_display_pg_json() -> Result<()> {
        let plan = display_plan()?;

        let expected_pg_json = r#"[
  {
    "Plan": {
      "Expressions": [
        "employee_csv.id"
      ],
      "Node Type": "Projection",
      "Output": [
        "id"
      ],
      "Plans": [
        {
          "Condition": "employee_csv.state IN (<subquery>)",
          "Node Type": "Filter",
          "Output": [
            "id",
            "state"
          ],
          "Plans": [
            {
              "Node Type": "Subquery",
              "Output": [
                "state"
              ],
              "Plans": [
                {
                  "Node Type": "TableScan",
                  "Output": [
                    "state"
                  ],
                  "Plans": [],
                  "Relation Name": "employee_csv"
                }
              ]
            },
            {
              "Node Type": "TableScan",
              "Output": [
                "id",
                "state"
              ],
              "Plans": [],
              "Relation Name": "employee_csv"
            }
          ]
        }
      ]
    }
  }
]"#;

        let pg_json = format!("{}", plan.display_pg_json());

        assert_eq!(expected_pg_json, pg_json);
        Ok(())
    }

    /// Tests for the Visitor trait and walking logical plan nodes
    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }

    impl<'n> TreeNodeVisitor<'n> for OkVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(TreeNodeRecursion::Continue)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(TreeNodeRecursion::Continue)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }

    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl<'n> TreeNodeVisitor<'n> for StoppingVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_false_from_pre_in.dec() {
                return Ok(TreeNodeRecursion::Stop);
            }
            self.inner.f_down(plan)?;

            Ok(TreeNodeRecursion::Continue)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_false_from_post_in.dec() {
                return Ok(TreeNodeRecursion::Stop);
            }

            self.inner.f_up(plan)
        }
    }

    /// test early stopping in pre-visit
    #[test]
    fn early_stopping_pre_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn early_stopping_post_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl<'n> TreeNodeVisitor<'n> for ErrorVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_error_from_pre_in.dec() {
                return not_impl_err!("Error in pre_visit");
            }

            self.inner.f_down(plan)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_error_from_post_in.dec() {
                return not_impl_err!("Error in post_visit");
            }

            self.inner.f_up(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor).unwrap_err();
        assert_eq!(
            "This feature is not implemented: Error in pre_visit",
            res.strip_backtrace()
        );
        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor).unwrap_err();
        assert_eq!(
            "This feature is not implemented: Error in post_visit",
            res.strip_backtrace()
        );
        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[test]
    fn projection_expr_schema_mismatch() -> Result<()> {
        let empty_schema = Arc::new(DFSchema::empty());
        let p = Projection::try_new_with_schema(
            vec![col("a")],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&empty_schema),
            })),
            empty_schema,
        );
        assert_eq!(p.err().unwrap().strip_backtrace(), "Error during planning: Projection has mismatch between number of expressions (1) and number of fields in schema (0)");
        Ok(())
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("state", DataType::Utf8, false),
        ]);

        table_scan(TableReference::none(), &schema, Some(vec![0, 1]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_replace_invalid_placeholder() {
        // test empty placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("")))
            .unwrap()
            .build()
            .unwrap();

        let param_values = vec![ScalarValue::Int32(Some(42))];
        plan.replace_params_with_values(&param_values.clone().into())
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");

        // test $0 placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("$0")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&param_values.clone().into())
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");

        // test $00 placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("$00")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&param_values.into())
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");
    }

    #[test]
    fn test_nullable_schema_after_grouping_set() {
        let schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Int32, false),
        ]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                    vec![col("foo")],
                    vec![col("bar")],
                ]))],
                vec![count(lit(true))],
            )
            .unwrap()
            .build()
            .unwrap();

        let output_schema = plan.schema();

        assert!(output_schema
            .field_with_name(None, "foo")
            .unwrap()
            .is_nullable(),);
        assert!(output_schema
            .field_with_name(None, "bar")
            .unwrap()
            .is_nullable());
    }

    #[test]
    fn test_filter_is_scalar() {
        // test empty placeholder
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let source = Arc::new(LogicalTableSource::new(schema));
        let schema = Arc::new(
            DFSchema::try_from_qualified_schema(
                TableReference::bare("tab"),
                &source.schema(),
            )
            .unwrap(),
        );
        let scan = Arc::new(LogicalPlan::TableScan(TableScan {
            table_name: TableReference::bare("tab"),
            source: Arc::clone(&source) as Arc<dyn TableSource>,
            projection: None,
            projected_schema: Arc::clone(&schema),
            filters: vec![],
            fetch: None,
        }));
        let col = schema.field_names()[0].clone();

        let filter = Filter::try_new(
            Expr::Column(col.into()).eq(Expr::Literal(ScalarValue::Int32(Some(1)))),
            scan,
        )
        .unwrap();
        assert!(!filter.is_scalar());
        let unique_schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_functional_dependencies(
                    FunctionalDependencies::new_from_constraints(
                        Some(&Constraints::new_unverified(vec![Constraint::Unique(
                            vec![0],
                        )])),
                        1,
                    ),
                )
                .unwrap(),
        );
        let scan = Arc::new(LogicalPlan::TableScan(TableScan {
            table_name: TableReference::bare("tab"),
            source,
            projection: None,
            projected_schema: Arc::clone(&unique_schema),
            filters: vec![],
            fetch: None,
        }));
        let col = schema.field_names()[0].clone();

        let filter =
            Filter::try_new(Expr::Column(col.into()).eq(lit(1i32)), scan).unwrap();
        assert!(filter.is_scalar());
    }

    #[test]
    fn test_transform_explain() {
        let schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Int32, false),
        ]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();

        let external_filter = col("foo").eq(lit(true));

        // after transformation, because plan is not the same anymore,
        // the parent plan is built again with call to LogicalPlan::with_new_inputs -> with_new_exprs
        let plan = plan
            .transform(|plan| match plan {
                LogicalPlan::TableScan(table) => {
                    let filter = Filter::try_new(
                        external_filter.clone(),
                        Arc::new(LogicalPlan::TableScan(table)),
                    )
                    .unwrap();
                    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
                }
                x => Ok(Transformed::no(x)),
            })
            .data()
            .unwrap();

        let expected = "Explain\
                        \n  Filter: foo = Boolean(true)\
                        \n    TableScan: ?table?";
        let actual = format!("{}", plan.display_indent());
        assert_eq!(expected.to_string(), actual)
    }

    #[test]
    fn test_plan_partial_ord() {
        let empty_relation = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        let describe_table = LogicalPlan::DescribeTable(DescribeTable {
            schema: Arc::new(Schema::new(vec![Field::new(
                "foo",
                DataType::Int32,
                false,
            )])),
            output_schema: DFSchemaRef::new(DFSchema::empty()),
        });

        let describe_table_clone = LogicalPlan::DescribeTable(DescribeTable {
            schema: Arc::new(Schema::new(vec![Field::new(
                "foo",
                DataType::Int32,
                false,
            )])),
            output_schema: DFSchemaRef::new(DFSchema::empty()),
        });

        assert_eq!(
            empty_relation.partial_cmp(&describe_table),
            Some(Ordering::Less)
        );
        assert_eq!(
            describe_table.partial_cmp(&empty_relation),
            Some(Ordering::Greater)
        );
        assert_eq!(describe_table.partial_cmp(&describe_table_clone), None);
    }

    #[test]
    fn test_limit_with_new_children() {
        let input = Arc::new(LogicalPlan::Values(Values {
            schema: Arc::new(DFSchema::empty()),
            values: vec![vec![]],
        }));
        let cases = [
            LogicalPlan::Limit(Limit {
                skip: None,
                fetch: None,
                input: Arc::clone(&input),
            }),
            LogicalPlan::Limit(Limit {
                skip: None,
                fetch: Some(Box::new(Expr::Literal(
                    ScalarValue::new_ten(&DataType::UInt32).unwrap(),
                ))),
                input: Arc::clone(&input),
            }),
            LogicalPlan::Limit(Limit {
                skip: Some(Box::new(Expr::Literal(
                    ScalarValue::new_ten(&DataType::UInt32).unwrap(),
                ))),
                fetch: None,
                input: Arc::clone(&input),
            }),
            LogicalPlan::Limit(Limit {
                skip: Some(Box::new(Expr::Literal(
                    ScalarValue::new_one(&DataType::UInt32).unwrap(),
                ))),
                fetch: Some(Box::new(Expr::Literal(
                    ScalarValue::new_ten(&DataType::UInt32).unwrap(),
                ))),
                input,
            }),
        ];

        for limit in cases {
            let new_limit = limit
                .with_new_exprs(
                    limit.expressions(),
                    limit.inputs().into_iter().cloned().collect(),
                )
                .unwrap();
            assert_eq!(limit, new_limit);
        }
    }

    #[test]
    fn test_with_subqueries_jump() {
        // The test plan contains a `Project` node above a `Filter` node, and the
        // `Project` node contains a subquery plan with a `Filter` root node, so returning
        // `TreeNodeRecursion::Jump` on `Project` should cause not visiting any of the
        // `Filter`s.
        let subquery_schema =
            Schema::new(vec![Field::new("sub_id", DataType::Int32, false)]);

        let subquery_plan =
            table_scan(TableReference::none(), &subquery_schema, Some(vec![0]))
                .unwrap()
                .filter(col("sub_id").eq(lit(0)))
                .unwrap()
                .build()
                .unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, Some(vec![0]))
            .unwrap()
            .filter(col("id").eq(lit(0)))
            .unwrap()
            .project(vec![col("id"), scalar_subquery(Arc::new(subquery_plan))])
            .unwrap()
            .build()
            .unwrap();

        let mut filter_found = false;
        plan.apply_with_subqueries(|plan| {
            match plan {
                LogicalPlan::Projection(..) => return Ok(TreeNodeRecursion::Jump),
                LogicalPlan::Filter(..) => filter_found = true,
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert!(!filter_found);

        struct ProjectJumpVisitor {
            filter_found: bool,
        }

        impl ProjectJumpVisitor {
            fn new() -> Self {
                Self {
                    filter_found: false,
                }
            }
        }

        impl<'n> TreeNodeVisitor<'n> for ProjectJumpVisitor {
            type Node = LogicalPlan;

            fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
                match node {
                    LogicalPlan::Projection(..) => return Ok(TreeNodeRecursion::Jump),
                    LogicalPlan::Filter(..) => self.filter_found = true,
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut visitor = ProjectJumpVisitor::new();
        plan.visit_with_subqueries(&mut visitor).unwrap();
        assert!(!visitor.filter_found);

        let mut filter_found = false;
        plan.clone()
            .transform_down_with_subqueries(|plan| {
                match plan {
                    LogicalPlan::Projection(..) => {
                        return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
                    }
                    LogicalPlan::Filter(..) => filter_found = true,
                    _ => {}
                }
                Ok(Transformed::no(plan))
            })
            .unwrap();
        assert!(!filter_found);

        let mut filter_found = false;
        plan.clone()
            .transform_down_up_with_subqueries(
                |plan| {
                    match plan {
                        LogicalPlan::Projection(..) => {
                            return Ok(Transformed::new(
                                plan,
                                false,
                                TreeNodeRecursion::Jump,
                            ))
                        }
                        LogicalPlan::Filter(..) => filter_found = true,
                        _ => {}
                    }
                    Ok(Transformed::no(plan))
                },
                |plan| Ok(Transformed::no(plan)),
            )
            .unwrap();
        assert!(!filter_found);

        struct ProjectJumpRewriter {
            filter_found: bool,
        }

        impl ProjectJumpRewriter {
            fn new() -> Self {
                Self {
                    filter_found: false,
                }
            }
        }

        impl TreeNodeRewriter for ProjectJumpRewriter {
            type Node = LogicalPlan;

            fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
                match node {
                    LogicalPlan::Projection(..) => {
                        return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
                    }
                    LogicalPlan::Filter(..) => self.filter_found = true,
                    _ => {}
                }
                Ok(Transformed::no(node))
            }
        }

        let mut rewriter = ProjectJumpRewriter::new();
        plan.rewrite_with_subqueries(&mut rewriter).unwrap();
        assert!(!rewriter.filter_found);
    }

    #[test]
    fn test_with_unresolved_placeholders() {
        let field_name = "id";
        let placeholder_value = "$1";
        let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col(field_name).eq(placeholder(placeholder_value)))
            .unwrap()
            .build()
            .unwrap();

        // Check that the placeholder parameters have not received a DataType.
        let params = plan.get_parameter_types().unwrap();
        assert_eq!(params.len(), 1);

        let parameter_type = params.clone().get(placeholder_value).unwrap().clone();
        assert_eq!(parameter_type, None);
    }

    #[test]
    fn test_join_with_new_exprs() -> Result<()> {
        fn create_test_join(
            on: Vec<(Expr, Expr)>,
            filter: Option<Expr>,
        ) -> Result<LogicalPlan> {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]);

            let left_schema = DFSchema::try_from_qualified_schema("t1", &schema)?;
            let right_schema = DFSchema::try_from_qualified_schema("t2", &schema)?;

            Ok(LogicalPlan::Join(Join {
                left: Arc::new(
                    table_scan(Some("t1"), left_schema.as_arrow(), None)?.build()?,
                ),
                right: Arc::new(
                    table_scan(Some("t2"), right_schema.as_arrow(), None)?.build()?,
                ),
                on,
                filter,
                join_type: JoinType::Inner,
                join_constraint: JoinConstraint::On,
                schema: Arc::new(left_schema.join(&right_schema)?),
                null_equals_null: false,
            }))
        }

        {
            let join = create_test_join(vec![(col("t1.a"), (col("t2.a")))], None)?;
            let LogicalPlan::Join(join) = join.with_new_exprs(
                join.expressions(),
                join.inputs().into_iter().cloned().collect(),
            )?
            else {
                unreachable!()
            };
            assert_eq!(join.on, vec![(col("t1.a"), (col("t2.a")))]);
            assert_eq!(join.filter, None);
        }

        {
            let join = create_test_join(vec![], Some(col("t1.a").gt(col("t2.a"))))?;
            let LogicalPlan::Join(join) = join.with_new_exprs(
                join.expressions(),
                join.inputs().into_iter().cloned().collect(),
            )?
            else {
                unreachable!()
            };
            assert_eq!(join.on, vec![]);
            assert_eq!(join.filter, Some(col("t1.a").gt(col("t2.a"))));
        }

        {
            let join = create_test_join(
                vec![(col("t1.a"), (col("t2.a")))],
                Some(col("t1.b").gt(col("t2.b"))),
            )?;
            let LogicalPlan::Join(join) = join.with_new_exprs(
                join.expressions(),
                join.inputs().into_iter().cloned().collect(),
            )?
            else {
                unreachable!()
            };
            assert_eq!(join.on, vec![(col("t1.a"), (col("t2.a")))]);
            assert_eq!(join.filter, Some(col("t1.b").gt(col("t2.b"))));
        }

        {
            let join = create_test_join(
                vec![(col("t1.a"), (col("t2.a"))), (col("t1.b"), (col("t2.b")))],
                None,
            )?;
            let LogicalPlan::Join(join) = join.with_new_exprs(
                vec![
                    binary_expr(col("t1.a"), Operator::Plus, lit(1)),
                    binary_expr(col("t2.a"), Operator::Plus, lit(2)),
                    col("t1.b"),
                    col("t2.b"),
                    lit(true),
                ],
                join.inputs().into_iter().cloned().collect(),
            )?
            else {
                unreachable!()
            };
            assert_eq!(
                join.on,
                vec![
                    (
                        binary_expr(col("t1.a"), Operator::Plus, lit(1)),
                        binary_expr(col("t2.a"), Operator::Plus, lit(2))
                    ),
                    (col("t1.b"), (col("t2.b")))
                ]
            );
            assert_eq!(join.filter, Some(lit(true)));
        }

        Ok(())
    }
}

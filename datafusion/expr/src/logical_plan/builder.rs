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

//! This module provides a builder for creating LogicalPlans

use crate::expr_rewriter::{
    coerce_plan_expr_for_schema, normalize_col, normalize_cols, rewrite_sort_cols_by_aggs,
};
use crate::type_coercion::binary::comparison_coercion;
use crate::utils::{columnize_expr, exprlist_to_fields, from_plan};
use crate::{and, binary_expr, Operator};
use crate::{
    logical_plan::{
        Aggregate, Analyze, CrossJoin, Distinct, EmptyRelation, Explain, Filter, Join,
        JoinConstraint, JoinType, Limit, LogicalPlan, Partitioning, PlanType, Prepare,
        Projection, Repartition, Sort, SubqueryAlias, TableScan, ToStringifiedPlan,
        Union, Values, Window,
    },
    utils::{
        can_hash, expand_qualified_wildcard, expand_wildcard,
        group_window_expr_by_sort_keys,
    },
    Expr, ExprSchemable, TableSource,
};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
    ToDFSchema,
};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;

/// Default table name for unnamed table
pub const UNNAMED_TABLE: &str = "?table?";

/// Builder for logical plans
///
/// ```
/// # use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
/// # use datafusion_common::Result;
/// # use arrow::datatypes::{Schema, DataType, Field};
/// #
/// # fn main() -> Result<()> {
/// #
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("id", DataType::Int32, false),
/// #           Field::new("first_name", DataType::Utf8, false),
/// #           Field::new("last_name", DataType::Utf8, false),
/// #           Field::new("state", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// #
/// // Create a plan similar to
/// // SELECT last_name
/// // FROM employees
/// // WHERE salary < 1000
/// let plan = table_scan(
///              Some("employee"),
///              &employee_schema(),
///              None,
///            )?
///            // Keep only rows where salary < 1000
///            .filter(col("salary").lt_eq(lit(1000)))?
///            // only show "last_name" in the final results
///            .project(vec![col("last_name")])?
///            .build()?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    /// Return the output schema of the plan build so far
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    /// Create an empty relation.
    ///
    /// `produce_one_row` set to true means this empty node needs to produce a placeholder row.
    pub fn empty(produce_one_row: bool) -> Self {
        Self::from(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    /// Create a values list based relation, and the schema is inferred from data, consuming
    /// `value`. See the [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    ///
    /// By default, it assigns the names column1, column2, etc. to the columns of a VALUES table.
    /// The column names are not specified by the SQL standard and different database systems do it differently,
    /// so it's usually better to override the default names with a table alias list.
    ///
    /// If the values include params/binders such as $1, $2, $3, etc, then the `param_data_types` should be provided.
    pub fn values(mut values: Vec<Vec<Expr>>) -> Result<Self> {
        if values.is_empty() {
            return Err(DataFusionError::Plan("Values list cannot be empty".into()));
        }
        let n_cols = values[0].len();
        if n_cols == 0 {
            return Err(DataFusionError::Plan(
                "Values list cannot be zero length".into(),
            ));
        }
        let empty_schema = DFSchema::empty();
        let mut field_types: Vec<Option<DataType>> = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            field_types.push(None);
        }
        // hold all the null holes so that we can correct their data types later
        let mut nulls: Vec<(usize, usize)> = Vec::new();
        for (i, row) in values.iter().enumerate() {
            if row.len() != n_cols {
                return Err(DataFusionError::Plan(format!(
                    "Inconsistent data length across values list: got {} values in row {} but expected {}",
                    row.len(),
                    i,
                    n_cols
                )));
            }
            field_types = row
                .iter()
                .enumerate()
                .map(|(j, expr)| {
                    if let Expr::Literal(ScalarValue::Null) = expr {
                        nulls.push((i, j));
                        Ok(field_types[j].clone())
                    } else {
                        let data_type = expr.get_type(&empty_schema)?;
                        if let Some(prev_data_type) = &field_types[j] {
                            if prev_data_type != &data_type {
                                let err = format!("Inconsistent data type across values list at row {} column {}", i, j);
                                return Err(DataFusionError::Plan(err));
                            }
                        }
                        Ok(Some(data_type))
                    }
                })
                .collect::<Result<Vec<Option<DataType>>>>()?;
        }
        let fields = field_types
            .iter()
            .enumerate()
            .map(|(j, data_type)| {
                // naming is following convention https://www.postgresql.org/docs/current/queries-values.html
                let name = &format!("column{}", j + 1);
                DFField::new(
                    None,
                    name,
                    data_type.clone().unwrap_or(DataType::Utf8),
                    true,
                )
            })
            .collect::<Vec<_>>();
        for (i, j) in nulls {
            values[i][j] = Expr::Literal(ScalarValue::try_from(fields[j].data_type())?);
        }
        let schema =
            DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new())?);
        Ok(Self::from(LogicalPlan::Values(Values { schema, values })))
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan(
        table_name: impl Into<String>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Self::scan_with_filters(table_name, table_source, projection, vec![])
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan_with_filters(
        table_name: impl Into<String>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.is_empty() {
            return Err(DataFusionError::Plan(
                "table_name cannot be empty".to_string(),
            ));
        }

        let schema = table_source.schema();

        let projected_schema = projection
            .as_ref()
            .map(|p| {
                DFSchema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            DFField::from_qualified(&table_name, schema.field(*i).clone())
                        })
                        .collect(),
                    schema.metadata().clone(),
                )
            })
            .unwrap_or_else(|| {
                DFSchema::try_from_qualified_schema(&table_name, &schema)
            })?;

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name,
            source: table_source,
            projected_schema: Arc::new(projected_schema),
            projection,
            filters,
            fetch: None,
        });
        Ok(Self::from(table_scan))
    }

    /// Wrap a plan in a window
    pub fn window_plan(
        input: LogicalPlan,
        window_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut plan = input;
        let mut groups = group_window_expr_by_sort_keys(&window_exprs)?;
        // sort by sort_key len descending, so that more deeply sorted plans gets nested further
        // down as children; to further mimic the behavior of PostgreSQL, we want stable sort
        // and a reverse so that tieing sort keys are reversed in order; note that by this rule
        // if there's an empty over, it'll be at the top level
        groups.sort_by(|(key_a, _), (key_b, _)| key_a.len().cmp(&key_b.len()));
        groups.reverse();
        for (_, exprs) in groups {
            let window_exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            // the partition and sort itself is done at physical level, see physical_planner's
            // fn create_initial_plan
            plan = LogicalPlanBuilder::from(plan)
                .window(window_exprs)?
                .build()?;
        }
        Ok(plan)
    }
    /// Apply a projection without alias.
    pub fn project(
        &self,
        expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        Ok(Self::from(project(self.plan.clone(), expr)?))
    }

    /// Apply a filter
    pub fn filter(&self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Ok(Self::from(LogicalPlan::Filter(Filter::try_new(
            expr,
            Arc::new(self.plan.clone()),
        )?)))
    }

    /// Make a builder for a prepare logical plan from the builder's plan
    pub fn prepare(&self, name: String, data_types: Vec<DataType>) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Prepare(Prepare {
            name,
            data_types,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Limit the number of rows returned
    ///
    /// `skip` - Number of rows to skip before fetch any row.
    ///
    /// `fetch` - Maximum number of rows to fetch, after skipping `skip` rows,
    ///          if specified.
    pub fn limit(&self, skip: usize, fetch: Option<usize>) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Limit(Limit {
            skip,
            fetch,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply an alias
    pub fn alias(&self, alias: &str) -> Result<Self> {
        Ok(Self::from(subquery_alias(&self.plan, alias)?))
    }

    /// Add missing sort columns to all downstream projection
    fn add_missing_columns(
        curr_plan: LogicalPlan,
        missing_cols: &[Column],
    ) -> Result<LogicalPlan> {
        match curr_plan {
            LogicalPlan::Projection(Projection {
                input,
                mut expr,
                schema: _,
            }) if missing_cols
                .iter()
                .all(|c| input.schema().field_from_column(c).is_ok()) =>
            {
                let mut missing_exprs = missing_cols
                    .iter()
                    .map(|c| normalize_col(Expr::Column(c.clone()), &input))
                    .collect::<Result<Vec<_>>>()?;

                // Do not let duplicate columns to be added, some of the
                // missing_cols may be already present but without the new
                // projected alias.
                missing_exprs.retain(|e| !expr.contains(e));
                expr.extend(missing_exprs);
                Ok(project((*input).clone(), expr)?)
            }
            _ => {
                let new_inputs = curr_plan
                    .inputs()
                    .into_iter()
                    .map(|input_plan| {
                        Self::add_missing_columns((*input_plan).clone(), missing_cols)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let expr = curr_plan.expressions();
                from_plan(&curr_plan, &expr, &new_inputs)
            }
        }
    }

    /// Apply a sort
    pub fn sort(
        &self,
        exprs: impl IntoIterator<Item = impl Into<Expr>> + Clone,
    ) -> Result<Self> {
        let exprs = rewrite_sort_cols_by_aggs(exprs, &self.plan)?;

        let schema = self.plan.schema();

        // Collect sort columns that are missing in the input plan's schema
        let mut missing_cols: Vec<Column> = vec![];
        exprs
            .clone()
            .into_iter()
            .try_for_each::<_, Result<()>>(|expr| {
                let columns = expr.to_columns()?;

                columns.into_iter().for_each(|c| {
                    if schema.field_from_column(&c).is_err() {
                        missing_cols.push(c);
                    }
                });

                Ok(())
            })?;

        if missing_cols.is_empty() {
            return Ok(Self::from(LogicalPlan::Sort(Sort {
                expr: normalize_cols(exprs, &self.plan)?,
                input: Arc::new(self.plan.clone()),
                fetch: None,
            })));
        }

        let plan = Self::add_missing_columns(self.plan.clone(), &missing_cols)?;
        let sort_plan = LogicalPlan::Sort(Sort {
            expr: normalize_cols(exprs, &plan)?,
            input: Arc::new(plan.clone()),
            fetch: None,
        });
        // remove pushed down sort columns
        let new_expr = schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        Ok(Self::from(LogicalPlan::Projection(Projection::try_new(
            new_expr,
            Arc::new(sort_plan),
        )?)))
    }

    /// Apply a union, preserving duplicate rows
    pub fn union(&self, plan: LogicalPlan) -> Result<Self> {
        Ok(Self::from(union(self.plan.clone(), plan)?))
    }

    /// Apply a union, removing duplicate rows
    pub fn union_distinct(&self, plan: LogicalPlan) -> Result<Self> {
        // unwrap top-level Distincts, to avoid duplication
        let left_plan = self.plan.clone();
        let left_plan: LogicalPlan = match left_plan {
            LogicalPlan::Distinct(Distinct { input }) => (*input).clone(),
            _ => left_plan,
        };
        let right_plan: LogicalPlan = match plan {
            LogicalPlan::Distinct(Distinct { input }) => (*input).clone(),
            _ => plan,
        };

        Ok(Self::from(LogicalPlan::Distinct(Distinct {
            input: Arc::new(union(left_plan, right_plan)?),
        })))
    }

    /// Apply deduplication: Only distinct (different) values are returned)
    pub fn distinct(&self) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Distinct(Distinct {
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a join with on constraint.
    ///
    /// Filter expression expected to contain non-equality predicates that can not be pushed
    /// down to any of join inputs.
    /// In case of outer join, filter applied to only matched rows.
    pub fn join(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        self.join_detailed(right, join_type, join_keys, filter, false)
    }

    fn normalize(
        plan: &LogicalPlan,
        column: impl Into<Column> + Clone,
    ) -> Result<Column> {
        let schemas = plan.all_schemas();
        let using_columns = plan.using_columns()?;
        column
            .into()
            .normalize_with_schemas(&schemas, &using_columns)
    }

    /// Apply a join with on constraint and specified null equality
    /// If null_equals_null is true then null == null, else null != null
    pub fn join_detailed(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
        null_equals_null: bool,
    ) -> Result<Self> {
        if join_keys.0.len() != join_keys.1.len() {
            return Err(DataFusionError::Plan(
                "left_keys and right_keys were not the same length".to_string(),
            ));
        }

        let (left_keys, right_keys): (Vec<Result<Column>>, Vec<Result<Column>>) =
            join_keys
                .0
                .into_iter()
                .zip(join_keys.1.into_iter())
                .map(|(l, r)| {
                    let l = l.into();
                    let r = r.into();

                    match (&l.relation, &r.relation) {
                        (Some(lr), Some(rr)) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (l_is_left, l_is_right, r_is_left, r_is_right) {
                                (_, Ok(_), Ok(_), _) => (Ok(r), Ok(l)),
                                (Ok(_), _, _, Ok(_)) => (Ok(l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (Some(lr), None) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);

                            match (l_is_left, l_is_right) {
                                (Ok(_), _) => (Ok(l), Self::normalize(right, r)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, r), Ok(l)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (None, Some(rr)) => {
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (r_is_left, r_is_right) {
                                (Ok(_), _) => (Ok(r), Self::normalize(right, l)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (None, None) => {
                            let mut swap = false;
                            let left_key = Self::normalize(&self.plan, l.clone())
                                .or_else(|_| {
                                    swap = true;
                                    Self::normalize(right, l)
                                });
                            if swap {
                                (Self::normalize(&self.plan, r), left_key)
                            } else {
                                (left_key, Self::normalize(right, r))
                            }
                        }
                    }
                })
                .unzip();

        let left_keys = left_keys.into_iter().collect::<Result<Vec<Column>>>()?;
        let right_keys = right_keys.into_iter().collect::<Result<Vec<Column>>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        Ok(Self::from(LogicalPlan::Join(Join {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(join_schema),
            null_equals_null,
        })))
    }

    /// Apply a join with using constraint, which duplicates all join columns in output schema.
    pub fn join_using(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        using_keys: Vec<impl Into<Column> + Clone>,
    ) -> Result<Self> {
        let left_keys: Vec<Column> = using_keys
            .clone()
            .into_iter()
            .map(|c| Self::normalize(&self.plan, c))
            .collect::<Result<_>>()?;
        let right_keys: Vec<Column> = using_keys
            .into_iter()
            .map(|c| Self::normalize(right, c))
            .collect::<Result<_>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;
        let mut join_on: Vec<(Column, Column)> = vec![];
        let mut filters: Option<Expr> = None;
        for (l, r) in &on {
            if self.plan.schema().field_from_column(l).is_ok()
                && right.schema().field_from_column(r).is_ok()
                && can_hash(self.plan.schema().field_from_column(l)?.data_type())
            {
                join_on.push((l.clone(), r.clone()));
            } else if self.plan.schema().field_from_column(r).is_ok()
                && right.schema().field_from_column(l).is_ok()
                && can_hash(self.plan.schema().field_from_column(r)?.data_type())
            {
                join_on.push((r.clone(), l.clone()));
            } else {
                let expr = binary_expr(
                    Expr::Column(l.clone()),
                    Operator::Eq,
                    Expr::Column(r.clone()),
                );
                match filters {
                    None => filters = Some(expr),
                    Some(filter_expr) => filters = Some(and(expr, filter_expr)),
                }
            }
        }
        if join_on.is_empty() {
            let join = Self::from(self.plan.clone()).cross_join(&right.clone())?;
            join.filter(filters.ok_or_else(|| {
                DataFusionError::Internal("filters should not be None here".to_string())
            })?)
        } else {
            Ok(Self::from(LogicalPlan::Join(Join {
                left: Arc::new(self.plan.clone()),
                right: Arc::new(right.clone()),
                on: join_on,
                filter: filters,
                join_type,
                join_constraint: JoinConstraint::Using,
                schema: DFSchemaRef::new(join_schema),
                null_equals_null: false,
            })))
        }
    }

    /// Apply a cross join
    pub fn cross_join(&self, right: &LogicalPlan) -> Result<Self> {
        let schema = self.plan.schema().join(right.schema())?;
        Ok(Self::from(LogicalPlan::CrossJoin(CrossJoin {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            schema: DFSchemaRef::new(schema),
        })))
    }

    /// Repartition
    pub fn repartition(&self, partitioning_scheme: Partitioning) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Repartition(Repartition {
            input: Arc::new(self.plan.clone()),
            partitioning_scheme,
        })))
    }

    /// Apply a window functions to extend the schema
    pub fn window(
        &self,
        window_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let window_expr = normalize_cols(window_expr, &self.plan)?;
        let all_expr = window_expr.iter();
        validate_unique_names("Windows", all_expr.clone())?;
        let mut window_fields: Vec<DFField> = exprlist_to_fields(all_expr, &self.plan)?;
        window_fields.extend_from_slice(self.plan.schema().fields());
        Ok(Self::from(LogicalPlan::Window(Window {
            input: Arc::new(self.plan.clone()),
            window_expr,
            schema: Arc::new(DFSchema::new_with_metadata(
                window_fields,
                self.plan.schema().metadata().clone(),
            )?),
        })))
    }

    /// Apply an aggregate: grouping on the `group_expr` expressions
    /// and calculating `aggr_expr` aggregates for each distinct
    /// value of the `group_expr`;
    pub fn aggregate(
        &self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;
        Ok(Self::from(LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
        )?)))
    }

    /// Create an expression to represent the explanation of the plan
    ///
    /// if `analyze` is true, runs the actual plan and produces
    /// information about metrics during run.
    ///
    /// if `verbose` is true, prints out additional details.
    pub fn explain(&self, verbose: bool, analyze: bool) -> Result<Self> {
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(Self::from(LogicalPlan::Analyze(Analyze {
                verbose,
                input: Arc::new(self.plan.clone()),
                schema,
            })))
        } else {
            let stringified_plans =
                vec![self.plan.to_stringified(PlanType::InitialLogicalPlan)];

            Ok(Self::from(LogicalPlan::Explain(Explain {
                verbose,
                plan: Arc::new(self.plan.clone()),
                stringified_plans,
                schema,
            })))
        }
    }

    /// Process intersect set operator
    pub fn intersect(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::LeftSemi,
            is_all,
        )
    }

    /// Process except set operator
    pub fn except(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::LeftAnti,
            is_all,
        )
    }

    /// Process intersect or except
    fn intersect_or_except(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        join_type: JoinType,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        let left_len = left_plan.schema().fields().len();
        let right_len = right_plan.schema().fields().len();

        if left_len != right_len {
            return Err(DataFusionError::Plan(format!(
                "INTERSECT/EXCEPT query must have the same number of columns. Left is {} and right is {}.",
                left_len, right_len
            )));
        }

        let join_keys = left_plan
            .schema()
            .fields()
            .iter()
            .zip(right_plan.schema().fields().iter())
            .map(|(left_field, right_field)| {
                (
                    (Column::from_name(left_field.name())),
                    (Column::from_name(right_field.name())),
                )
            })
            .unzip();
        if is_all {
            LogicalPlanBuilder::from(left_plan)
                .join_detailed(&right_plan, join_type, join_keys, None, true)?
                .build()
        } else {
            LogicalPlanBuilder::from(left_plan)
                .distinct()?
                .join_detailed(&right_plan, join_type, join_keys, None, true)?
                .build()
        }
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &DFSchema,
    right: &DFSchema,
    join_type: &JoinType,
) -> Result<DFSchema> {
    let fields: Vec<DFField> = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let right_fields = right.fields().iter();
            let left_fields = left.fields().iter();
            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // Only use the left side for the schema
            left.fields().clone()
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            // Only use the right side for the schema
            right.fields().clone()
        }
    };

    let mut metadata = left.metadata().clone();
    metadata.extend(right.metadata().clone());
    DFSchema::new_with_metadata(fields, metadata)
}

/// Errors if one or more expressions have equal names.
pub(crate) fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.into_iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.display_name()?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                Err(DataFusionError::Plan(
                    format!("{} require unique expression names \
                             but the expression \"{:?}\" at position {} and \"{:?}\" \
                             at position {} have the same name. Consider aliasing (\"AS\") one of them.",
                             node_name, existing_expr, existing_position, expr, position,
                            )
                ))
            }
        }
    })
}

pub fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            ignore_alias @ Expr::Alias { .. } => ignore_alias,
            ignore_col @ Expr::Column { .. } => ignore_col,
            x => x.alias(schema.field(i).name()),
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        alias_expr, input, schema,
    )?))
}

/// Union two logical plans.
pub fn union(left_plan: LogicalPlan, right_plan: LogicalPlan) -> Result<LogicalPlan> {
    let left_col_num = left_plan.schema().fields().len();

    // check union plan length same.
    let right_col_num = right_plan.schema().fields().len();
    if right_col_num != left_col_num {
        return Err(DataFusionError::Plan(format!(
            "Union queries must have the same number of columns, (left is {}, right is {})",
            left_col_num, right_col_num)
        ));
    }

    // create union schema
    let union_schema = (0..left_col_num)
        .map(|i| {
            let left_field = left_plan.schema().field(i);
            let right_field = right_plan.schema().field(i);
            let nullable = left_field.is_nullable() || right_field.is_nullable();
            let data_type =
                comparison_coercion(left_field.data_type(), right_field.data_type())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                    "UNION Column {} (type: {}) is not compatible with column {} (type: {})",
                    right_field.name(),
                    right_field.data_type(),
                    left_field.name(),
                    left_field.data_type()
                ))
                    })?;

            Ok(DFField::new(
                None,
                left_field.name(),
                data_type,
                nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .to_dfschema()?;

    let inputs = vec![left_plan, right_plan]
        .into_iter()
        .flat_map(|p| match p {
            LogicalPlan::Union(Union { inputs, .. }) => inputs,
            x => vec![Arc::new(x)],
        })
        .map(|p| {
            let plan = coerce_plan_expr_for_schema(&p, &union_schema)?;
            match plan {
                LogicalPlan::Projection(Projection { expr, input, .. }) => {
                    Ok(Arc::new(project_with_column_index(
                        expr.to_vec(),
                        input,
                        Arc::new(union_schema.clone()),
                    )?))
                }
                x => Ok(Arc::new(x)),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    if inputs.is_empty() {
        return Err(DataFusionError::Plan("Empty UNION".to_string()));
    }

    Ok(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::new(union_schema),
    }))
}

/// Create Projection
/// # Errors
/// This function errors under any of the following conditions:
/// * Two or more expressions have the same name
/// * An invalid expression is used (e.g. a `sort` expression)
pub fn project(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<Expr>>,
) -> Result<LogicalPlan> {
    let input_schema = plan.schema();
    let mut projected_expr = vec![];
    for e in expr {
        let e = e.into();
        match e {
            Expr::Wildcard => {
                projected_expr.extend(expand_wildcard(input_schema, &plan)?)
            }
            Expr::QualifiedWildcard { ref qualifier } => projected_expr
                .extend(expand_qualified_wildcard(qualifier, input_schema, &plan)?),
            _ => projected_expr
                .push(columnize_expr(normalize_col(e, &plan)?, input_schema)),
        }
    }
    validate_unique_names("Projections", projected_expr.iter())?;
    let input_schema = DFSchema::new_with_metadata(
        exprlist_to_fields(&projected_expr, &plan)?,
        plan.schema().metadata().clone(),
    )?;

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        projected_expr,
        Arc::new(plan.clone()),
        DFSchemaRef::new(input_schema),
    )?))
}

/// Create a SubqueryAlias to wrap a LogicalPlan.
pub fn subquery_alias(plan: &LogicalPlan, alias: &str) -> Result<LogicalPlan> {
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        plan.clone(),
        alias,
    )?))
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
/// This is mostly used for testing and documentation.
pub fn table_scan(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let table_source = Arc::new(LogicalTableSource { table_schema });
    LogicalPlanBuilder::scan(name.unwrap_or(UNNAMED_TABLE), table_source, projection)
}

/// Wrap projection for a plan, if the join keys contains normal expression.
pub fn wrap_projection_for_join_if_necessary(
    join_keys: &[Expr],
    input: LogicalPlan,
) -> Result<(LogicalPlan, Vec<Column>, bool)> {
    let input_schema = input.schema();
    let alias_join_keys: Vec<Expr> = join_keys
        .iter()
        .map(|key| {
            // The display_name() of cast expression will ignore the cast info, and show the inner expression name.
            // If we do not add alais, it will throw same field name error in the schema when adding projection.
            // For example:
            //    input scan : [a, b, c],
            //    join keys: [cast(a as int)]
            //
            //  then a and cast(a as int) will use the same field name - `a` in projection schema.
            //  https://github.com/apache/arrow-datafusion/issues/4478
            if matches!(key, Expr::Cast(_))
                || matches!(
                    key,
                    Expr::TryCast {
                        expr: _,
                        data_type: _
                    }
                )
            {
                let alias = format!("{:?}", key);
                key.clone().alias(alias)
            } else {
                key.clone()
            }
        })
        .collect::<Vec<_>>();

    let need_project = join_keys.iter().any(|key| !matches!(key, Expr::Column(_)));
    let plan = if need_project {
        let mut projection = expand_wildcard(input_schema, &input)?;
        let join_key_items = alias_join_keys
            .iter()
            .flat_map(|expr| expr.try_into_col().is_err().then_some(expr))
            .cloned()
            .collect::<HashSet<Expr>>();
        projection.extend(join_key_items);

        LogicalPlanBuilder::from(input)
            .project(projection)?
            .build()?
    } else {
        input
    };

    let join_on = alias_join_keys
        .into_iter()
        .map(|key| {
            key.try_into_col()
                .or_else(|_| Ok(Column::from_name(key.display_name()?)))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((plan, join_on, need_project))
}

/// Basic TableSource implementation intended for use in tests and documentation. It is expected
/// that users will provide their own TableSource implementations or use DataFusion's
/// DefaultTableSource.
pub struct LogicalTableSource {
    table_schema: SchemaRef,
}

impl LogicalTableSource {
    /// Create a new LogicalTableSource
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl TableSource for LogicalTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::expr_fn::exists;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::SchemaError;

    use crate::logical_plan::StringifiedPlan;

    use super::*;
    use crate::{col, in_subquery, lit, scalar_subquery, sum};

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .filter(col("state").eq(lit("CO")))?
                .project(vec![col("id")])?
                .build()?;

        let expected = "Projection: employee_csv.id\
        \n  Filter: employee_csv.state = Utf8(\"CO\")\
        \n    TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_schema() {
        let schema = employee_schema();
        let plan = table_scan(Some("employee_csv"), &schema, None).unwrap();

        let expected =
            DFSchema::try_from_qualified_schema("employee_csv", &schema).unwrap();

        assert_eq!(&expected, plan.schema().as_ref())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .aggregate(
                    vec![col("state")],
                    vec![sum(col("salary")).alias("total_salary")],
                )?
                .project(vec![col("state"), col("total_salary")])?
                .limit(2, Some(10))?
                .build()?;

        let expected = "Limit: skip=2, fetch=10\
                \n  Projection: employee_csv.state, total_salary\
                \n    Aggregate: groupBy=[[employee_csv.state]], aggr=[[SUM(employee_csv.salary) AS total_salary]]\
                \n      TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .sort(vec![
                    Expr::Sort {
                        expr: Box::new(col("state")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("salary")),
                        asc: false,
                        nulls_first: false,
                    },
                ])?
                .build()?;

        let expected = "Sort: employee_csv.state ASC NULLS FIRST, employee_csv.salary DESC NULLS LAST\
        \n  TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_using_join_wildcard_projection() -> Result<()> {
        let t2 = table_scan(Some("t2"), &employee_schema(), None)?.build()?;

        let plan = table_scan(Some("t1"), &employee_schema(), None)?
            .join_using(&t2, JoinType::Inner, vec!["id"])?
            .project(vec![Expr::Wildcard])?
            .build()?;

        // id column should only show up once in projection
        let expected = "Projection: t1.id, t1.first_name, t1.last_name, t1.state, t1.salary, t2.first_name, t2.last_name, t2.state, t2.salary\
        \n  Inner Join: Using t1.id = t2.id\
        \n    TableScan: t1\
        \n    TableScan: t2";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_union_combined_single_union() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .union(plan.build()?)?
            .union(plan.build()?)?
            .union(plan.build()?)?
            .build()?;

        // output has only one union
        let expected = "Union\
        \n  TableScan: employee_csv projection=[state, salary]\
        \n  TableScan: employee_csv projection=[state, salary]\
        \n  TableScan: employee_csv projection=[state, salary]\
        \n  TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_union_distinct_combined_single_union() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .union_distinct(plan.build()?)?
            .union_distinct(plan.build()?)?
            .union_distinct(plan.build()?)?
            .build()?;

        // output has only one union
        let expected = "\
        Distinct:\
        \n  Union\
        \n    TableScan: employee_csv projection=[state, salary]\
        \n    TableScan: employee_csv projection=[state, salary]\
        \n    TableScan: employee_csv projection=[state, salary]\
        \n    TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_union_different_num_columns_error() -> Result<()> {
        let plan1 = table_scan(None, &employee_schema(), Some(vec![3]))?;

        let plan2 = table_scan(None, &employee_schema(), Some(vec![3, 4]))?;

        let expected = "Error during planning: Union queries must have the same number of columns, (left is 1, right is 2)";
        let err_msg1 = plan1.union(plan2.build()?).unwrap_err();
        let err_msg2 = plan1.union_distinct(plan2.build()?).unwrap_err();

        assert_eq!(err_msg1.to_string(), expected);
        assert_eq!(err_msg2.to_string(), expected);

        Ok(())
    }

    #[test]
    fn plan_builder_simple_distinct() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .filter(col("state").eq(lit("CO")))?
                .project(vec![col("id")])?
                .distinct()?
                .build()?;

        let expected = "\
        Distinct:\
        \n  Projection: employee_csv.id\
        \n    Filter: employee_csv.state = Utf8(\"CO\")\
        \n      TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn exists_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .build()?;

        let expected = "Filter: EXISTS (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.a\
        \n        TableScan: foo\
        \n  Projection: bar.a\
        \n    TableScan: bar";
        assert_eq!(expected, format!("{:?}", outer_query));

        Ok(())
    }

    #[test]
    fn filter_in_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        // SELECT a FROM bar WHERE a IN (SELECT a FROM foo WHERE a = bar.a)
        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![col("a")])?
            .filter(in_subquery(col("a"), Arc::new(subquery)))?
            .build()?;

        let expected = "Filter: bar.a IN (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.a\
        \n        TableScan: foo\
        \n  Projection: bar.a\
        \n    TableScan: bar";
        assert_eq!(expected, format!("{:?}", outer_query));

        Ok(())
    }

    #[test]
    fn select_scalar_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("b")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        // SELECT (SELECT a FROM foo WHERE a = bar.a) FROM bar
        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![scalar_subquery(Arc::new(subquery))])?
            .build()?;

        let expected = "Projection: (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.b\
        \n        TableScan: foo\
        \n  TableScan: bar";
        assert_eq!(expected, format!("{:?}", outer_query));

        Ok(())
    }

    #[test]
    fn projection_non_unique_names() -> Result<()> {
        let plan = table_scan(
            Some("employee_csv"),
            &employee_schema(),
            // project id and first_name by column index
            Some(vec![0, 1]),
        )?
        // two columns with the same name => error
        .project(vec![col("id"), col("first_name").alias("id")]);

        match plan {
            Err(DataFusionError::SchemaError(SchemaError::AmbiguousReference {
                qualifier,
                name,
            })) => {
                assert_eq!("employee_csv", qualifier.unwrap().as_str());
                assert_eq!("id", &name);
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::SchemaError".to_string(),
            )),
        }
    }

    #[test]
    fn aggregate_non_unique_names() -> Result<()> {
        let plan = table_scan(
            Some("employee_csv"),
            &employee_schema(),
            // project state and salary by column index
            Some(vec![3, 4]),
        )?
        // two columns with the same name => error
        .aggregate(vec![col("state")], vec![sum(col("salary")).alias("state")]);

        match plan {
            Err(DataFusionError::SchemaError(SchemaError::AmbiguousReference {
                qualifier,
                name,
            })) => {
                assert_eq!("employee_csv", qualifier.unwrap().as_str());
                assert_eq!("state", &name);
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::SchemaError".to_string(),
            )),
        }
    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn stringified_plan() {
        let stringified_plan =
            StringifiedPlan::new(PlanType::InitialLogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false)); // not in non verbose mode

        let stringified_plan =
            StringifiedPlan::new(PlanType::FinalLogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode too

        let stringified_plan =
            StringifiedPlan::new(PlanType::InitialPhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false)); // not in non verbose mode

        let stringified_plan =
            StringifiedPlan::new(PlanType::FinalPhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode

        let stringified_plan = StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "random opt pass".into(),
            },
            "...the plan...",
        );
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));
    }

    fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    #[test]
    fn plan_builder_intersect_different_num_columns_error() -> Result<()> {
        let plan1 = table_scan(None, &employee_schema(), Some(vec![3]))?;

        let plan2 = table_scan(None, &employee_schema(), Some(vec![3, 4]))?;

        let expected = "Error during planning: INTERSECT/EXCEPT query must have the same number of columns. \
         Left is 1 and right is 2.";
        let err_msg1 =
            LogicalPlanBuilder::intersect(plan1.build()?, plan2.build()?, true)
                .unwrap_err();

        assert_eq!(err_msg1.to_string(), expected);

        Ok(())
    }
}

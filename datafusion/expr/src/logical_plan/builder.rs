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

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::iter::once;
use std::sync::Arc;

use crate::dml::CopyTo;
use crate::expr::{PlannedReplaceSelectItem, Sort as SortExpr};
use crate::expr_rewriter::{
    coerce_plan_expr_for_schema, normalize_col,
    normalize_col_with_schemas_and_ambiguity_check, normalize_cols, normalize_sorts,
    rewrite_sort_cols_by_aggs,
};
use crate::logical_plan::{
    Aggregate, Analyze, Distinct, DistinctOn, EmptyRelation, Explain, Filter, Join,
    JoinConstraint, JoinType, Limit, LogicalPlan, Partitioning, PlanType, Prepare,
    Projection, Repartition, Sort, SubqueryAlias, TableScan, Union, Unnest, Values,
    Window,
};
use crate::select_expr::SelectExpr;
use crate::utils::{
    can_hash, columnize_expr, compare_sort_expr, expand_qualified_wildcard,
    expand_wildcard, expr_to_columns, find_valid_equijoin_key_pair,
    group_window_expr_by_sort_keys,
};
use crate::{
    and, binary_expr, lit, DmlStatement, ExplainOption, Expr, ExprSchemable, Operator,
    RecursiveQuery, Statement, TableProviderFilterPushDown, TableSource, WriteOp,
};

use super::dml::InsertOp;
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{
    exec_err, get_target_functional_dependencies, not_impl_err, plan_datafusion_err,
    plan_err, Column, Constraints, DFSchema, DFSchemaRef, DataFusionError, NullEquality,
    Result, ScalarValue, TableReference, ToDFSchema, UnnestOptions,
};
use datafusion_expr_common::type_coercion::binary::type_union_resolution;

use indexmap::IndexSet;

/// Default table name for unnamed table
pub const UNNAMED_TABLE: &str = "?table?";

/// Options for [`LogicalPlanBuilder`]
#[derive(Default, Debug, Clone)]
pub struct LogicalPlanBuilderOptions {
    /// Flag indicating whether the plan builder should add
    /// functionally dependent expressions as additional aggregation groupings.
    add_implicit_group_by_exprs: bool,
}

impl LogicalPlanBuilderOptions {
    pub fn new() -> Self {
        Default::default()
    }

    /// Should the builder add functionally dependent expressions as additional aggregation groupings.
    pub fn with_add_implicit_group_by_exprs(mut self, add: bool) -> Self {
        self.add_implicit_group_by_exprs = add;
        self
    }
}

/// Builder for logical plans
///
/// # Example building a simple plan
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
/// let plan = table_scan(Some("employee"), &employee_schema(), None)?
///  // Keep only rows where salary < 1000
///  .filter(col("salary").lt(lit(1000)))?
///  // only show "last_name" in the final results
///  .project(vec![col("last_name")])?
///  .build()?;
///
/// // Convert from plan back to builder
/// let builder = LogicalPlanBuilder::from(plan);
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct LogicalPlanBuilder {
    plan: Arc<LogicalPlan>,
    options: LogicalPlanBuilderOptions,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            plan: Arc::new(plan),
            options: LogicalPlanBuilderOptions::default(),
        }
    }

    /// Create a builder from an existing plan
    pub fn new_from_arc(plan: Arc<LogicalPlan>) -> Self {
        Self {
            plan,
            options: LogicalPlanBuilderOptions::default(),
        }
    }

    pub fn with_options(mut self, options: LogicalPlanBuilderOptions) -> Self {
        self.options = options;
        self
    }

    /// Return the output schema of the plan build so far
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    /// Return the LogicalPlan of the plan build so far
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Create an empty relation.
    ///
    /// `produce_one_row` set to true means this empty node needs to produce a placeholder row.
    pub fn empty(produce_one_row: bool) -> Self {
        Self::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    /// Convert a regular plan into a recursive query.
    /// `is_distinct` indicates whether the recursive term should be de-duplicated (`UNION`) after each iteration or not (`UNION ALL`).
    pub fn to_recursive_query(
        self,
        name: String,
        recursive_term: LogicalPlan,
        is_distinct: bool,
    ) -> Result<Self> {
        // TODO: we need to do a bunch of validation here. Maybe more.
        if is_distinct {
            return not_impl_err!(
                "Recursive queries with a distinct 'UNION' (in which the previous iteration's results will be de-duplicated) is not supported"
            );
        }
        // Ensure that the static term and the recursive term have the same number of fields
        let static_fields_len = self.plan.schema().fields().len();
        let recursive_fields_len = recursive_term.schema().fields().len();
        if static_fields_len != recursive_fields_len {
            return plan_err!(
                "Non-recursive term and recursive term must have the same number of columns ({} != {})",
                static_fields_len, recursive_fields_len
            );
        }
        // Ensure that the recursive term has the same field types as the static term
        let coerced_recursive_term =
            coerce_plan_expr_for_schema(recursive_term, self.plan.schema())?;
        Ok(Self::from(LogicalPlan::RecursiveQuery(RecursiveQuery {
            name,
            static_term: self.plan,
            recursive_term: Arc::new(coerced_recursive_term),
            is_distinct,
        })))
    }

    /// Create a values list based relation, and the schema is inferred from data, consuming
    /// `value`. See the [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    ///
    /// so it's usually better to override the default names with a table alias list.
    ///
    /// If the values include params/binders such as $1, $2, $3, etc, then the `param_data_types` should be provided.
    pub fn values(values: Vec<Vec<Expr>>) -> Result<Self> {
        if values.is_empty() {
            return plan_err!("Values list cannot be empty");
        }
        let n_cols = values[0].len();
        if n_cols == 0 {
            return plan_err!("Values list cannot be zero length");
        }
        for (i, row) in values.iter().enumerate() {
            if row.len() != n_cols {
                return plan_err!(
                    "Inconsistent data length across values list: got {} values in row {} but expected {}",
                    row.len(),
                    i,
                    n_cols
                );
            }
        }

        // Infer from data itself
        Self::infer_data(values)
    }

    /// Create a values list based relation, and the schema is inferred from data itself or table schema if provided, consuming
    /// `value`. See the [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    ///
    /// By default, it assigns the names column1, column2, etc. to the columns of a VALUES table.
    /// The column names are not specified by the SQL standard and different database systems do it differently,
    /// so it's usually better to override the default names with a table alias list.
    ///
    /// If the values include params/binders such as $1, $2, $3, etc, then the `param_data_types` should be provided.
    pub fn values_with_schema(
        values: Vec<Vec<Expr>>,
        schema: &DFSchemaRef,
    ) -> Result<Self> {
        if values.is_empty() {
            return plan_err!("Values list cannot be empty");
        }
        let n_cols = schema.fields().len();
        if n_cols == 0 {
            return plan_err!("Values list cannot be zero length");
        }
        for (i, row) in values.iter().enumerate() {
            if row.len() != n_cols {
                return plan_err!(
                    "Inconsistent data length across values list: got {} values in row {} but expected {}",
                    row.len(),
                    i,
                    n_cols
                );
            }
        }

        // Check the type of value against the schema
        Self::infer_values_from_schema(values, schema)
    }

    fn infer_values_from_schema(
        values: Vec<Vec<Expr>>,
        schema: &DFSchema,
    ) -> Result<Self> {
        let n_cols = values[0].len();
        let mut fields = ValuesFields::new();
        for j in 0..n_cols {
            let field_type = schema.field(j).data_type();
            let field_nullable = schema.field(j).is_nullable();
            for row in values.iter() {
                let value = &row[j];
                let data_type = value.get_type(schema)?;

                if !data_type.equals_datatype(field_type) {
                    if can_cast_types(&data_type, field_type) {
                    } else {
                        return exec_err!(
                            "type mismatch and can't cast to got {} and {}",
                            data_type,
                            field_type
                        );
                    }
                }
            }
            fields.push(field_type.to_owned(), field_nullable);
        }

        Self::infer_inner(values, fields, schema)
    }

    fn infer_data(values: Vec<Vec<Expr>>) -> Result<Self> {
        let n_cols = values[0].len();
        let schema = DFSchema::empty();
        let mut fields = ValuesFields::new();

        for j in 0..n_cols {
            let mut common_type: Option<DataType> = None;
            for (i, row) in values.iter().enumerate() {
                let value = &row[j];
                let data_type = value.get_type(&schema)?;
                if data_type == DataType::Null {
                    continue;
                }

                if let Some(prev_type) = common_type {
                    // get common type of each column values.
                    let data_types = vec![prev_type.clone(), data_type.clone()];
                    let Some(new_type) = type_union_resolution(&data_types) else {
                        return plan_err!("Inconsistent data type across values list at row {i} column {j}. Was {prev_type} but found {data_type}");
                    };
                    common_type = Some(new_type);
                } else {
                    common_type = Some(data_type);
                }
            }
            // assuming common_type was not set, and no error, therefore the type should be NULL
            // since the code loop skips NULL
            fields.push(common_type.unwrap_or(DataType::Null), true);
        }

        Self::infer_inner(values, fields, &schema)
    }

    fn infer_inner(
        mut values: Vec<Vec<Expr>>,
        fields: ValuesFields,
        schema: &DFSchema,
    ) -> Result<Self> {
        let fields = fields.into_fields();
        // wrap cast if data type is not same as common type.
        for row in &mut values {
            for (j, field_type) in fields.iter().map(|f| f.data_type()).enumerate() {
                if let Expr::Literal(ScalarValue::Null, metadata) = &row[j] {
                    row[j] = Expr::Literal(
                        ScalarValue::try_from(field_type)?,
                        metadata.clone(),
                    );
                } else {
                    row[j] = std::mem::take(&mut row[j]).cast_to(field_type, schema)?;
                }
            }
        }

        let dfschema = DFSchema::from_unqualified_fields(fields, HashMap::new())?;
        let schema = DFSchemaRef::new(dfschema);

        Ok(Self::new(LogicalPlan::Values(Values { schema, values })))
    }

    /// Convert a table provider into a builder with a TableScan
    ///
    /// Note that if you pass a string as `table_name`, it is treated
    /// as a SQL identifier, as described on [`TableReference`] and
    /// thus is normalized
    ///
    /// # Example:
    /// ```
    /// # use datafusion_expr::{lit, col, LogicalPlanBuilder,
    /// #  logical_plan::builder::LogicalTableSource, logical_plan::table_scan
    /// # };
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, DataType, Field};
    /// # use datafusion_common::TableReference;
    /// #
    /// # let employee_schema = Arc::new(Schema::new(vec![
    /// #           Field::new("id", DataType::Int32, false),
    /// # ])) as _;
    /// # let table_source = Arc::new(LogicalTableSource::new(employee_schema));
    /// // Scan table_source with the name "mytable" (after normalization)
    /// # let table = table_source.clone();
    /// let scan = LogicalPlanBuilder::scan("MyTable", table, None);
    ///
    /// // Scan table_source with the name "MyTable" by enclosing in quotes
    /// # let table = table_source.clone();
    /// let scan = LogicalPlanBuilder::scan(r#""MyTable""#, table, None);
    ///
    /// // Scan table_source with the name "MyTable" by forming the table reference
    /// # let table = table_source.clone();
    /// let table_reference = TableReference::bare("MyTable");
    /// let scan = LogicalPlanBuilder::scan(table_reference, table, None);
    /// ```
    pub fn scan(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Self::scan_with_filters(table_name, table_source, projection, vec![])
    }

    /// Create a [CopyTo] for copying the contents of this builder to the specified file(s)
    pub fn copy_to(
        input: LogicalPlan,
        output_url: String,
        file_type: Arc<dyn FileType>,
        options: HashMap<String, String>,
        partition_by: Vec<String>,
    ) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Copy(CopyTo::new(
            Arc::new(input),
            output_url,
            partition_by,
            file_type,
            options,
        ))))
    }

    /// Create a [`DmlStatement`] for inserting the contents of this builder into the named table.
    ///
    /// Note,  use a [`DefaultTableSource`] to insert into a [`TableProvider`]
    ///
    /// [`DefaultTableSource`]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/struct.DefaultTableSource.html
    /// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
    ///
    /// # Example:
    /// ```
    /// # use datafusion_expr::{lit, LogicalPlanBuilder,
    /// #  logical_plan::builder::LogicalTableSource,
    /// # };
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, DataType, Field};
    /// # use datafusion_expr::dml::InsertOp;
    /// #
    /// # fn test() -> datafusion_common::Result<()> {
    /// # let employee_schema = Arc::new(Schema::new(vec![
    /// #     Field::new("id", DataType::Int32, false),
    /// # ])) as _;
    /// # let table_source = Arc::new(LogicalTableSource::new(employee_schema));
    /// // VALUES (1), (2)
    /// let input = LogicalPlanBuilder::values(vec![vec![lit(1)], vec![lit(2)]])?
    ///   .build()?;
    /// // INSERT INTO MyTable VALUES (1), (2)
    /// let insert_plan = LogicalPlanBuilder::insert_into(
    ///   input,
    ///   "MyTable",
    ///   table_source,
    ///   InsertOp::Append,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_into(
        input: LogicalPlan,
        table_name: impl Into<TableReference>,
        target: Arc<dyn TableSource>,
        insert_op: InsertOp,
    ) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Dml(DmlStatement::new(
            table_name.into(),
            target,
            WriteOp::Insert(insert_op),
            Arc::new(input),
        ))))
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan_with_filters(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<Self> {
        Self::scan_with_filters_inner(table_name, table_source, projection, filters, None)
    }

    /// Convert a table provider into a builder with a TableScan with filter and fetch
    pub fn scan_with_filters_fetch(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        Self::scan_with_filters_inner(
            table_name,
            table_source,
            projection,
            filters,
            fetch,
        )
    }

    fn scan_with_filters_inner(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let table_scan =
            TableScan::try_new(table_name, table_source, projection, filters, fetch)?;

        // Inline TableScan
        if table_scan.filters.is_empty() {
            if let Some(p) = table_scan.source.get_logical_plan() {
                let sub_plan = p.into_owned();

                if let Some(proj) = table_scan.projection {
                    let projection_exprs = proj
                        .into_iter()
                        .map(|i| {
                            Expr::Column(Column::from(
                                sub_plan.schema().qualified_field(i),
                            ))
                        })
                        .collect::<Vec<_>>();
                    return Self::new(sub_plan)
                        .project(projection_exprs)?
                        .alias(table_scan.table_name);
                }

                // Ensures that the reference to the inlined table remains the
                // same, meaning we don't have to change any of the parent nodes
                // that reference this table.
                return Self::new(sub_plan).alias(table_scan.table_name);
            }
        }

        Ok(Self::new(LogicalPlan::TableScan(table_scan)))
    }

    /// Wrap a plan in a window
    pub fn window_plan(
        input: LogicalPlan,
        window_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<LogicalPlan> {
        let mut plan = input;
        let mut groups = group_window_expr_by_sort_keys(window_exprs)?;
        // To align with the behavior of PostgreSQL, we want the sort_keys sorted as same rule as PostgreSQL that first
        // we compare the sort key themselves and if one window's sort keys are a prefix of another
        // put the window with more sort keys first. so more deeply sorted plans gets nested further down as children.
        // The sort_by() implementation here is a stable sort.
        // Note that by this rule if there's an empty over, it'll be at the top level
        groups.sort_by(|(key_a, _), (key_b, _)| {
            for ((first, _), (second, _)) in key_a.iter().zip(key_b.iter()) {
                let key_ordering = compare_sort_expr(first, second, plan.schema());
                match key_ordering {
                    Ordering::Less => {
                        return Ordering::Less;
                    }
                    Ordering::Greater => {
                        return Ordering::Greater;
                    }
                    Ordering::Equal => {}
                }
            }
            key_b.len().cmp(&key_a.len())
        });
        for (_, exprs) in groups {
            let window_exprs = exprs.into_iter().collect::<Vec<_>>();
            // Partition and sorting is done at physical level, see the EnforceDistribution
            // and EnforceSorting rules.
            plan = LogicalPlanBuilder::from(plan)
                .window(window_exprs)?
                .build()?;
        }
        Ok(plan)
    }

    /// Apply a projection without alias.
    pub fn project(
        self,
        expr: impl IntoIterator<Item = impl Into<SelectExpr>>,
    ) -> Result<Self> {
        project(Arc::unwrap_or_clone(self.plan), expr).map(Self::new)
    }

    /// Apply a projection without alias with optional validation
    /// (true to validate, false to not validate)
    pub fn project_with_validation(
        self,
        expr: Vec<(impl Into<SelectExpr>, bool)>,
    ) -> Result<Self> {
        project_with_validation(Arc::unwrap_or_clone(self.plan), expr).map(Self::new)
    }

    /// Select the given column indices
    pub fn select(self, indices: impl IntoIterator<Item = usize>) -> Result<Self> {
        let exprs: Vec<_> = indices
            .into_iter()
            .map(|x| Expr::Column(Column::from(self.plan.schema().qualified_field(x))))
            .collect();
        self.project(exprs)
    }

    /// Apply a filter
    pub fn filter(self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Filter::try_new(expr, self.plan)
            .map(LogicalPlan::Filter)
            .map(Self::new)
    }

    /// Apply a filter which is used for a having clause
    pub fn having(self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Filter::try_new(expr, self.plan)
            .map(LogicalPlan::Filter)
            .map(Self::from)
    }

    /// Make a builder for a prepare logical plan from the builder's plan
    pub fn prepare(self, name: String, data_types: Vec<DataType>) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Statement(Statement::Prepare(
            Prepare {
                name,
                data_types,
                input: self.plan,
            },
        ))))
    }

    /// Limit the number of rows returned
    ///
    /// `skip` - Number of rows to skip before fetch any row.
    ///
    /// `fetch` - Maximum number of rows to fetch, after skipping `skip` rows,
    ///          if specified.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self> {
        let skip_expr = if skip == 0 {
            None
        } else {
            Some(lit(skip as i64))
        };
        let fetch_expr = fetch.map(|f| lit(f as i64));
        self.limit_by_expr(skip_expr, fetch_expr)
    }

    /// Limit the number of rows returned
    ///
    /// Similar to `limit` but uses expressions for `skip` and `fetch`
    pub fn limit_by_expr(self, skip: Option<Expr>, fetch: Option<Expr>) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Limit(Limit {
            skip: skip.map(Box::new),
            fetch: fetch.map(Box::new),
            input: self.plan,
        })))
    }

    /// Apply an alias
    pub fn alias(self, alias: impl Into<TableReference>) -> Result<Self> {
        subquery_alias(Arc::unwrap_or_clone(self.plan), alias).map(Self::new)
    }

    /// Add missing sort columns to all downstream projection
    ///
    /// Thus, if you have a LogicalPlan that selects A and B and have
    /// not requested a sort by C, this code will add C recursively to
    /// all input projections.
    ///
    /// Adding a new column is not correct if there is a `Distinct`
    /// node, which produces only distinct values of its
    /// inputs. Adding a new column to its input will result in
    /// potentially different results than with the original column.
    ///
    /// For example, if the input is like:
    ///
    /// Distinct(A, B)
    ///
    /// If the input looks like
    ///
    /// a | b | c
    /// --+---+---
    /// 1 | 2 | 3
    /// 1 | 2 | 4
    ///
    /// Distinct (A, B) --> (1,2)
    ///
    /// But Distinct (A, B, C) --> (1, 2, 3), (1, 2, 4)
    ///  (which will appear as a (1, 2), (1, 2) if a and b are projected
    ///
    /// See <https://github.com/apache/datafusion/issues/5065> for more details
    fn add_missing_columns(
        curr_plan: LogicalPlan,
        missing_cols: &IndexSet<Column>,
        is_distinct: bool,
    ) -> Result<LogicalPlan> {
        match curr_plan {
            LogicalPlan::Projection(Projection {
                input,
                mut expr,
                schema: _,
            }) if missing_cols.iter().all(|c| input.schema().has_column(c)) => {
                let mut missing_exprs = missing_cols
                    .iter()
                    .map(|c| normalize_col(Expr::Column(c.clone()), &input))
                    .collect::<Result<Vec<_>>>()?;

                // Do not let duplicate columns to be added, some of the
                // missing_cols may be already present but without the new
                // projected alias.
                missing_exprs.retain(|e| !expr.contains(e));
                if is_distinct {
                    Self::ambiguous_distinct_check(&missing_exprs, missing_cols, &expr)?;
                }
                expr.extend(missing_exprs);
                project(Arc::unwrap_or_clone(input), expr)
            }
            _ => {
                let is_distinct =
                    is_distinct || matches!(curr_plan, LogicalPlan::Distinct(_));
                let new_inputs = curr_plan
                    .inputs()
                    .into_iter()
                    .map(|input_plan| {
                        Self::add_missing_columns(
                            (*input_plan).clone(),
                            missing_cols,
                            is_distinct,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                curr_plan.with_new_exprs(curr_plan.expressions(), new_inputs)
            }
        }
    }

    fn ambiguous_distinct_check(
        missing_exprs: &[Expr],
        missing_cols: &IndexSet<Column>,
        projection_exprs: &[Expr],
    ) -> Result<()> {
        if missing_exprs.is_empty() {
            return Ok(());
        }

        // if the missing columns are all only aliases for things in
        // the existing select list, it is ok
        //
        // This handles the special case for
        // SELECT col as <alias> ORDER BY <alias>
        //
        // As described in https://github.com/apache/datafusion/issues/5293
        let all_aliases = missing_exprs.iter().all(|e| {
            projection_exprs.iter().any(|proj_expr| {
                if let Expr::Alias(boxed_alias) = proj_expr {
                    e == boxed_alias.expr.as_ref()
                } else {
                    false
                }
            })
        });
        if all_aliases {
            return Ok(());
        }

        let missing_col_names = missing_cols
            .iter()
            .map(|col| col.flat_name())
            .collect::<String>();

        plan_err!("For SELECT DISTINCT, ORDER BY expressions {missing_col_names} must appear in select list")
    }

    /// Apply a sort by provided expressions with default direction
    pub fn sort_by(
        self,
        expr: impl IntoIterator<Item = impl Into<Expr>> + Clone,
    ) -> Result<Self> {
        self.sort(
            expr.into_iter()
                .map(|e| e.into().sort(true, false))
                .collect::<Vec<SortExpr>>(),
        )
    }

    pub fn sort(
        self,
        sorts: impl IntoIterator<Item = impl Into<SortExpr>> + Clone,
    ) -> Result<Self> {
        self.sort_with_limit(sorts, None)
    }

    /// Apply a sort
    pub fn sort_with_limit(
        self,
        sorts: impl IntoIterator<Item = impl Into<SortExpr>> + Clone,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let sorts = rewrite_sort_cols_by_aggs(sorts, &self.plan)?;

        let schema = self.plan.schema();

        // Collect sort columns that are missing in the input plan's schema
        let mut missing_cols: IndexSet<Column> = IndexSet::new();
        sorts.iter().try_for_each::<_, Result<()>>(|sort| {
            let columns = sort.expr.column_refs();

            missing_cols.extend(
                columns
                    .into_iter()
                    .filter(|c| !schema.has_column(c))
                    .cloned(),
            );

            Ok(())
        })?;

        if missing_cols.is_empty() {
            return Ok(Self::new(LogicalPlan::Sort(Sort {
                expr: normalize_sorts(sorts, &self.plan)?,
                input: self.plan,
                fetch,
            })));
        }

        // remove pushed down sort columns
        let new_expr = schema.columns().into_iter().map(Expr::Column).collect();

        let is_distinct = false;
        let plan = Self::add_missing_columns(
            Arc::unwrap_or_clone(self.plan),
            &missing_cols,
            is_distinct,
        )?;

        let sort_plan = LogicalPlan::Sort(Sort {
            expr: normalize_sorts(sorts, &plan)?,
            input: Arc::new(plan),
            fetch,
        });

        Projection::try_new(new_expr, Arc::new(sort_plan))
            .map(LogicalPlan::Projection)
            .map(Self::new)
    }

    /// Apply a union, preserving duplicate rows
    pub fn union(self, plan: LogicalPlan) -> Result<Self> {
        union(Arc::unwrap_or_clone(self.plan), plan).map(Self::new)
    }

    /// Apply a union by name, preserving duplicate rows
    pub fn union_by_name(self, plan: LogicalPlan) -> Result<Self> {
        union_by_name(Arc::unwrap_or_clone(self.plan), plan).map(Self::new)
    }

    /// Apply a union by name, removing duplicate rows
    pub fn union_by_name_distinct(self, plan: LogicalPlan) -> Result<Self> {
        let left_plan: LogicalPlan = Arc::unwrap_or_clone(self.plan);
        let right_plan: LogicalPlan = plan;

        Ok(Self::new(LogicalPlan::Distinct(Distinct::All(Arc::new(
            union_by_name(left_plan, right_plan)?,
        )))))
    }

    /// Apply a union, removing duplicate rows
    pub fn union_distinct(self, plan: LogicalPlan) -> Result<Self> {
        let left_plan: LogicalPlan = Arc::unwrap_or_clone(self.plan);
        let right_plan: LogicalPlan = plan;

        Ok(Self::new(LogicalPlan::Distinct(Distinct::All(Arc::new(
            union(left_plan, right_plan)?,
        )))))
    }

    /// Apply deduplication: Only distinct (different) values are returned)
    pub fn distinct(self) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Distinct(Distinct::All(self.plan))))
    }

    /// Project first values of the specified expression list according to the provided
    /// sorting expressions grouped by the `DISTINCT ON` clause expressions.
    pub fn distinct_on(
        self,
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<SortExpr>>,
    ) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Distinct(Distinct::On(
            DistinctOn::try_new(on_expr, select_expr, sort_expr, self.plan)?,
        ))))
    }

    /// Apply a join to `right` using explicitly specified columns and an
    /// optional filter expression.
    ///
    /// See [`join_on`](Self::join_on) for a more concise way to specify the
    /// join condition. Since DataFusion will automatically identify and
    /// optimize equality predicates there is no performance difference between
    /// this function and `join_on`
    ///
    /// `left_cols` and `right_cols` are used to form "equijoin" predicates (see
    /// example below), which are then combined with the optional `filter`
    /// expression.
    ///
    /// Note that in case of outer join, the `filter` is applied to only matched rows.
    pub fn join(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        self.join_detailed(
            right,
            join_type,
            join_keys,
            filter,
            NullEquality::NullEqualsNothing,
        )
    }

    /// Apply a join using the specified expressions.
    ///
    /// Note that DataFusion automatically optimizes joins, including
    /// identifying and optimizing equality predicates.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion_expr::{Expr, col, LogicalPlanBuilder,
    /// #  logical_plan::builder::LogicalTableSource, logical_plan::JoinType,};
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, DataType, Field};
    /// # use datafusion_common::Result;
    /// # fn main() -> Result<()> {
    /// let example_schema = Arc::new(Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Int32, false),
    ///     Field::new("c", DataType::Int32, false),
    /// ]));
    /// let table_source = Arc::new(LogicalTableSource::new(example_schema));
    /// let left_table = table_source.clone();
    /// let right_table = table_source.clone();
    ///
    /// let right_plan = LogicalPlanBuilder::scan("right", right_table, None)?.build()?;
    ///
    /// // Form the expression `(left.a != right.a)` AND `(left.b != right.b)`
    /// let exprs = vec![
    ///     col("left.a").eq(col("right.a")),
    ///     col("left.b").not_eq(col("right.b"))
    ///  ];
    ///
    /// // Perform the equivalent of `left INNER JOIN right ON (a != a2 AND b != b2)`
    /// // finding all pairs of rows from `left` and `right` where
    /// // where `a = a2` and `b != b2`.
    /// let plan = LogicalPlanBuilder::scan("left", left_table, None)?
    ///     .join_on(right_plan, JoinType::Inner, exprs)?
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<Self> {
        let filter = on_exprs.into_iter().reduce(Expr::and);

        self.join_detailed(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter,
            NullEquality::NullEqualsNothing,
        )
    }

    pub(crate) fn normalize(plan: &LogicalPlan, column: Column) -> Result<Column> {
        if column.relation.is_some() {
            // column is already normalized
            return Ok(column);
        }

        let schema = plan.schema();
        let fallback_schemas = plan.fallback_normalize_schemas();
        let using_columns = plan.using_columns()?;
        column.normalize_with_schemas_and_ambiguity_check(
            &[&[schema], &fallback_schemas],
            &using_columns,
        )
    }

    /// Apply a join with on constraint and specified null equality.
    ///
    /// The behavior is the same as [`join`](Self::join) except that it allows
    /// specifying the null equality behavior.
    ///
    /// The `null_equality` dictates how `null` values are joined.
    pub fn join_detailed(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        if join_keys.0.len() != join_keys.1.len() {
            return plan_err!("left_keys and right_keys were not the same length");
        }

        let filter = if let Some(expr) = filter {
            let filter = normalize_col_with_schemas_and_ambiguity_check(
                expr,
                &[&[self.schema(), right.schema()]],
                &[],
            )?;
            Some(filter)
        } else {
            None
        };

        let (left_keys, right_keys): (Vec<Result<Column>>, Vec<Result<Column>>) =
            join_keys
                .0
                .into_iter()
                .zip(join_keys.1)
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
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (Some(lr), None) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);

                            match (l_is_left, l_is_right) {
                                (Ok(_), _) => (Ok(l), Self::normalize(&right, r)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, r), Ok(l)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (None, Some(rr)) => {
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (r_is_left, r_is_right) {
                                (Ok(_), _) => (Ok(r), Self::normalize(&right, l)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (None, None) => {
                            let mut swap = false;
                            let left_key = Self::normalize(&self.plan, l.clone())
                                .or_else(|_| {
                                    swap = true;
                                    Self::normalize(&right, l)
                                });
                            if swap {
                                (Self::normalize(&self.plan, r), left_key)
                            } else {
                                (left_key, Self::normalize(&right, r))
                            }
                        }
                    }
                })
                .unzip();

        let left_keys = left_keys.into_iter().collect::<Result<Vec<Column>>>()?;
        let right_keys = right_keys.into_iter().collect::<Result<Vec<Column>>>()?;

        let on: Vec<_> = left_keys
            .into_iter()
            .zip(right_keys)
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        // Inner type without join condition is cross join
        if join_type != JoinType::Inner && on.is_empty() && filter.is_none() {
            return plan_err!("join condition should not be empty");
        }

        Ok(Self::new(LogicalPlan::Join(Join {
            left: self.plan,
            right: Arc::new(right),
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(join_schema),
            null_equality,
        })))
    }

    /// Apply a join with using constraint, which duplicates all join columns in output schema.
    pub fn join_using(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        using_keys: Vec<Column>,
    ) -> Result<Self> {
        let left_keys: Vec<Column> = using_keys
            .clone()
            .into_iter()
            .map(|c| Self::normalize(&self.plan, c))
            .collect::<Result<_>>()?;
        let right_keys: Vec<Column> = using_keys
            .into_iter()
            .map(|c| Self::normalize(&right, c))
            .collect::<Result<_>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys).collect();
        let mut join_on: Vec<(Expr, Expr)> = vec![];
        let mut filters: Option<Expr> = None;
        for (l, r) in &on {
            if self.plan.schema().has_column(l)
                && right.schema().has_column(r)
                && can_hash(
                    datafusion_common::ExprSchema::field_from_column(
                        self.plan.schema(),
                        l,
                    )?
                    .data_type(),
                )
            {
                join_on.push((Expr::Column(l.clone()), Expr::Column(r.clone())));
            } else if self.plan.schema().has_column(l)
                && right.schema().has_column(r)
                && can_hash(
                    datafusion_common::ExprSchema::field_from_column(
                        self.plan.schema(),
                        r,
                    )?
                    .data_type(),
                )
            {
                join_on.push((Expr::Column(r.clone()), Expr::Column(l.clone())));
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
            let join = Self::from(self.plan).cross_join(right)?;
            join.filter(filters.ok_or_else(|| {
                DataFusionError::Internal("filters should not be None here".to_string())
            })?)
        } else {
            let join = Join::try_new(
                self.plan,
                Arc::new(right),
                join_on,
                filters,
                join_type,
                JoinConstraint::Using,
                NullEquality::NullEqualsNothing,
            )?;

            Ok(Self::new(LogicalPlan::Join(join)))
        }
    }

    /// Apply a cross join
    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let join = Join::try_new(
            self.plan,
            Arc::new(right),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?;

        Ok(Self::new(LogicalPlan::Join(join)))
    }

    /// Repartition
    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<Self> {
        Ok(Self::new(LogicalPlan::Repartition(Repartition {
            input: self.plan,
            partitioning_scheme,
        })))
    }

    /// Apply a window functions to extend the schema
    pub fn window(
        self,
        window_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let window_expr = normalize_cols(window_expr, &self.plan)?;
        validate_unique_names("Windows", &window_expr)?;
        Ok(Self::new(LogicalPlan::Window(Window::try_new(
            window_expr,
            self.plan,
        )?)))
    }

    /// Apply an aggregate: grouping on the `group_expr` expressions
    /// and calculating `aggr_expr` aggregates for each distinct
    /// value of the `group_expr`;
    pub fn aggregate(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;

        let group_expr = if self.options.add_implicit_group_by_exprs {
            add_group_by_exprs_from_dependencies(group_expr, self.plan.schema())?
        } else {
            group_expr
        };

        Aggregate::try_new(self.plan, group_expr, aggr_expr)
            .map(LogicalPlan::Aggregate)
            .map(Self::new)
    }

    /// Create an expression to represent the explanation of the plan
    ///
    /// if `analyze` is true, runs the actual plan and produces
    /// information about metrics during run.
    ///
    /// if `verbose` is true, prints out additional details.
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<Self> {
        // Keep the format default to Indent
        self.explain_option_format(
            ExplainOption::default()
                .with_verbose(verbose)
                .with_analyze(analyze),
        )
    }

    /// Create an expression to represent the explanation of the plan
    /// The`explain_option` is used to specify the format and verbosity of the explanation.
    /// Details see [`ExplainOption`].
    pub fn explain_option_format(self, explain_option: ExplainOption) -> Result<Self> {
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if explain_option.analyze {
            Ok(Self::new(LogicalPlan::Analyze(Analyze {
                verbose: explain_option.verbose,
                input: self.plan,
                schema,
            })))
        } else {
            let stringified_plans =
                vec![self.plan.to_stringified(PlanType::InitialLogicalPlan)];

            Ok(Self::new(LogicalPlan::Explain(Explain {
                verbose: explain_option.verbose,
                plan: self.plan,
                explain_format: explain_option.format,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
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
            return plan_err!(
                "INTERSECT/EXCEPT query must have the same number of columns. Left is {left_len} and right is {right_len}."
            );
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
                .join_detailed(
                    right_plan,
                    join_type,
                    join_keys,
                    None,
                    NullEquality::NullEqualsNull,
                )?
                .build()
        } else {
            LogicalPlanBuilder::from(left_plan)
                .distinct()?
                .join_detailed(
                    right_plan,
                    join_type,
                    join_keys,
                    None,
                    NullEquality::NullEqualsNull,
                )?
                .build()
        }
    }

    /// Build the plan
    pub fn build(self) -> Result<LogicalPlan> {
        Ok(Arc::unwrap_or_clone(self.plan))
    }

    /// Apply a join with both explicit equijoin and non equijoin predicates.
    ///
    /// Note this is a low level API that requires identifying specific
    /// predicate types. Most users should use  [`join_on`](Self::join_on) that
    /// automatically identifies predicates appropriately.
    ///
    /// `equi_exprs` defines equijoin predicates, of the form `l = r)` for each
    /// `(l, r)` tuple. `l`, the first element of the tuple, must only refer
    /// to columns from the existing input. `r`, the second element of the tuple,
    /// must only refer to columns from the right input.
    ///
    /// `filter` contains any other filter expression to apply during the
    /// join. Note that `equi_exprs` predicates are evaluated more efficiently
    /// than the filter expressions, so they are preferred.
    pub fn join_with_expr_keys(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        equi_exprs: (Vec<impl Into<Expr>>, Vec<impl Into<Expr>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        if equi_exprs.0.len() != equi_exprs.1.len() {
            return plan_err!("left_keys and right_keys were not the same length");
        }

        let join_key_pairs = equi_exprs
            .0
            .into_iter()
            .zip(equi_exprs.1)
            .map(|(l, r)| {
                let left_key = l.into();
                let right_key = r.into();
                let mut left_using_columns  = HashSet::new();
                expr_to_columns(&left_key, &mut left_using_columns)?;
                let normalized_left_key = normalize_col_with_schemas_and_ambiguity_check(
                    left_key,
                    &[&[self.plan.schema()]],
                    &[],
                )?;

                let mut right_using_columns = HashSet::new();
                expr_to_columns(&right_key, &mut right_using_columns)?;
                let normalized_right_key = normalize_col_with_schemas_and_ambiguity_check(
                    right_key,
                    &[&[right.schema()]],
                    &[],
                )?;

                // find valid equijoin
                find_valid_equijoin_key_pair(
                        &normalized_left_key,
                        &normalized_right_key,
                        self.plan.schema(),
                        right.schema(),
                    )?.ok_or_else(||
                        plan_datafusion_err!(
                            "can't create join plan, join key should belong to one input, error key: ({normalized_left_key},{normalized_right_key})"
                        ))
            })
            .collect::<Result<Vec<_>>>()?;

        let join = Join::try_new(
            self.plan,
            Arc::new(right),
            join_key_pairs,
            filter,
            join_type,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?;

        Ok(Self::new(LogicalPlan::Join(join)))
    }

    /// Unnest the given column.
    pub fn unnest_column(self, column: impl Into<Column>) -> Result<Self> {
        unnest(Arc::unwrap_or_clone(self.plan), vec![column.into()]).map(Self::new)
    }

    /// Unnest the given column given [`UnnestOptions`]
    pub fn unnest_column_with_options(
        self,
        column: impl Into<Column>,
        options: UnnestOptions,
    ) -> Result<Self> {
        unnest_with_options(
            Arc::unwrap_or_clone(self.plan),
            vec![column.into()],
            options,
        )
        .map(Self::new)
    }

    /// Unnest the given columns with the given [`UnnestOptions`]
    pub fn unnest_columns_with_options(
        self,
        columns: Vec<Column>,
        options: UnnestOptions,
    ) -> Result<Self> {
        unnest_with_options(Arc::unwrap_or_clone(self.plan), columns, options)
            .map(Self::new)
    }
}

impl From<LogicalPlan> for LogicalPlanBuilder {
    fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder::new(plan)
    }
}

impl From<Arc<LogicalPlan>> for LogicalPlanBuilder {
    fn from(plan: Arc<LogicalPlan>) -> Self {
        LogicalPlanBuilder::new_from_arc(plan)
    }
}

/// Container used when building fields for a `VALUES` node.
#[derive(Default)]
struct ValuesFields {
    inner: Vec<Field>,
}

impl ValuesFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, data_type: DataType, nullable: bool) {
        // Naming follows the convention described here:
        // https://www.postgresql.org/docs/current/queries-values.html
        let name = format!("column{}", self.inner.len() + 1);
        self.inner.push(Field::new(name, data_type, nullable));
    }

    pub fn into_fields(self) -> Fields {
        self.inner.into()
    }
}

// `name_map` tracks a mapping between a field name and the number of appearances of that field.
//
// Some field names might already come to this function with the count (number of times it appeared)
// as a sufix e.g. id:1, so there's still a chance of name collisions, for example,
// if these three fields passed to this function: "col:1", "col" and "col", the function
// would rename them to -> col:1, col, col:1 causing a posteriror error when building the DFSchema.
// that's why we need the `seen` set, so the fields are always unique.
//
pub fn change_redundant_column(fields: &Fields) -> Vec<Field> {
    let mut name_map = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();

    fields
        .into_iter()
        .map(|field| {
            let base_name = field.name();
            let count = name_map.entry(base_name.clone()).or_insert(0);
            let mut new_name = base_name.clone();

            // Loop until we find a name that hasn't been used
            while seen.contains(&new_name) {
                *count += 1;
                new_name = format!("{base_name}:{count}");
            }

            seen.insert(new_name.clone());

            let mut modified_field =
                Field::new(&new_name, field.data_type().clone(), field.is_nullable());
            modified_field.set_metadata(field.metadata().clone());
            modified_field
        })
        .collect()
}

fn mark_field(schema: &DFSchema) -> (Option<TableReference>, Arc<Field>) {
    let mut table_references = schema
        .iter()
        .filter_map(|(qualifier, _)| qualifier)
        .collect::<Vec<_>>();
    table_references.dedup();
    let table_reference = if table_references.len() == 1 {
        table_references.pop().cloned()
    } else {
        None
    };

    (
        table_reference,
        Arc::new(Field::new("mark", DataType::Boolean, false)),
    )
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &DFSchema,
    right: &DFSchema,
    join_type: &JoinType,
) -> Result<DFSchema> {
    fn nullify_fields<'a>(
        fields: impl Iterator<Item = (Option<&'a TableReference>, &'a Arc<Field>)>,
    ) -> Vec<(Option<TableReference>, Arc<Field>)> {
        fields
            .map(|(q, f)| {
                // TODO: find a good way to do that
                let field = f.as_ref().clone().with_nullable(true);
                (q.cloned(), Arc::new(field))
            })
            .collect()
    }

    let right_fields = right.iter();
    let left_fields = left.iter();

    let qualified_fields: Vec<(Option<TableReference>, Arc<Field>)> = match join_type {
        JoinType::Inner => {
            // left then right
            let left_fields = left_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect::<Vec<_>>();
            let right_fields = right_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect::<Vec<_>>();
            left_fields.into_iter().chain(right_fields).collect()
        }
        JoinType::Left => {
            // left then right, right set to nullable in case of not matched scenario
            let left_fields = left_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect::<Vec<_>>();
            left_fields
                .into_iter()
                .chain(nullify_fields(right_fields))
                .collect()
        }
        JoinType::Right => {
            // left then right, left set to nullable in case of not matched scenario
            let right_fields = right_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect::<Vec<_>>();
            nullify_fields(left_fields)
                .into_iter()
                .chain(right_fields)
                .collect()
        }
        JoinType::Full => {
            // left then right, all set to nullable in case of not matched scenario
            nullify_fields(left_fields)
                .into_iter()
                .chain(nullify_fields(right_fields))
                .collect()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // Only use the left side for the schema
            left_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect()
        }
        JoinType::LeftMark => left_fields
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .chain(once(mark_field(right)))
            .collect(),
        JoinType::RightSemi | JoinType::RightAnti => {
            // Only use the right side for the schema
            right_fields
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect()
        }
        JoinType::RightMark => right_fields
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .chain(once(mark_field(left)))
            .collect(),
    };
    let func_dependencies = left.functional_dependencies().join(
        right.functional_dependencies(),
        join_type,
        left.fields().len(),
    );

    let (schema1, schema2) = match join_type {
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => (left, right),
        _ => (right, left),
    };

    let metadata = schema1
        .metadata()
        .clone()
        .into_iter()
        .chain(schema2.metadata().clone())
        .collect();

    let dfschema = DFSchema::new_with_metadata(qualified_fields, metadata)?;
    dfschema.with_functional_dependencies(func_dependencies)
}

/// (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
/// conflict with the columns from the other.
/// This is especially useful for queries that come as Substrait, since Substrait doesn't currently allow specifying
/// aliases, neither for columns nor for tables.  DataFusion requires columns to be uniquely identifiable, in some
/// places (see e.g. DFSchema::check_names).
/// The function returns:
/// - The requalified or original left logical plan
/// - The requalified or original right logical plan
/// - If a requalification was needed or not
pub fn requalify_sides_if_needed(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
) -> Result<(LogicalPlanBuilder, LogicalPlanBuilder, bool)> {
    let left_cols = left.schema().columns();
    let right_cols = right.schema().columns();
    if left_cols.iter().any(|l| {
        right_cols.iter().any(|r| {
            l == r || (l.name == r.name && (l.relation.is_none() || r.relation.is_none()))
        })
    }) {
        // These names have no connection to the original plan, but they'll make the columns
        // (mostly) unique.
        Ok((
            left.alias(TableReference::bare("left"))?,
            right.alias(TableReference::bare("right"))?,
            true,
        ))
    } else {
        Ok((left, right, false))
    }
}

/// Add additional "synthetic" group by expressions based on functional
/// dependencies.
///
/// For example, if we are grouping on `[c1]`, and we know from
/// functional dependencies that column `c1` determines `c2`, this function
/// adds `c2` to the group by list.
///
/// This allows MySQL style selects like
/// `SELECT col FROM t WHERE pk = 5` if col is unique
pub fn add_group_by_exprs_from_dependencies(
    mut group_expr: Vec<Expr>,
    schema: &DFSchemaRef,
) -> Result<Vec<Expr>> {
    // Names of the fields produced by the GROUP BY exprs for example, `GROUP BY
    // c1 + 1` produces an output field named `"c1 + 1"`
    let mut group_by_field_names = group_expr
        .iter()
        .map(|e| e.schema_name().to_string())
        .collect::<Vec<_>>();

    if let Some(target_indices) =
        get_target_functional_dependencies(schema, &group_by_field_names)
    {
        for idx in target_indices {
            let expr = Expr::Column(Column::from(schema.qualified_field(idx)));
            let expr_name = expr.schema_name().to_string();
            if !group_by_field_names.contains(&expr_name) {
                group_by_field_names.push(expr_name);
                group_expr.push(expr);
            }
        }
    }
    Ok(group_expr)
}

/// Errors if one or more expressions have equal names.
pub fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let mut unique_names = HashMap::new();

    expressions.into_iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.schema_name().to_string();
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                plan_err!("{node_name} require unique expression names \
                             but the expression \"{existing_expr}\" at position {existing_position} and \"{expr}\" \
                             at position {position} have the same name. Consider aliasing (\"AS\") one of them."
                            )
            }
        }
    })
}

/// Union two [`LogicalPlan`]s.
///
/// Constructs the UNION plan, but does not perform type-coercion. Therefore the
/// subtree expressions will not be properly typed until the optimizer pass.
///
/// If a properly typed UNION plan is needed, refer to [`TypeCoercionRewriter::coerce_union`]
/// or alternatively, merge the union input schema using [`coerce_union_schema`] and
/// apply the expression rewrite with [`coerce_plan_expr_for_schema`].
///
/// [`TypeCoercionRewriter::coerce_union`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/analyzer/type_coercion/struct.TypeCoercionRewriter.html#method.coerce_union
/// [`coerce_union_schema`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/analyzer/type_coercion/fn.coerce_union_schema.html
pub fn union(left_plan: LogicalPlan, right_plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Union(Union::try_new_with_loose_types(vec![
        Arc::new(left_plan),
        Arc::new(right_plan),
    ])?))
}

/// Like [`union`], but combine rows from different tables by name, rather than
/// by position.
pub fn union_by_name(
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Union(Union::try_new_by_name(vec![
        Arc::new(left_plan),
        Arc::new(right_plan),
    ])?))
}

/// Create Projection
/// # Errors
/// This function errors under any of the following conditions:
/// * Two or more expressions have the same name
/// * An invalid expression is used (e.g. a `sort` expression)
pub fn project(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<SelectExpr>>,
) -> Result<LogicalPlan> {
    project_with_validation(plan, expr.into_iter().map(|e| (e, true)))
}

/// Create Projection. Similar to project except that the expressions
/// passed in have a flag to indicate if that expression requires
/// validation (normalize & columnize) (true) or not (false)
/// # Errors
/// This function errors under any of the following conditions:
/// * Two or more expressions have the same name
/// * An invalid expression is used (e.g. a `sort` expression)
fn project_with_validation(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = (impl Into<SelectExpr>, bool)>,
) -> Result<LogicalPlan> {
    let mut projected_expr = vec![];
    for (e, validate) in expr {
        let e = e.into();
        match e {
            SelectExpr::Wildcard(opt) => {
                let expanded = expand_wildcard(plan.schema(), &plan, Some(&opt))?;

                // If there is a REPLACE statement, replace that column with the given
                // replace expression. Column name remains the same.
                let expanded = if let Some(replace) = opt.replace {
                    replace_columns(expanded, &replace)?
                } else {
                    expanded
                };

                for e in expanded {
                    if validate {
                        projected_expr
                            .push(columnize_expr(normalize_col(e, &plan)?, &plan)?)
                    } else {
                        projected_expr.push(e)
                    }
                }
            }
            SelectExpr::QualifiedWildcard(table_ref, opt) => {
                let expanded =
                    expand_qualified_wildcard(&table_ref, plan.schema(), Some(&opt))?;

                // If there is a REPLACE statement, replace that column with the given
                // replace expression. Column name remains the same.
                let expanded = if let Some(replace) = opt.replace {
                    replace_columns(expanded, &replace)?
                } else {
                    expanded
                };

                for e in expanded {
                    if validate {
                        projected_expr
                            .push(columnize_expr(normalize_col(e, &plan)?, &plan)?)
                    } else {
                        projected_expr.push(e)
                    }
                }
            }
            SelectExpr::Expression(e) => {
                if validate {
                    projected_expr.push(columnize_expr(normalize_col(e, &plan)?, &plan)?)
                } else {
                    projected_expr.push(e)
                }
            }
        }
    }
    validate_unique_names("Projections", projected_expr.iter())?;

    Projection::try_new(projected_expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

/// If there is a REPLACE statement in the projected expression in the form of
/// "REPLACE (some_column_within_an_expr AS some_column)", this function replaces
/// that column with the given replace expression. Column name remains the same.
/// Multiple REPLACEs are also possible with comma separations.
fn replace_columns(
    mut exprs: Vec<Expr>,
    replace: &PlannedReplaceSelectItem,
) -> Result<Vec<Expr>> {
    for expr in exprs.iter_mut() {
        if let Expr::Column(Column { name, .. }) = expr {
            if let Some((_, new_expr)) = replace
                .items()
                .iter()
                .zip(replace.expressions().iter())
                .find(|(item, _)| item.column_name.value == *name)
            {
                *expr = new_expr.clone().alias(name.clone())
            }
        }
    }
    Ok(exprs)
}

/// Create a SubqueryAlias to wrap a LogicalPlan.
pub fn subquery_alias(
    plan: LogicalPlan,
    alias: impl Into<TableReference>,
) -> Result<LogicalPlan> {
    SubqueryAlias::try_new(Arc::new(plan), alias).map(LogicalPlan::SubqueryAlias)
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
/// This is mostly used for testing and documentation.
pub fn table_scan(
    name: Option<impl Into<TableReference>>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    table_scan_with_filters(name, table_schema, projection, vec![])
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
/// and inlined filters.
/// This is mostly used for testing and documentation.
pub fn table_scan_with_filters(
    name: Option<impl Into<TableReference>>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
) -> Result<LogicalPlanBuilder> {
    let table_source = table_source(table_schema);
    let name = name
        .map(|n| n.into())
        .unwrap_or_else(|| TableReference::bare(UNNAMED_TABLE));
    LogicalPlanBuilder::scan_with_filters(name, table_source, projection, filters)
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
/// filters, and inlined fetch.
/// This is mostly used for testing and documentation.
pub fn table_scan_with_filter_and_fetch(
    name: Option<impl Into<TableReference>>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
) -> Result<LogicalPlanBuilder> {
    let table_source = table_source(table_schema);
    let name = name
        .map(|n| n.into())
        .unwrap_or_else(|| TableReference::bare(UNNAMED_TABLE));
    LogicalPlanBuilder::scan_with_filters_fetch(
        name,
        table_source,
        projection,
        filters,
        fetch,
    )
}

pub fn table_source(table_schema: &Schema) -> Arc<dyn TableSource> {
    let table_schema = Arc::new(table_schema.clone());
    Arc::new(LogicalTableSource {
        table_schema,
        constraints: Default::default(),
    })
}

pub fn table_source_with_constraints(
    table_schema: &Schema,
    constraints: Constraints,
) -> Arc<dyn TableSource> {
    let table_schema = Arc::new(table_schema.clone());
    Arc::new(LogicalTableSource {
        table_schema,
        constraints,
    })
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
            // If we do not add alias, it will throw same field name error in the schema when adding projection.
            // For example:
            //    input scan : [a, b, c],
            //    join keys: [cast(a as int)]
            //
            //  then a and cast(a as int) will use the same field name - `a` in projection schema.
            //  https://github.com/apache/datafusion/issues/4478
            if matches!(key, Expr::Cast(_)) || matches!(key, Expr::TryCast(_)) {
                let alias = format!("{key}");
                key.clone().alias(alias)
            } else {
                key.clone()
            }
        })
        .collect::<Vec<_>>();

    let need_project = join_keys.iter().any(|key| !matches!(key, Expr::Column(_)));
    let plan = if need_project {
        // Include all columns from the input and extend them with the join keys
        let mut projection = input_schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let join_key_items = alias_join_keys
            .iter()
            .flat_map(|expr| expr.try_as_col().is_none().then_some(expr))
            .cloned()
            .collect::<HashSet<Expr>>();
        projection.extend(join_key_items);

        LogicalPlanBuilder::from(input)
            .project(projection.into_iter().map(SelectExpr::from))?
            .build()?
    } else {
        input
    };

    let join_on = alias_join_keys
        .into_iter()
        .map(|key| {
            if let Some(col) = key.try_as_col() {
                Ok(col.clone())
            } else {
                let name = key.schema_name().to_string();
                Ok(Column::from_name(name))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((plan, join_on, need_project))
}

/// Basic TableSource implementation intended for use in tests and documentation. It is expected
/// that users will provide their own TableSource implementations or use DataFusion's
/// DefaultTableSource.
pub struct LogicalTableSource {
    table_schema: SchemaRef,
    constraints: Constraints,
}

impl LogicalTableSource {
    /// Create a new LogicalTableSource
    pub fn new(table_schema: SchemaRef) -> Self {
        Self {
            table_schema,
            constraints: Constraints::default(),
        }
    }

    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }
}

impl TableSource for LogicalTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// Create a [`LogicalPlan::Unnest`] plan
pub fn unnest(input: LogicalPlan, columns: Vec<Column>) -> Result<LogicalPlan> {
    unnest_with_options(input, columns, UnnestOptions::default())
}

pub fn get_struct_unnested_columns(
    col_name: &String,
    inner_fields: &Fields,
) -> Vec<Column> {
    inner_fields
        .iter()
        .map(|f| Column::from_name(format!("{}.{}", col_name, f.name())))
        .collect()
}

/// Create a [`LogicalPlan::Unnest`] plan with options
/// This function receive a list of columns to be unnested
/// because multiple unnest can be performed on the same column (e.g unnest with different depth)
/// The new schema will contains post-unnest fields replacing the original field
///
/// For example:
/// Input schema as
/// ```text
/// +---------------------+-------------------+
/// | col1                | col2              |
/// +---------------------+-------------------+
/// | Struct(INT64,INT32) | List(List(Int64)) |
/// +---------------------+-------------------+
/// ```
///
///
///
/// Then unnesting columns with:
/// - (col1,Struct)
/// - (col2,List(\[depth=1,depth=2\]))
///
/// will generate a new schema as
/// ```text
/// +---------+---------+---------------------+---------------------+
/// | col1.c0 | col1.c1 | unnest_col2_depth_1 | unnest_col2_depth_2 |
/// +---------+---------+---------------------+---------------------+
/// | Int64   | Int32   | List(Int64)         |  Int64              |
/// +---------+---------+---------------------+---------------------+
/// ```
pub fn unnest_with_options(
    input: LogicalPlan,
    columns_to_unnest: Vec<Column>,
    options: UnnestOptions,
) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Unnest(Unnest::try_new(
        Arc::new(input),
        columns_to_unnest,
        options,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::StringifiedPlan;
    use crate::{col, expr, expr_fn::exists, in_subquery, lit, scalar_subquery};

    use crate::test::function_stub::sum;
    use datafusion_common::{Constraint, RecursionUnnestOption, SchemaError};
    use insta::assert_snapshot;

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .filter(col("state").eq(lit("CO")))?
                .project(vec![col("id")])?
                .build()?;

        assert_snapshot!(plan, @r#"
        Projection: employee_csv.id
          Filter: employee_csv.state = Utf8("CO")
            TableScan: employee_csv projection=[id, state]
        "#);

        Ok(())
    }

    #[test]
    fn plan_builder_schema() {
        let schema = employee_schema();
        let projection = None;
        let plan =
            LogicalPlanBuilder::scan("employee_csv", table_source(&schema), projection)
                .unwrap();
        assert_snapshot!(plan.schema().as_ref(), @"fields:[employee_csv.id, employee_csv.first_name, employee_csv.last_name, employee_csv.state, employee_csv.salary], metadata:{}");

        // Note scan of "EMPLOYEE_CSV" is treated as a SQL identifier
        // (and thus normalized to "employee"csv") as well
        let projection = None;
        let plan =
            LogicalPlanBuilder::scan("EMPLOYEE_CSV", table_source(&schema), projection)
                .unwrap();
        assert_snapshot!(plan.schema().as_ref(), @"fields:[employee_csv.id, employee_csv.first_name, employee_csv.last_name, employee_csv.state, employee_csv.salary], metadata:{}");
    }

    #[test]
    fn plan_builder_empty_name() {
        let schema = employee_schema();
        let projection = None;
        let err =
            LogicalPlanBuilder::scan("", table_source(&schema), projection).unwrap_err();
        assert_snapshot!(
            err.strip_backtrace(),
            @"Error during planning: table_name cannot be empty"
        );
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .sort(vec![
                    expr::Sort::new(col("state"), true, true),
                    expr::Sort::new(col("salary"), false, false),
                ])?
                .build()?;

        assert_snapshot!(plan, @r"
        Sort: employee_csv.state ASC NULLS FIRST, employee_csv.salary DESC NULLS LAST
          TableScan: employee_csv projection=[state, salary]
        ");

        Ok(())
    }

    #[test]
    fn plan_builder_union() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .clone()
            .union(plan.clone().build()?)?
            .union(plan.clone().build()?)?
            .union(plan.build()?)?
            .build()?;

        assert_snapshot!(plan, @r"
        Union
          Union
            Union
              TableScan: employee_csv projection=[state, salary]
              TableScan: employee_csv projection=[state, salary]
            TableScan: employee_csv projection=[state, salary]
          TableScan: employee_csv projection=[state, salary]
        ");

        Ok(())
    }

    #[test]
    fn plan_builder_union_distinct() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .clone()
            .union_distinct(plan.clone().build()?)?
            .union_distinct(plan.clone().build()?)?
            .union_distinct(plan.build()?)?
            .build()?;

        assert_snapshot!(plan, @r"
        Distinct:
          Union
            Distinct:
              Union
                Distinct:
                  Union
                    TableScan: employee_csv projection=[state, salary]
                    TableScan: employee_csv projection=[state, salary]
                TableScan: employee_csv projection=[state, salary]
            TableScan: employee_csv projection=[state, salary]
        ");

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

        assert_snapshot!(plan, @r#"
        Distinct:
          Projection: employee_csv.id
            Filter: employee_csv.state = Utf8("CO")
              TableScan: employee_csv projection=[id, state]
        "#);

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

        assert_snapshot!(outer_query, @r"
        Filter: EXISTS (<subquery>)
          Subquery:
            Filter: foo.a = bar.a
              Projection: foo.a
                TableScan: foo
          Projection: bar.a
            TableScan: bar
        ");

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

        assert_snapshot!(outer_query, @r"
        Filter: bar.a IN (<subquery>)
          Subquery:
            Filter: foo.a = bar.a
              Projection: foo.a
                TableScan: foo
          Projection: bar.a
            TableScan: bar
        ");

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

        assert_snapshot!(outer_query, @r"
        Projection: (<subquery>)
          Subquery:
            Filter: foo.a = bar.a
              Projection: foo.b
                TableScan: foo
          TableScan: bar
        ");

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
            Err(DataFusionError::SchemaError(err, _)) => {
                if let SchemaError::AmbiguousReference { field } = *err {
                    let Column {
                        relation,
                        name,
                        spans: _,
                    } = *field;
                    let Some(TableReference::Bare { table }) = relation else {
                        return plan_err!(
                            "wrong relation: {relation:?}, expected table name"
                        );
                    };
                    assert_eq!(*"employee_csv", *table);
                    assert_eq!("id", &name);
                    Ok(())
                } else {
                    plan_err!("Plan should have returned an DataFusionError::SchemaError")
                }
            }
            _ => plan_err!("Plan should have returned an DataFusionError::SchemaError"),
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
        let plan1 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3]))?;
        let plan2 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3, 4]))?;

        let err_msg1 =
            LogicalPlanBuilder::intersect(plan1.build()?, plan2.build()?, true)
                .unwrap_err();

        assert_snapshot!(err_msg1.strip_backtrace(), @"Error during planning: INTERSECT/EXCEPT query must have the same number of columns. Left is 1 and right is 2.");

        Ok(())
    }

    #[test]
    fn plan_builder_unnest() -> Result<()> {
        // Cannot unnest on a scalar column
        let err = nested_table_scan("test_table")?
            .unnest_column("scalar")
            .unwrap_err();

        let DataFusionError::Internal(desc) = err else {
            return plan_err!("Plan should have returned an DataFusionError::Internal");
        };

        let desc = desc
            .split(DataFusionError::BACK_TRACE_SEP)
            .collect::<Vec<&str>>()
            .first()
            .unwrap_or(&"")
            .to_string();

        assert_snapshot!(desc, @"trying to unnest on invalid data type UInt32");

        // Unnesting the strings list.
        let plan = nested_table_scan("test_table")?
            .unnest_column("strings")?
            .build()?;

        assert_snapshot!(plan, @r"
        Unnest: lists[test_table.strings|depth=1] structs[]
          TableScan: test_table
        ");

        // Check unnested field is a scalar
        let field = plan.schema().field_with_name(None, "strings").unwrap();
        assert_eq!(&DataType::Utf8, field.data_type());

        // Unnesting the singular struct column result into 2 new columns for each subfield
        let plan = nested_table_scan("test_table")?
            .unnest_column("struct_singular")?
            .build()?;

        assert_snapshot!(plan, @r"
        Unnest: lists[] structs[test_table.struct_singular]
          TableScan: test_table
        ");

        for field_name in &["a", "b"] {
            // Check unnested struct field is a scalar
            let field = plan
                .schema()
                .field_with_name(None, &format!("struct_singular.{field_name}"))
                .unwrap();
            assert_eq!(&DataType::UInt32, field.data_type());
        }

        // Unnesting multiple fields in separate plans
        let plan = nested_table_scan("test_table")?
            .unnest_column("strings")?
            .unnest_column("structs")?
            .unnest_column("struct_singular")?
            .build()?;

        assert_snapshot!(plan, @r"
        Unnest: lists[] structs[test_table.struct_singular]
          Unnest: lists[test_table.structs|depth=1] structs[]
            Unnest: lists[test_table.strings|depth=1] structs[]
              TableScan: test_table
        ");

        // Check unnested struct list field should be a struct.
        let field = plan.schema().field_with_name(None, "structs").unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));

        // Unnesting multiple fields at the same time, using infer syntax
        let cols = vec!["strings", "structs", "struct_singular"]
            .into_iter()
            .map(|c| c.into())
            .collect();

        let plan = nested_table_scan("test_table")?
            .unnest_columns_with_options(cols, UnnestOptions::default())?
            .build()?;

        assert_snapshot!(plan, @r"
        Unnest: lists[test_table.strings|depth=1, test_table.structs|depth=1] structs[test_table.struct_singular]
          TableScan: test_table
        ");

        // Unnesting missing column should fail.
        let plan = nested_table_scan("test_table")?.unnest_column("missing");
        assert!(plan.is_err());

        // Simultaneously unnesting a list (with different depth) and a struct column
        let plan = nested_table_scan("test_table")?
            .unnest_columns_with_options(
                vec!["stringss".into(), "struct_singular".into()],
                UnnestOptions::default()
                    .with_recursions(RecursionUnnestOption {
                        input_column: "stringss".into(),
                        output_column: "stringss_depth_1".into(),
                        depth: 1,
                    })
                    .with_recursions(RecursionUnnestOption {
                        input_column: "stringss".into(),
                        output_column: "stringss_depth_2".into(),
                        depth: 2,
                    }),
            )?
            .build()?;

        assert_snapshot!(plan, @r"
        Unnest: lists[test_table.stringss|depth=1, test_table.stringss|depth=2] structs[test_table.struct_singular]
          TableScan: test_table
        ");

        // Check output columns has correct type
        let field = plan
            .schema()
            .field_with_name(None, "stringss_depth_1")
            .unwrap();
        assert_eq!(
            &DataType::new_list(DataType::Utf8, false),
            field.data_type()
        );
        let field = plan
            .schema()
            .field_with_name(None, "stringss_depth_2")
            .unwrap();
        assert_eq!(&DataType::Utf8, field.data_type());
        // unnesting struct is still correct
        for field_name in &["a", "b"] {
            let field = plan
                .schema()
                .field_with_name(None, &format!("struct_singular.{field_name}"))
                .unwrap();
            assert_eq!(&DataType::UInt32, field.data_type());
        }

        Ok(())
    }

    fn nested_table_scan(table_name: &str) -> Result<LogicalPlanBuilder> {
        // Create a schema with a scalar field, a list of strings, a list of structs
        // and a singular struct
        let struct_field_in_list = Field::new_struct(
            "item",
            vec![
                Field::new("a", DataType::UInt32, false),
                Field::new("b", DataType::UInt32, false),
            ],
            false,
        );
        let string_field = Field::new_list_field(DataType::Utf8, false);
        let strings_field = Field::new_list("item", string_field.clone(), false);
        let schema = Schema::new(vec![
            Field::new("scalar", DataType::UInt32, false),
            Field::new_list("strings", string_field, false),
            Field::new_list("structs", struct_field_in_list, false),
            Field::new(
                "struct_singular",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::UInt32, false),
                    Field::new("b", DataType::UInt32, false),
                ])),
                false,
            ),
            Field::new_list("stringss", strings_field, false),
        ]);

        table_scan(Some(table_name), &schema, None)
    }

    #[test]
    fn test_union_after_join() -> Result<()> {
        let values = vec![vec![lit(1)]];

        let left = LogicalPlanBuilder::values(values.clone())?
            .alias("left")?
            .build()?;
        let right = LogicalPlanBuilder::values(values)?
            .alias("right")?
            .build()?;

        let join = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

        let plan = LogicalPlanBuilder::from(join.clone())
            .union(join)?
            .build()?;

        assert_snapshot!(plan, @r"
        Union
          Cross Join: 
            SubqueryAlias: left
              Values: (Int32(1))
            SubqueryAlias: right
              Values: (Int32(1))
          Cross Join: 
            SubqueryAlias: left
              Values: (Int32(1))
            SubqueryAlias: right
              Values: (Int32(1))
        ");

        Ok(())
    }

    #[test]
    fn test_change_redundant_column() -> Result<()> {
        let t1_field_1 = Field::new("a", DataType::Int32, false);
        let t2_field_1 = Field::new("a", DataType::Int32, false);
        let t2_field_3 = Field::new("a", DataType::Int32, false);
        let t2_field_4 = Field::new("a:1", DataType::Int32, false);
        let t1_field_2 = Field::new("b", DataType::Int32, false);
        let t2_field_2 = Field::new("b", DataType::Int32, false);

        let field_vec = vec![
            t1_field_1, t2_field_1, t1_field_2, t2_field_2, t2_field_3, t2_field_4,
        ];
        let remove_redundant = change_redundant_column(&Fields::from(field_vec));

        assert_eq!(
            remove_redundant,
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("a:1", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("b:1", DataType::Int32, false),
                Field::new("a:2", DataType::Int32, false),
                Field::new("a:1:1", DataType::Int32, false),
            ]
        );
        Ok(())
    }

    #[test]
    fn plan_builder_from_logical_plan() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .sort(vec![
                    expr::Sort::new(col("state"), true, true),
                    expr::Sort::new(col("salary"), false, false),
                ])?
                .build()?;

        let plan_expected = format!("{plan}");
        let plan_builder: LogicalPlanBuilder = Arc::new(plan).into();
        assert_eq!(plan_expected, format!("{}", plan_builder.plan));

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate_without_implicit_group_by_exprs() -> Result<()> {
        let constraints =
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        let table_source = table_source_with_constraints(&employee_schema(), constraints);

        let plan =
            LogicalPlanBuilder::scan("employee_csv", table_source, Some(vec![0, 3, 4]))?
                .aggregate(vec![col("id")], vec![sum(col("salary"))])?
                .build()?;

        assert_snapshot!(plan, @r"
        Aggregate: groupBy=[[employee_csv.id]], aggr=[[sum(employee_csv.salary)]]
          TableScan: employee_csv projection=[id, state, salary]
        ");

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate_with_implicit_group_by_exprs() -> Result<()> {
        let constraints =
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        let table_source = table_source_with_constraints(&employee_schema(), constraints);

        let options =
            LogicalPlanBuilderOptions::new().with_add_implicit_group_by_exprs(true);
        let plan =
            LogicalPlanBuilder::scan("employee_csv", table_source, Some(vec![0, 3, 4]))?
                .with_options(options)
                .aggregate(vec![col("id")], vec![sum(col("salary"))])?
                .build()?;

        assert_snapshot!(plan, @r"
        Aggregate: groupBy=[[employee_csv.id, employee_csv.state, employee_csv.salary]], aggr=[[sum(employee_csv.salary)]]
          TableScan: employee_csv projection=[id, state, salary]
        ");

        Ok(())
    }

    #[test]
    fn test_join_metadata() -> Result<()> {
        let left_schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(Field::new("a", DataType::Int32, false)))],
            HashMap::from([("key".to_string(), "left".to_string())]),
        )?;
        let right_schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(Field::new("b", DataType::Int32, false)))],
            HashMap::from([("key".to_string(), "right".to_string())]),
        )?;

        let join_schema =
            build_join_schema(&left_schema, &right_schema, &JoinType::Left)?;
        assert_eq!(
            join_schema.metadata(),
            &HashMap::from([("key".to_string(), "left".to_string())])
        );
        let join_schema =
            build_join_schema(&left_schema, &right_schema, &JoinType::Right)?;
        assert_eq!(
            join_schema.metadata(),
            &HashMap::from([("key".to_string(), "right".to_string())])
        );

        Ok(())
    }
}

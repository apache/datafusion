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

//! This module provides a user-defined builder for creating LogicalPlans

use std::{cmp::Ordering, sync::Arc};

use crate::{
    expr::Alias,
    expr_rewriter::rewrite_sort_cols_by_aggs,
    type_coercion::TypeCoerceResult,
    utils::{compare_sort_expr, group_window_expr_by_sort_keys},
    Expr, ExprSchemable, SortExpr,
};

use super::{
    Distinct, LogicalPlan, LogicalPlanBuilder, LogicalPlanBuilderConfig,
    LogicalPlanBuilderOptions, Projection, Union,
};

use arrow::datatypes::Field;
use datafusion_common::{
    exec_err, plan_datafusion_err, plan_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Column, DFSchema, DFSchemaRef, JoinType, Result,
};
use datafusion_expr_common::type_coercion::binary::comparison_coercion;

use indexmap::IndexSet;
use itertools::izip;

#[derive(Clone, Debug)]
pub struct UserDefinedLogicalBuilder<'a, C: LogicalPlanBuilderConfig> {
    config: &'a C,
    plan: LogicalPlan,
}

impl<'a, C: LogicalPlanBuilderConfig> UserDefinedLogicalBuilder<'a, C> {
    /// Create a new UserDefinedLogicalBuilder
    pub fn new(config: &'a C, plan: LogicalPlan) -> Self {
        Self { config, plan }
    }

    // Return Result since most of the use cases expect Result
    pub fn build(self) -> Result<LogicalPlan> {
        Ok(self.plan)
    }

    pub fn filter(self, predicate: Expr) -> Result<Self> {
        let predicate = self.try_coerce_filter_predicate(predicate)?;
        let plan = LogicalPlanBuilder::from(self.plan)
            .filter(predicate)?
            .build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn project(self, expr: Vec<Expr>) -> Result<Self> {
        let expr = self.try_coerce_projection(expr)?;
        let plan = LogicalPlanBuilder::from(self.plan).project(expr)?.build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn distinct(self) -> Result<Self> {
        let plan = LogicalPlan::Distinct(Distinct::All(Arc::new(self.plan)));
        Ok(Self::new(self.config, plan))
    }

    pub fn aggregate(
        self,
        options: LogicalPlanBuilderOptions,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        let group_expr = self.try_coerce_group_expr(group_expr)?;

        let plan = LogicalPlanBuilder::from(self.plan)
            .with_options(options)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn having(self, expr: Expr) -> Result<Self> {
        let expr = self.try_coerce_having_expr(expr)?;
        let plan = LogicalPlanBuilder::from(self.plan).having(expr)?.build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn join_on(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        on_exprs: Vec<Expr>,
    ) -> Result<Self> {
        let on_exprs = self.try_coerce_join_on_exprs(right.schema(), on_exprs)?;
        let plan = LogicalPlanBuilder::from(self.plan)
            .join_on(right, join_type, on_exprs)?
            .build()?;
        Ok(Self::new(self.config, plan))
    }

    /// Empty sort_expr indicates no sorting
    pub fn distinct_on(
        self,
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<SortExpr>>,
    ) -> Result<Self> {
        let on_expr = self.try_coerce_distinct_on_expr(on_expr)?;
        // select_expr is the same as projection expr
        let select_expr = self.try_coerce_projection(select_expr)?;
        let sort_expr = sort_expr
            .map(|expr| self.try_coerce_order_by_expr(expr))
            .transpose()?;
        let plan = LogicalPlanBuilder::from(self.plan)
            .distinct_on(on_expr, select_expr, sort_expr)?
            .build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn union(self, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let base_plan_field_count = self.plan.schema().fields().len();
        let fields_count = inputs
            .iter()
            .map(|p| p.schema().fields().len())
            .collect::<Vec<_>>();
        if fields_count
            .iter()
            .any(|&count| count != base_plan_field_count)
        {
            return plan_err!(
                "UNION queries have different number of columns: \
                base plan has {} columns whereas union plans has columns {:?}",
                base_plan_field_count,
                fields_count
            );
        }

        // let union_fields = (0..base_plan_field_count)
        //     .map(|i| {
        //         let base_field = self.plan.schema().field(i);
        //         let union_fields = inputs.iter().map(|p| p.schema().field(i)).collect::<Vec<_>>();
        //         if union_fields.iter().any(|f| f.data_type() != base_field.data_type()) {
        //             return plan_err!(
        //                 "UNION queries have different data types for column {}: \
        //                 base plan has data type {:?} whereas union plans has data types {:?}",
        //                 i,
        //                 base_field.data_type(),
        //                 union_fields.iter().map(|f| f.data_type()).collect::<Vec<_>>()
        //             )
        //         }

        //         let union_nullabilities = union_fields.iter().map(|f| f.is_nullable()).collect::<Vec<_>>();
        //         if union_nullabilities.iter().any(|&nullable| nullable != base_field.is_nullable()) {
        //             return plan_err!(
        //                 "UNION queries have different nullabilities for column {}: \
        //                 base plan has nullable {:?} whereas union plans has nullabilities {:?}",
        //                 i,
        //                 base_field.is_nullable(),
        //                 union_nullabilities
        //             )
        //         }

        //         let union_field_meta = union_fields.iter().map(|f| f.metadata().clone()).collect::<Vec<_>>();
        //         let mut metadata = base_field.metadata().clone();
        //         for field_meta in union_field_meta {
        //             metadata.extend(field_meta);
        //         }

        //         Ok(base_field.clone().with_metadata(metadata))
        //     })
        //     .collect::<Result<Vec<_>>>()?;

        // self.plan + inputs
        let plan_ref = std::iter::once(&self.plan)
            .chain(inputs.iter())
            .collect::<Vec<_>>();

        let union_schema = Arc::new(coerce_union_schema(&plan_ref)?);
        let inputs = std::iter::once(self.plan)
            .chain(inputs.into_iter())
            .collect::<Vec<_>>();

        let inputs = inputs
            .into_iter()
            .map(|p| {
                let plan = coerce_plan_expr_for_schema(p, &union_schema)?;
                match plan {
                    LogicalPlan::Projection(Projection { expr, input, .. }) => {
                        Ok(project_with_column_index(
                            expr,
                            input,
                            Arc::clone(&union_schema),
                        )?)
                    }
                    plan => Ok(plan),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let inputs = inputs.into_iter().map(Arc::new).collect::<Vec<_>>();
        let plan = LogicalPlan::Union(Union {
            inputs,
            schema: union_schema,
        });
        Ok(Self::new(self.config, plan))
    }

    // Similar to `sort_with_limit` in `LogicalPlanBuilder` + coercion
    pub fn sort(self, sorts: Vec<SortExpr>, fetch: Option<usize>) -> Result<Self> {
        // println!("sorts: {:?}", sorts);
        let sorts = self.try_coerce_order_by_expr(sorts)?;
        // println!("sorts after coercion: {:?}", sorts);
        let plan = LogicalPlanBuilder::from(self.plan)
            .sort_with_limit(sorts, fetch)?
            .build()?;
        Ok(Self::new(self.config, plan))
    }

    pub fn window(self, window_exprs: Vec<Expr>) -> Result<Self> {
        let mut groups = group_window_expr_by_sort_keys(window_exprs)?;
        // To align with the behavior of PostgreSQL, we want the sort_keys sorted as same rule as PostgreSQL that first
        // we compare the sort key themselves and if one window's sort keys are a prefix of another
        // put the window with more sort keys first. so more deeply sorted plans gets nested further down as children.
        // The sort_by() implementation here is a stable sort.
        // Note that by this rule if there's an empty over, it'll be at the top level
        groups.sort_by(|(key_a, _), (key_b, _)| {
            for ((first, _), (second, _)) in key_a.iter().zip(key_b.iter()) {
                let key_ordering = compare_sort_expr(first, second, self.plan.schema());
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

        let mut result = self;
        for (_, window_exprs) in groups {
            result = result.window_inner(window_exprs)?;
        }
        Ok(result)
    }

    fn window_inner(self, window_exprs: Vec<Expr>) -> Result<Self> {
        let window_exprs = self.try_coerce_window_exprs(window_exprs)?;

        // Partition and sorting is done at physical level, see the EnforceDistribution
        // and EnforceSorting rules.
        let plan = LogicalPlanBuilder::from(self.plan)
            .window(window_exprs)?
            .build()?;

        Ok(Self::new(self.config, plan))
    }

    ///
    /// Coercion level - LogicalPlan
    ///

    fn try_coerce_filter_predicate(&self, predicate: Expr) -> Result<Expr> {
        self.try_coerce_binary_expr(predicate, self.plan.schema())
    }

    fn try_coerce_projection(&self, expr: Vec<Expr>) -> Result<Vec<Expr>> {
        expr.into_iter()
            .map(|e| self.try_coerce_binary_expr(e, self.plan.schema()))
            .collect()
    }

    fn try_coerce_group_expr(&self, group_expr: Vec<Expr>) -> Result<Vec<Expr>> {
        group_expr
            .into_iter()
            .map(|e| self.try_coerce_binary_expr(e, self.plan.schema()))
            .collect()
    }

    fn try_coerce_having_expr(&self, expr: Expr) -> Result<Expr> {
        self.try_coerce_binary_expr(expr, self.plan.schema())
    }

    fn try_coerce_join_on_exprs(
        &self,
        right_schema: &DFSchemaRef,
        on_exprs: Vec<Expr>,
    ) -> Result<Vec<Expr>> {
        let schema = self.plan.schema().join(&right_schema).map(Arc::new)?;

        on_exprs
            .into_iter()
            .map(|e| self.try_coerce_binary_expr(e, &schema))
            .collect()
    }

    fn try_coerce_distinct_on_expr(&self, expr: Vec<Expr>) -> Result<Vec<Expr>> {
        expr.into_iter()
            .map(|e| self.try_coerce_binary_expr(e, self.plan.schema()))
            .collect()
    }

    fn try_coerce_window_exprs(&self, expr: Vec<Expr>) -> Result<Vec<Expr>> {
        expr.into_iter()
            .map(|e| self.try_coerce_binary_expr(e, self.plan.schema()))
            .collect()
    }

    fn try_coerce_order_by_expr(&self, expr: Vec<SortExpr>) -> Result<Vec<SortExpr>> {
        expr.into_iter()
            .map(|e| {
                let SortExpr { expr, .. } = e;
                self.try_coerce_binary_expr(expr, self.plan.schema())
                    .map(|expr| SortExpr { expr, ..e })
            })
            .collect()
    }

    ///
    /// Coercion level - Expr
    ///

    fn try_coerce_binary_expr(
        &self,
        binary_expr: Expr,
        schema: &DFSchemaRef,
    ) -> Result<Expr> {
        binary_expr.transform_up(|binary_expr| {
            if let Expr::BinaryExpr(mut e) = binary_expr {
                for type_coercion in self.config.get_type_coercions() {
                    match type_coercion.coerce_binary_expr(e, schema)? {
                        TypeCoerceResult::CoercedExpr(expr) => {
                            return Ok(Transformed::yes(expr));
                        }
                        TypeCoerceResult::Original(expr) => {
                            e = expr;
                        }
                        _ => return exec_err!(
                            "CoercedPlan is not an expected result for `coerce_binary_expr`"
                        ),
                    }
                }
                return exec_err!(
                    "Likely DefaultTypeCoercion is not added to the SessionState"
                );
            } else {
                Ok(Transformed::no(binary_expr))
            }
        }).data()
    }
}

/// Get a common schema that is compatible with all inputs of UNION.
///
/// This method presumes that the wildcard expansion is unneeded, or has already
/// been applied.
fn coerce_union_schema(inputs: &[&LogicalPlan]) -> Result<DFSchema> {
    let base_schema = inputs[0].schema();
    let mut union_datatypes = base_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut union_nullabilities = base_schema
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();
    let mut union_field_meta = base_schema
        .fields()
        .iter()
        .map(|f| f.metadata().clone())
        .collect::<Vec<_>>();

    let mut metadata = base_schema.metadata().clone();

    for (i, plan) in inputs.iter().enumerate().skip(1) {
        let plan_schema = plan.schema();
        metadata.extend(plan_schema.metadata().clone());

        if plan_schema.fields().len() != base_schema.fields().len() {
            return plan_err!(
                "Union schemas have different number of fields: \
                query 1 has {} fields whereas query {} has {} fields",
                base_schema.fields().len(),
                i + 1,
                plan_schema.fields().len()
            );
        }

        // coerce data type and nullability for each field
        for (union_datatype, union_nullable, union_field_map, plan_field) in izip!(
            union_datatypes.iter_mut(),
            union_nullabilities.iter_mut(),
            union_field_meta.iter_mut(),
            plan_schema.fields().iter()
        ) {
            let coerced_type =
                comparison_coercion(union_datatype, plan_field.data_type()).ok_or_else(
                    || {
                        plan_datafusion_err!(
                            "Incompatible inputs for Union: Previous inputs were \
                            of type {}, but got incompatible type {} on column '{}'",
                            union_datatype,
                            plan_field.data_type(),
                            plan_field.name()
                        )
                    },
                )?;

            *union_datatype = coerced_type;
            *union_nullable = *union_nullable || plan_field.is_nullable();
            union_field_map.extend(plan_field.metadata().clone());
        }
    }
    let union_qualified_fields = izip!(
        base_schema.iter(),
        union_datatypes.into_iter(),
        union_nullabilities,
        union_field_meta.into_iter()
    )
    .map(|((qualifier, field), datatype, nullable, metadata)| {
        let mut field = Field::new(field.name().clone(), datatype, nullable);
        field.set_metadata(metadata);
        (qualifier.cloned(), field.into())
    })
    .collect::<Vec<_>>();

    DFSchema::new_with_metadata(union_qualified_fields, metadata)
}

/// See `<https://github.com/apache/datafusion/pull/2108>`
fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            Expr::Alias(Alias { ref name, .. }) if name != schema.field(i).name() => {
                Ok(e.unalias().alias(schema.field(i).name()))
            }
            Expr::Column(Column {
                relation: _,
                ref name,
                spans: _,
            }) if name != schema.field(i).name() => Ok(e.alias(schema.field(i).name())),
            Expr::Alias { .. } | Expr::Column { .. } => Ok(e),
            #[expect(deprecated)]
            Expr::Wildcard { .. } => {
                plan_err!("Wildcard should be expanded before type coercion")
            }
            _ => Ok(e.alias(schema.field(i).name())),
        })
        .collect::<Result<Vec<_>>>()?;

    Projection::try_new_with_schema(alias_expr, input, schema)
        .map(LogicalPlan::Projection)
}

/// Returns plan with expressions coerced to types compatible with
/// schema types
fn coerce_plan_expr_for_schema(
    plan: LogicalPlan,
    schema: &DFSchema,
) -> Result<LogicalPlan> {
    match plan {
        // special case Projection to avoid adding multiple projections
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let new_exprs = coerce_exprs_for_schema(expr, input.schema(), schema)?;
            let projection = Projection::try_new(new_exprs, input)?;
            Ok(LogicalPlan::Projection(projection))
        }
        _ => {
            let exprs: Vec<Expr> = plan.schema().iter().map(Expr::from).collect();
            let new_exprs = coerce_exprs_for_schema(exprs, plan.schema(), schema)?;
            let add_project = new_exprs.iter().any(|expr| expr.try_as_col().is_none());
            if add_project {
                let projection = Projection::try_new(new_exprs, Arc::new(plan))?;
                Ok(LogicalPlan::Projection(projection))
            } else {
                Ok(plan)
            }
        }
    }
}

fn coerce_exprs_for_schema(
    exprs: Vec<Expr>,
    src_schema: &DFSchema,
    dst_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            let new_type = dst_schema.field(idx).data_type();
            if new_type != &expr.get_type(src_schema)? {
                let (table_ref, name) = expr.qualified_name();

                let new_expr = match expr {
                    Expr::Alias(Alias { expr, name, .. }) => {
                        expr.cast_to(new_type, src_schema)?.alias(name)
                    }
                    #[expect(deprecated)]
                    Expr::Wildcard { .. } => expr,
                    _ => expr.cast_to(new_type, src_schema)?,
                };

                let (new_table_ref, new_name) = new_expr.qualified_name();
                if table_ref != new_table_ref || name != new_name {
                    Ok(new_expr.alias_qualified(table_ref, name))
                } else {
                    Ok(new_expr)
                }
            } else {
                Ok(expr)
            }
        })
        .collect::<Result<_>>()
}

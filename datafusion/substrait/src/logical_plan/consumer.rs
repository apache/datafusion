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

use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, OffsetBuffer};
use async_recursion::async_recursion;
use datafusion::arrow::array::{GenericListArray, MapArray};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit,
};
use datafusion::common::plan_err;
use datafusion::common::{
    not_impl_err, plan_datafusion_err, substrait_datafusion_err, substrait_err, DFSchema,
    DFSchemaRef,
};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{Exists, InSubquery, Sort};

use datafusion::logical_expr::{
    expr::find_df_window_func, Aggregate, BinaryExpr, Case, EmptyRelation, Expr,
    ExprSchemable, LogicalPlan, Operator, Projection, SortExpr, Values,
};
use substrait::proto::expression::subquery::set_predicate::PredicateOp;
use url::Url;

use crate::extensions::Extensions;
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    INTERVAL_MONTH_DAY_NANO_TYPE_NAME, LARGE_CONTAINER_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF, VIEW_CONTAINER_TYPE_VARIATION_REF,
};
#[allow(deprecated)]
use crate::variation_const::{
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_MONTH_DAY_NANO_TYPE_REF,
    INTERVAL_YEAR_MONTH_TYPE_REF, TIMESTAMP_MICRO_TYPE_VARIATION_REF,
    TIMESTAMP_MILLI_TYPE_VARIATION_REF, TIMESTAMP_NANO_TYPE_VARIATION_REF,
    TIMESTAMP_SECOND_TYPE_VARIATION_REF,
};
use datafusion::arrow::array::{new_empty_array, AsArray};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    col, expr, Cast, Extension, GroupingSet, Like, LogicalPlanBuilder, Partitioning,
    Repartition, Subquery, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::prelude::JoinType;
use datafusion::sql::TableReference;
use datafusion::{
    error::Result,
    logical_expr::utils::split_conjunction,
    prelude::{Column, SessionContext},
    scalar::ScalarValue,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use substrait::proto::exchange_rel::ExchangeKind;
use substrait::proto::expression::literal::interval_day_to_second::PrecisionMode;
use substrait::proto::expression::literal::user_defined::Val;
use substrait::proto::expression::literal::{
    IntervalDayToSecond, IntervalYearToMonth, UserDefined,
};
use substrait::proto::expression::subquery::SubqueryType;
use substrait::proto::expression::{self, FieldReference, Literal, ScalarFunction};
use substrait::proto::read_rel::local_files::file_or_files::PathType::UriFile;
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind, window_function::Bound,
        window_function::BoundsType, MaskExpression, RexType,
    },
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    set_rel,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, Expression, NamedStruct, Plan, Rel, Type,
};
use substrait::proto::{FunctionArgument, SortField};

// Substrait PrecisionTimestampTz indicates that the timestamp is relative to UTC, which
// is the same as the expectation for any non-empty timezone in DF, so any non-empty timezone
// results in correct points on the timeline, and we pick UTC as a reasonable default.
// However, DF uses the timezone also for some arithmetic and display purposes (see e.g.
// https://github.com/apache/arrow-rs/blob/ee5694078c86c8201549654246900a4232d531a9/arrow-cast/src/cast/mod.rs#L1749).
const DEFAULT_TIMEZONE: &str = "UTC";

pub fn name_to_op(name: &str) -> Option<Operator> {
    match name {
        "equal" => Some(Operator::Eq),
        "not_equal" => Some(Operator::NotEq),
        "lt" => Some(Operator::Lt),
        "lte" => Some(Operator::LtEq),
        "gt" => Some(Operator::Gt),
        "gte" => Some(Operator::GtEq),
        "add" => Some(Operator::Plus),
        "subtract" => Some(Operator::Minus),
        "multiply" => Some(Operator::Multiply),
        "divide" => Some(Operator::Divide),
        "mod" => Some(Operator::Modulo),
        "and" => Some(Operator::And),
        "or" => Some(Operator::Or),
        "is_distinct_from" => Some(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Some(Operator::IsNotDistinctFrom),
        "regex_match" => Some(Operator::RegexMatch),
        "regex_imatch" => Some(Operator::RegexIMatch),
        "regex_not_match" => Some(Operator::RegexNotMatch),
        "regex_not_imatch" => Some(Operator::RegexNotIMatch),
        "bitwise_and" => Some(Operator::BitwiseAnd),
        "bitwise_or" => Some(Operator::BitwiseOr),
        "str_concat" => Some(Operator::StringConcat),
        "at_arrow" => Some(Operator::AtArrow),
        "arrow_at" => Some(Operator::ArrowAt),
        "bitwise_xor" => Some(Operator::BitwiseXor),
        "bitwise_shift_right" => Some(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Some(Operator::BitwiseShiftLeft),
        _ => None,
    }
}

pub fn substrait_fun_name(name: &str) -> &str {
    let name = match name.rsplit_once(':') {
        // Since 0.32.0, Substrait requires the function names to be in a compound format
        // https://substrait.io/extensions/#function-signature-compound-names
        // for example, `add:i8_i8`.
        // On the consumer side, we don't really care about the signature though, just the name.
        Some((name, _)) => name,
        None => name,
    };
    name
}

fn split_eq_and_noneq_join_predicate_with_nulls_equality(
    filter: &Expr,
) -> (Vec<(Column, Column)>, bool, Option<Expr>) {
    let exprs = split_conjunction(filter);

    let mut accum_join_keys: Vec<(Column, Column)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    let mut nulls_equal_nulls = false;

    for expr in exprs {
        #[allow(clippy::collapsible_match)]
        match expr {
            Expr::BinaryExpr(binary_expr) => match binary_expr {
                x @ (BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }
                | BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                }) => {
                    nulls_equal_nulls = match x.op {
                        Operator::Eq => false,
                        Operator::IsNotDistinctFrom => true,
                        _ => unreachable!(),
                    };

                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(l), Expr::Column(r)) => {
                            accum_join_keys.push((l.clone(), r.clone()));
                        }
                        _ => accum_filters.push(expr.clone()),
                    }
                }
                _ => accum_filters.push(expr.clone()),
            },
            _ => accum_filters.push(expr.clone()),
        }
    }

    let join_filter = accum_filters.into_iter().reduce(Expr::and);
    (accum_join_keys, nulls_equal_nulls, join_filter)
}

/// Convert Substrait Plan to DataFusion LogicalPlan
pub async fn from_substrait_plan(
    ctx: &SessionContext,
    plan: &Plan,
) -> Result<LogicalPlan> {
    // Register function extension
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, rel, &extensions).await?)
                    },
                    plan_rel::RelType::Root(root) => {
                        let plan = from_substrait_rel(ctx, root.input.as_ref().unwrap(), &extensions).await?;
                        if root.names.is_empty() {
                            // Backwards compatibility for plans missing names
                            return Ok(plan);
                        }
                        let renamed_schema = make_renamed_schema(plan.schema(), &root.names)?;
                        if renamed_schema.equivalent_names_and_types(plan.schema()) {
                            // Nothing to do if the schema is already equivalent
                            return Ok(plan);
                        }

                        match plan {
                            // If the last node of the plan produces expressions, bake the renames into those expressions.
                            // This isn't necessary for correctness, but helps with roundtrip tests.
                            LogicalPlan::Projection(p) => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(p.expr, p.input.schema(), &renamed_schema)?, p.input)?)),
                            LogicalPlan::Aggregate(a) => {
                                let new_aggr_exprs = rename_expressions(a.aggr_expr, a.input.schema(), &renamed_schema)?;
                                Ok(LogicalPlan::Aggregate(Aggregate::try_new(a.input, a.group_expr, new_aggr_exprs)?))
                            },
                            // There are probably more plans where we could bake things in, can add them later as needed.
                            // Otherwise, add a new Project to handle the renaming.
                            _ => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(plan.schema().columns().iter().map(|c| col(c.to_owned())), plan.schema(), &renamed_schema)?, Arc::new(plan))?))
                        }
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
}

/// parse projection
pub fn extract_projection(
    t: LogicalPlan,
    projection: &::core::option::Option<expression::MaskExpression>,
) -> Result<LogicalPlan> {
    match projection {
        Some(MaskExpression { select, .. }) => match &select.as_ref() {
            Some(projection) => {
                let column_indices: Vec<usize> = projection
                    .struct_items
                    .iter()
                    .map(|item| item.field as usize)
                    .collect();
                match t {
                    LogicalPlan::TableScan(mut scan) => {
                        let fields = column_indices
                            .iter()
                            .map(|i| scan.projected_schema.qualified_field(*i))
                            .map(|(qualifier, field)| {
                                (qualifier.cloned(), Arc::new(field.clone()))
                            })
                            .collect();
                        scan.projection = Some(column_indices);
                        scan.projected_schema = DFSchemaRef::new(
                            DFSchema::new_with_metadata(fields, HashMap::new())?,
                        );
                        Ok(LogicalPlan::TableScan(scan))
                    }
                    LogicalPlan::Projection(projection) => {
                        // create another Projection around the Projection to handle the field masking
                        let fields: Vec<Expr> = column_indices
                            .into_iter()
                            .map(|i| {
                                let (qualifier, field) =
                                    projection.schema.qualified_field(i);
                                let column =
                                    Column::new(qualifier.cloned(), field.name());
                                Expr::Column(column)
                            })
                            .collect();
                        project(LogicalPlan::Projection(projection), fields)
                    }
                    _ => plan_err!("unexpected plan for table"),
                }
            }
            _ => Ok(t),
        },
        _ => Ok(t),
    }
}

/// Ensure the expressions have the right name(s) according to the new schema.
/// This includes the top-level (column) name, which will be renamed through aliasing if needed,
/// as well as nested names (if the expression produces any struct types), which will be renamed
/// through casting if needed.
fn rename_expressions(
    exprs: impl IntoIterator<Item = Expr>,
    input_schema: &DFSchema,
    new_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .zip(new_schema.fields())
        .map(|(old_expr, new_field)| {
            // Check if type (i.e. nested struct field names) match, use Cast to rename if needed
            let new_expr = if &old_expr.get_type(input_schema)? != new_field.data_type() {
                Expr::Cast(Cast::new(
                    Box::new(old_expr),
                    new_field.data_type().to_owned(),
                ))
            } else {
                old_expr
            };
            // Alias column if needed to fix the top-level name
            match &new_expr {
                // If expr is a column reference, alias_if_changed would cause an aliasing if the old expr has a qualifier
                Expr::Column(c) if &c.name == new_field.name() => Ok(new_expr),
                _ => new_expr.alias_if_changed(new_field.name().to_owned()),
            }
        })
        .collect()
}

/// Produce a version of the given schema with names matching the given list of names.
/// Substrait doesn't deal with column (incl. nested struct field) names within the schema,
/// but it does give us the list of expected names at the end of the plan, so we use this
/// to rename the schema to match the expected names.
fn make_renamed_schema(
    schema: &DFSchemaRef,
    dfs_names: &Vec<String>,
) -> Result<DFSchema> {
    fn rename_inner_fields(
        dtype: &DataType,
        dfs_names: &Vec<String>,
        name_idx: &mut usize,
    ) -> Result<DataType> {
        match dtype {
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| {
                        let name = next_struct_field_name(0, dfs_names, name_idx)?;
                        Ok((**f).to_owned().with_name(name).with_data_type(
                            rename_inner_fields(f.data_type(), dfs_names, name_idx)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                Ok(DataType::Struct(fields))
            }
            DataType::List(inner) => Ok(DataType::List(FieldRef::new(
                (**inner).to_owned().with_data_type(rename_inner_fields(
                    inner.data_type(),
                    dfs_names,
                    name_idx,
                )?),
            ))),
            DataType::LargeList(inner) => Ok(DataType::LargeList(FieldRef::new(
                (**inner).to_owned().with_data_type(rename_inner_fields(
                    inner.data_type(),
                    dfs_names,
                    name_idx,
                )?),
            ))),
            _ => Ok(dtype.to_owned()),
        }
    }

    let mut name_idx = 0;

    let (qualifiers, fields): (_, Vec<Field>) = schema
        .iter()
        .map(|(q, f)| {
            let name = next_struct_field_name(0, dfs_names, &mut name_idx)?;
            Ok((
                q.cloned(),
                (**f)
                    .to_owned()
                    .with_name(name)
                    .with_data_type(rename_inner_fields(
                        f.data_type(),
                        dfs_names,
                        &mut name_idx,
                    )?),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    if name_idx != dfs_names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            dfs_names.len());
    }

    DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(Schema::new(fields)),
    )
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    ctx: &SessionContext,
    rel: &Rel,
    extensions: &Extensions,
) -> Result<LogicalPlan> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            if let Some(input) = p.input.as_ref() {
                let mut input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut names: HashSet<String> = HashSet::new();
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x =
                        from_substrait_rex(ctx, e, input.clone().schema(), extensions)
                            .await?;
                    // if the expression is WindowFunction, wrap in a Window relation
                    if let Expr::WindowFunction(_) = &x {
                        // Adding the same expression here and in the project below
                        // works because the project's builder uses columnize_expr(..)
                        // to transform it into a column reference
                        input = input.window(vec![x.clone()])?
                    }
                    // Ensure the expression has a unique display name, so that project's
                    // validate_unique_names doesn't fail
                    let name = x.schema_name().to_string();
                    let mut new_name = name.clone();
                    let mut i = 0;
                    while names.contains(&new_name) {
                        new_name = format!("{}__temp__{}", name, i);
                        i += 1;
                    }
                    if new_name != name {
                        exprs.push(x.alias(new_name.clone()));
                    } else {
                        exprs.push(x);
                    }
                    names.insert(new_name);
                }
                input.project(exprs)?.build()
            } else {
                not_impl_err!("Projection without an input is not supported")
            }
        }
        Some(RelType::Filter(filter)) => {
            if let Some(input) = filter.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                if let Some(condition) = filter.condition.as_ref() {
                    let expr =
                        from_substrait_rex(ctx, condition, input.schema(), extensions)
                            .await?;
                    input.filter(expr)?.build()
                } else {
                    not_impl_err!("Filter without an condition is not valid")
                }
            } else {
                not_impl_err!("Filter without an input is not valid")
            }
        }
        Some(RelType::Fetch(fetch)) => {
            if let Some(input) = fetch.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let offset = fetch.offset as usize;
                // Since protobuf can't directly distinguish `None` vs `0` `None` is encoded as `MAX`
                let count = if fetch.count as usize == usize::MAX {
                    None
                } else {
                    Some(fetch.count as usize)
                };
                input.limit(offset, count)?.build()
            } else {
                not_impl_err!("Fetch without an input is not valid")
            }
        }
        Some(RelType::Sort(sort)) => {
            if let Some(input) = sort.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let sorts =
                    from_substrait_sorts(ctx, &sort.sorts, input.schema(), extensions)
                        .await?;
                input.sort(sorts)?.build()
            } else {
                not_impl_err!("Sort without an input is not valid")
            }
        }
        Some(RelType::Aggregate(agg)) => {
            if let Some(input) = agg.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut group_expr = vec![];
                let mut aggr_expr = vec![];

                match agg.groupings.len() {
                    1 => {
                        for e in &agg.groupings[0].grouping_expressions {
                            let x =
                                from_substrait_rex(ctx, e, input.schema(), extensions)
                                    .await?;
                            group_expr.push(x);
                        }
                    }
                    _ => {
                        let mut grouping_sets = vec![];
                        for grouping in &agg.groupings {
                            let mut grouping_set = vec![];
                            for e in &grouping.grouping_expressions {
                                let x = from_substrait_rex(
                                    ctx,
                                    e,
                                    input.schema(),
                                    extensions,
                                )
                                .await?;
                                grouping_set.push(x);
                            }
                            grouping_sets.push(grouping_set);
                        }
                        // Single-element grouping expression of type Expr::GroupingSet.
                        // Note that GroupingSet::Rollup would become GroupingSet::GroupingSets, when
                        // parsed by the producer and consumer, since Substrait does not have a type dedicated
                        // to ROLLUP. Only vector of Groupings (grouping sets) is available.
                        group_expr.push(Expr::GroupingSet(GroupingSet::GroupingSets(
                            grouping_sets,
                        )));
                    }
                };

                for m in &agg.measures {
                    let filter = match &m.filter {
                        Some(fil) => Some(Box::new(
                            from_substrait_rex(ctx, fil, input.schema(), extensions)
                                .await?,
                        )),
                        None => None,
                    };
                    let agg_func = match &m.measure {
                        Some(f) => {
                            let distinct = match f.invocation {
                                _ if f.invocation
                                    == AggregationInvocation::Distinct as i32 =>
                                {
                                    true
                                }
                                _ if f.invocation
                                    == AggregationInvocation::All as i32 =>
                                {
                                    false
                                }
                                _ => false,
                            };
                            from_substrait_agg_func(
                                ctx,
                                f,
                                input.schema(),
                                extensions,
                                filter,
                                // TODO: Add parsing of order_by also
                                None,
                                distinct,
                            )
                            .await
                        }
                        None => not_impl_err!(
                            "Aggregate without aggregate function is not supported"
                        ),
                    };
                    aggr_expr.push(agg_func?.as_ref().clone());
                }
                input.aggregate(group_expr, aggr_expr)?.build()
            } else {
                not_impl_err!("Aggregate without an input is not valid")
            }
        }
        Some(RelType::Join(join)) => {
            if join.post_join_filter.is_some() {
                return not_impl_err!(
                    "JoinRel with post_join_filter is not yet supported"
                );
            }

            let left: LogicalPlanBuilder = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.left.as_ref().unwrap(), extensions).await?,
            );
            let right = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.right.as_ref().unwrap(), extensions).await?,
            );
            let (left, right) = requalify_sides_if_needed(left, right)?;

            let join_type = from_substrait_jointype(join.r#type)?;
            // The join condition expression needs full input schema and not the output schema from join since we lose columns from
            // certain join types such as semi and anti joins
            let in_join_schema = left.schema().join(right.schema())?;

            // If join expression exists, parse the `on` condition expression, build join and return
            // Otherwise, build join with only the filter, without join keys
            match &join.expression.as_ref() {
                Some(expr) => {
                    let on = from_substrait_rex(ctx, expr, &in_join_schema, extensions)
                        .await?;
                    // The join expression can contain both equal and non-equal ops.
                    // As of datafusion 31.0.0, the equal and non equal join conditions are in separate fields.
                    // So we extract each part as follows:
                    // - If an Eq or IsNotDistinctFrom op is encountered, add the left column, right column and is_null_equal_nulls to `join_ons` vector
                    // - Otherwise we add the expression to join_filter (use conjunction if filter already exists)
                    let (join_ons, nulls_equal_nulls, join_filter) =
                        split_eq_and_noneq_join_predicate_with_nulls_equality(&on);
                    let (left_cols, right_cols): (Vec<_>, Vec<_>) =
                        itertools::multiunzip(join_ons);
                    left.join_detailed(
                        right.build()?,
                        join_type,
                        (left_cols, right_cols),
                        join_filter,
                        nulls_equal_nulls,
                    )?
                    .build()
                }
                None => plan_err!("JoinRel without join condition is not allowed"),
            }
        }
        Some(RelType::Cross(cross)) => {
            let left = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, cross.left.as_ref().unwrap(), extensions).await?,
            );
            let right = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, cross.right.as_ref().unwrap(), extensions)
                    .await?,
            );
            let (left, right) = requalify_sides_if_needed(left, right)?;
            left.cross_join(right.build()?)?.build()
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let named_struct = read.base_schema.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("No base schema provided for Named Table")
                })?;

                let table_reference = match nt.names.len() {
                    0 => {
                        return plan_err!("No table name found in NamedTable");
                    }
                    1 => TableReference::Bare {
                        table: nt.names[0].clone().into(),
                    },
                    2 => TableReference::Partial {
                        schema: nt.names[0].clone().into(),
                        table: nt.names[1].clone().into(),
                    },
                    _ => TableReference::Full {
                        catalog: nt.names[0].clone().into(),
                        schema: nt.names[1].clone().into(),
                        table: nt.names[2].clone().into(),
                    },
                };

                let substrait_schema =
                    from_substrait_named_struct(named_struct, extensions)?
                        .replace_qualifier(table_reference.clone());

                let t = ctx.table(table_reference.clone()).await?;
                let t = ensure_schema_compatability(t, substrait_schema)?;
                let t = t.into_optimized_plan()?;
                extract_projection(t, &read.projection)
            }
            Some(ReadType::VirtualTable(vt)) => {
                let base_schema = read.base_schema.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("No base schema provided for Virtual Table")
                })?;

                let schema = from_substrait_named_struct(base_schema, extensions)?;

                if vt.values.is_empty() {
                    return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: DFSchemaRef::new(schema),
                    }));
                }

                let values = vt
                    .values
                    .iter()
                    .map(|row| {
                        let mut name_idx = 0;
                        let lits = row
                            .fields
                            .iter()
                            .map(|lit| {
                                name_idx += 1; // top-level names are provided through schema
                                Ok(Expr::Literal(from_substrait_literal(
                                    lit,
                                    extensions,
                                    &base_schema.names,
                                    &mut name_idx,
                                )?))
                            })
                            .collect::<Result<_>>()?;
                        if name_idx != base_schema.names.len() {
                            return substrait_err!(
                                "Names list must match exactly to nested schema, but found {} uses for {} names",
                                name_idx,
                                base_schema.names.len()
                            );
                        }
                        Ok(lits)
                    })
                    .collect::<Result<_>>()?;

                Ok(LogicalPlan::Values(Values {
                    schema: DFSchemaRef::new(schema),
                    values,
                }))
            }
            Some(ReadType::LocalFiles(lf)) => {
                fn extract_filename(name: &str) -> Option<String> {
                    let corrected_url =
                        if name.starts_with("file://") && !name.starts_with("file:///") {
                            name.replacen("file://", "file:///", 1)
                        } else {
                            name.to_string()
                        };

                    Url::parse(&corrected_url).ok().and_then(|url| {
                        let path = url.path();
                        std::path::Path::new(path)
                            .file_name()
                            .map(|filename| filename.to_string_lossy().to_string())
                    })
                }

                // we could use the file name to check the original table provider
                // TODO: currently does not support multiple local files
                let filename: Option<String> =
                    lf.items.first().and_then(|x| match x.path_type.as_ref() {
                        Some(UriFile(name)) => extract_filename(name),
                        _ => None,
                    });

                if lf.items.len() > 1 || filename.is_none() {
                    return not_impl_err!("Only single file reads are supported");
                }
                let name = filename.unwrap();
                // directly use unwrap here since we could determine it is a valid one
                let table_reference = TableReference::Bare { table: name.into() };
                let t = ctx.table(table_reference).await?;
                let t = t.into_optimized_plan()?;
                extract_projection(t, &read.projection)
            }
            _ => not_impl_err!("Unsupported ReadType: {:?}", &read.as_ref().read_type),
        },
        Some(RelType::Set(set)) => match set_rel::SetOp::try_from(set.op) {
            Ok(set_op) => match set_op {
                set_rel::SetOp::UnionAll => {
                    if !set.inputs.is_empty() {
                        let mut union_builder = Ok(LogicalPlanBuilder::from(
                            from_substrait_rel(ctx, &set.inputs[0], extensions).await?,
                        ));
                        for input in &set.inputs[1..] {
                            union_builder = union_builder?
                                .union(from_substrait_rel(ctx, input, extensions).await?);
                        }
                        union_builder?.build()
                    } else {
                        not_impl_err!("Union relation requires at least one input")
                    }
                }
                _ => not_impl_err!("Unsupported set operator: {set_op:?}"),
            },
            Err(e) => not_impl_err!("Invalid set operation type {}: {e}", set.op),
        },
        Some(RelType::ExtensionLeaf(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionLeafRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionSingle(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            let Some(input_rel) = &extension.input else {
                return substrait_err!(
                    "ExtensionSingleRel doesn't contains input rel. Try use ExtensionLeafRel instead"
                );
            };
            let input_plan = from_substrait_rel(ctx, input_rel, extensions).await?;
            let plan =
                plan.with_exprs_and_inputs(plan.expressions(), vec![input_plan])?;
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionMulti(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            let mut inputs = Vec::with_capacity(extension.inputs.len());
            for input in &extension.inputs {
                let input_plan = from_substrait_rel(ctx, input, extensions).await?;
                inputs.push(input_plan);
            }
            let plan = plan.with_exprs_and_inputs(plan.expressions(), inputs)?;
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::Exchange(exchange)) => {
            let Some(input) = exchange.input.as_ref() else {
                return substrait_err!("Unexpected empty input in ExchangeRel");
            };
            let input = Arc::new(from_substrait_rel(ctx, input, extensions).await?);

            let Some(exchange_kind) = &exchange.exchange_kind else {
                return substrait_err!("Unexpected empty input in ExchangeRel");
            };

            // ref: https://substrait.io/relations/physical_relations/#exchange-types
            let partitioning_scheme = match exchange_kind {
                ExchangeKind::ScatterByFields(scatter_fields) => {
                    let mut partition_columns = vec![];
                    let input_schema = input.schema();
                    for field_ref in &scatter_fields.fields {
                        let column =
                            from_substrait_field_reference(field_ref, input_schema)?;
                        partition_columns.push(column);
                    }
                    Partitioning::Hash(
                        partition_columns,
                        exchange.partition_count as usize,
                    )
                }
                ExchangeKind::RoundRobin(_) => {
                    Partitioning::RoundRobinBatch(exchange.partition_count as usize)
                }
                ExchangeKind::SingleTarget(_)
                | ExchangeKind::MultiTarget(_)
                | ExchangeKind::Broadcast(_) => {
                    return not_impl_err!("Unsupported exchange kind: {exchange_kind:?}");
                }
            };
            Ok(LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }))
        }
        _ => not_impl_err!("Unsupported RelType: {:?}", rel.rel_type),
    }
}

/// Ensures that the given Substrait schema is compatible with the schema as given by DataFusion
///
/// This means:
/// 1. All fields present in the Substrait schema are present in the DataFusion schema. The
///    DataFusion schema may have MORE fields, but not the other way around.
/// 2. All fields are compatible. See [`ensure_field_compatability`] for details
///
/// This function returns a DataFrame with fields adjusted if necessary in the event that the
/// Substrait schema is a subset of the DataFusion schema.
fn ensure_schema_compatability(
    table: DataFrame,
    substrait_schema: DFSchema,
) -> Result<DataFrame> {
    let df_schema = table.schema().to_owned().strip_qualifiers();
    if df_schema.logically_equivalent_names_and_types(&substrait_schema) {
        return Ok(table);
    }
    let selected_columns = substrait_schema
        .strip_qualifiers()
        .fields()
        .iter()
        .map(|substrait_field| {
            let df_field =
                df_schema.field_with_unqualified_name(substrait_field.name())?;
            ensure_field_compatability(df_field, substrait_field)?;
            Ok(col(format!("\"{}\"", df_field.name())))
        })
        .collect::<Result<_>>()?;

    table.select(selected_columns)
}

/// Ensures that the given Substrait field is compatible with the given DataFusion field
///
/// A field is compatible between Substrait and DataFusion if:
/// 1. They have logically equivalent types.
/// 2. They have the same nullability OR the Substrait field is nullable and the DataFusion fields
///    is not nullable.
///
/// If a Substrait field is not nullable, the Substrait plan may be built around assuming it is not
/// nullable. As such if DataFusion has that field as nullable the plan should be rejected.
fn ensure_field_compatability(
    datafusion_field: &Field,
    substrait_field: &Field,
) -> Result<()> {
    if !DFSchema::datatype_is_logically_equal(
        datafusion_field.data_type(),
        substrait_field.data_type(),
    ) {
        return substrait_err!(
            "Field '{}' in Substrait schema has a different type ({}) than the corresponding field in the table schema ({}).",
            substrait_field.name(),
            substrait_field.data_type(),
            datafusion_field.data_type()
        );
    }

    if !compatible_nullabilities(
        datafusion_field.is_nullable(),
        substrait_field.is_nullable(),
    ) {
        // TODO: from_substrait_struct_type needs to be updated to set the nullability correctly. It defaults to true for now.
        return substrait_err!(
            "Field '{}' is nullable in the DataFusion schema but not nullable in the Substrait schema.",
            substrait_field.name()
        );
    }
    Ok(())
}

/// Returns true if the DataFusion and Substrait nullabilities are compatible, false otherwise
fn compatible_nullabilities(
    datafusion_nullability: bool,
    substrait_nullability: bool,
) -> bool {
    // DataFusion and Substrait have the same nullability
    (datafusion_nullability == substrait_nullability)
    // DataFusion is not nullable and Substrait is nullable
     || (!datafusion_nullability && substrait_nullability)
}

/// (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
/// conflict with the columns from the other.  
/// Substrait doesn't currently allow specifying aliases, neither for columns nor for tables. For
/// Substrait the names don't matter since it only refers to columns by indices, however DataFusion
/// requires columns to be uniquely identifiable, in some places (see e.g. DFSchema::check_names).
fn requalify_sides_if_needed(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
) -> Result<(LogicalPlanBuilder, LogicalPlanBuilder)> {
    let left_cols = left.schema().columns();
    let right_cols = right.schema().columns();
    if left_cols.iter().any(|l| {
        right_cols.iter().any(|r| {
            l == r || (l.name == r.name && (l.relation.is_none() || r.relation.is_none()))
        })
    }) {
        // These names have no connection to the original plan, but they'll make the columns
        // (mostly) unique. There may be cases where this still causes duplicates, if either left
        // or right side itself contains duplicate names with different qualifiers.
        Ok((
            left.alias(TableReference::bare("left"))?,
            right.alias(TableReference::bare("right"))?,
        ))
    } else {
        Ok((left, right))
    }
}

fn from_substrait_jointype(join_type: i32) -> Result<JoinType> {
    if let Ok(substrait_join_type) = join_rel::JoinType::try_from(join_type) {
        match substrait_join_type {
            join_rel::JoinType::Inner => Ok(JoinType::Inner),
            join_rel::JoinType::Left => Ok(JoinType::Left),
            join_rel::JoinType::Right => Ok(JoinType::Right),
            join_rel::JoinType::Outer => Ok(JoinType::Full),
            join_rel::JoinType::LeftAnti => Ok(JoinType::LeftAnti),
            join_rel::JoinType::LeftSemi => Ok(JoinType::LeftSemi),
            _ => plan_err!("unsupported join type {substrait_join_type:?}"),
        }
    } else {
        plan_err!("invalid join type variant {join_type:?}")
    }
}

/// Convert Substrait Sorts to DataFusion Exprs
pub async fn from_substrait_sorts(
    ctx: &SessionContext,
    substrait_sorts: &Vec<SortField>,
    input_schema: &DFSchema,
    extensions: &Extensions,
) -> Result<Vec<Sort>> {
    let mut sorts: Vec<Sort> = vec![];
    for s in substrait_sorts {
        let expr =
            from_substrait_rex(ctx, s.expr.as_ref().unwrap(), input_schema, extensions)
                .await?;
        let asc_nullfirst = match &s.sort_kind {
            Some(k) => match k {
                Direction(d) => {
                    let Ok(direction) = SortDirection::try_from(*d) else {
                        return not_impl_err!(
                            "Unsupported Substrait SortDirection value {d}"
                        );
                    };

                    match direction {
                        SortDirection::AscNullsFirst => Ok((true, true)),
                        SortDirection::AscNullsLast => Ok((true, false)),
                        SortDirection::DescNullsFirst => Ok((false, true)),
                        SortDirection::DescNullsLast => Ok((false, false)),
                        SortDirection::Clustered => not_impl_err!(
                            "Sort with direction clustered is not yet supported"
                        ),
                        SortDirection::Unspecified => {
                            not_impl_err!("Unspecified sort direction is invalid")
                        }
                    }
                }
                ComparisonFunctionReference(_) => not_impl_err!(
                    "Sort using comparison function reference is not supported"
                ),
            },
            None => not_impl_err!("Sort without sort kind is invalid"),
        };
        let (asc, nulls_first) = asc_nullfirst.unwrap();
        sorts.push(Sort {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(sorts)
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    ctx: &SessionContext,
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
    extensions: &Extensions,
) -> Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = from_substrait_rex(ctx, expr, input_schema, extensions).await?;
        expressions.push(expression);
    }
    Ok(expressions)
}

/// Convert Substrait FunctionArguments to DataFusion Exprs
pub async fn from_substrait_func_args(
    ctx: &SessionContext,
    arguments: &Vec<FunctionArgument>,
    input_schema: &DFSchema,
    extensions: &Extensions,
) -> Result<Vec<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => {
                from_substrait_rex(ctx, e, input_schema, extensions).await
            }
            _ => not_impl_err!("Function argument non-Value type not supported"),
        };
        args.push(arg_expr?);
    }
    Ok(args)
}

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    ctx: &SessionContext,
    f: &AggregateFunction,
    input_schema: &DFSchema,
    extensions: &Extensions,
    filter: Option<Box<Expr>>,
    order_by: Option<Vec<SortExpr>>,
    distinct: bool,
) -> Result<Arc<Expr>> {
    let args =
        from_substrait_func_args(ctx, &f.arguments, input_schema, extensions).await?;

    let Some(function_name) = extensions.functions.get(&f.function_reference) else {
        return plan_err!(
            "Aggregate function not registered: function anchor = {:?}",
            f.function_reference
        );
    };

    let function_name = substrait_fun_name(function_name);
    // try udaf first, then built-in aggr fn.
    if let Ok(fun) = ctx.udaf(function_name) {
        // deal with situation that count(*) got no arguments
        let args = if fun.name() == "count" && args.is_empty() {
            vec![Expr::Literal(ScalarValue::Int64(Some(1)))]
        } else {
            args
        };

        Ok(Arc::new(Expr::AggregateFunction(
            expr::AggregateFunction::new_udf(fun, args, distinct, filter, order_by, None),
        )))
    } else {
        not_impl_err!(
            "Aggregate function {} is not supported: function anchor = {:?}",
            function_name,
            f.function_reference
        )
    }
}

/// Convert Substrait Rex to DataFusion Expr
#[async_recursion]
pub async fn from_substrait_rex(
    ctx: &SessionContext,
    e: &Expression,
    input_schema: &DFSchema,
    extensions: &Extensions,
) -> Result<Expr> {
    match &e.rex_type {
        Some(RexType::SingularOrList(s)) => {
            let substrait_expr = s.value.as_ref().unwrap();
            let substrait_list = s.options.as_ref();
            Ok(Expr::InList(InList {
                expr: Box::new(
                    from_substrait_rex(ctx, substrait_expr, input_schema, extensions)
                        .await?,
                ),
                list: from_substrait_rex_vec(
                    ctx,
                    substrait_list,
                    input_schema,
                    extensions,
                )
                .await?,
                negated: false,
            }))
        }
        Some(RexType::Selection(field_ref)) => {
            Ok(from_substrait_field_reference(field_ref, input_schema)?)
        }
        Some(RexType::IfThen(if_then)) => {
            // Parse `ifs`
            // If the first element does not have a `then` part, then we can assume it's a base expression
            let mut when_then_expr: Vec<(Box<Expr>, Box<Expr>)> = vec![];
            let mut expr = None;
            for (i, if_expr) in if_then.ifs.iter().enumerate() {
                if i == 0 {
                    // Check if the first element is type base expression
                    if if_expr.then.is_none() {
                        expr = Some(Box::new(
                            from_substrait_rex(
                                ctx,
                                if_expr.r#if.as_ref().unwrap(),
                                input_schema,
                                extensions,
                            )
                            .await?,
                        ));
                        continue;
                    }
                }
                when_then_expr.push((
                    Box::new(
                        from_substrait_rex(
                            ctx,
                            if_expr.r#if.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?,
                    ),
                    Box::new(
                        from_substrait_rex(
                            ctx,
                            if_expr.then.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?,
                    ),
                ));
            }
            // Parse `else`
            let else_expr = match &if_then.r#else {
                Some(e) => Some(Box::new(
                    from_substrait_rex(ctx, e, input_schema, extensions).await?,
                )),
                None => None,
            };
            Ok(Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }))
        }
        Some(RexType::ScalarFunction(f)) => {
            let Some(fn_name) = extensions.functions.get(&f.function_reference) else {
                return plan_err!(
                    "Scalar function not found: function reference = {:?}",
                    f.function_reference
                );
            };
            let fn_name = substrait_fun_name(fn_name);

            let args =
                from_substrait_func_args(ctx, &f.arguments, input_schema, extensions)
                    .await?;

            // try to first match the requested function into registered udfs, then built-in ops
            // and finally built-in expressions
            if let Some(func) = ctx.state().scalar_functions().get(fn_name) {
                Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
                    func.to_owned(),
                    args,
                )))
            } else if let Some(op) = name_to_op(fn_name) {
                if f.arguments.len() < 2 {
                    return not_impl_err!(
                        "Expect at least two arguments for binary operator {op:?}, the provided number of operators is {:?}",
                        f.arguments.len()
                    );
                }
                // Some expressions are binary in DataFusion but take in a variadic number of args in Substrait.
                // In those cases we iterate through all the arguments, applying the binary expression against them all
                let combined_expr = args
                    .into_iter()
                    .fold(None, |combined_expr: Option<Expr>, arg: Expr| {
                        Some(match combined_expr {
                            Some(expr) => Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(expr),
                                op,
                                right: Box::new(arg),
                            }),
                            None => arg,
                        })
                    })
                    .unwrap();

                Ok(combined_expr)
            } else if let Some(builder) = BuiltinExprBuilder::try_from_name(fn_name) {
                builder.build(ctx, f, input_schema, extensions).await
            } else {
                not_impl_err!("Unsupported function name: {fn_name:?}")
            }
        }
        Some(RexType::Literal(lit)) => {
            let scalar_value = from_substrait_literal_without_names(lit, extensions)?;
            Ok(Expr::Literal(scalar_value))
        }
        Some(RexType::Cast(cast)) => match cast.as_ref().r#type.as_ref() {
            Some(output_type) => Ok(Expr::Cast(Cast::new(
                Box::new(
                    from_substrait_rex(
                        ctx,
                        cast.as_ref().input.as_ref().unwrap().as_ref(),
                        input_schema,
                        extensions,
                    )
                    .await?,
                ),
                from_substrait_type_without_names(output_type, extensions)?,
            ))),
            None => substrait_err!("Cast expression without output type is not allowed"),
        },
        Some(RexType::WindowFunction(window)) => {
            let Some(fn_name) = extensions.functions.get(&window.function_reference)
            else {
                return plan_err!(
                    "Window function not found: function reference = {:?}",
                    window.function_reference
                );
            };
            let fn_name = substrait_fun_name(fn_name);

            // check udwf first, then udaf, then built-in window and aggregate functions
            let fun = if let Ok(udwf) = ctx.udwf(fn_name) {
                Ok(WindowFunctionDefinition::WindowUDF(udwf))
            } else if let Ok(udaf) = ctx.udaf(fn_name) {
                Ok(WindowFunctionDefinition::AggregateUDF(udaf))
            } else if let Some(fun) = find_df_window_func(fn_name) {
                Ok(fun)
            } else {
                not_impl_err!(
                    "Window function {} is not supported: function anchor = {:?}",
                    fn_name,
                    window.function_reference
                )
            }?;

            let order_by =
                from_substrait_sorts(ctx, &window.sorts, input_schema, extensions)
                    .await?;

            let bound_units =
                match BoundsType::try_from(window.bounds_type).map_err(|e| {
                    plan_datafusion_err!("Invalid bound type {}: {e}", window.bounds_type)
                })? {
                    BoundsType::Rows => WindowFrameUnits::Rows,
                    BoundsType::Range => WindowFrameUnits::Range,
                    BoundsType::Unspecified => {
                        // If the plan does not specify the bounds type, then we use a simple logic to determine the units
                        // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
                        // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
                        if order_by.is_empty() {
                            WindowFrameUnits::Rows
                        } else {
                            WindowFrameUnits::Range
                        }
                    }
                };
            Ok(Expr::WindowFunction(expr::WindowFunction {
                fun,
                args: from_substrait_func_args(
                    ctx,
                    &window.arguments,
                    input_schema,
                    extensions,
                )
                .await?,
                partition_by: from_substrait_rex_vec(
                    ctx,
                    &window.partitions,
                    input_schema,
                    extensions,
                )
                .await?,
                order_by,
                window_frame: datafusion::logical_expr::WindowFrame::new_bounds(
                    bound_units,
                    from_substrait_bound(&window.lower_bound, true)?,
                    from_substrait_bound(&window.upper_bound, false)?,
                ),
                null_treatment: None,
            }))
        }
        Some(RexType::Subquery(subquery)) => match &subquery.as_ref().subquery_type {
            Some(subquery_type) => match subquery_type {
                SubqueryType::InPredicate(in_predicate) => {
                    if in_predicate.needles.len() != 1 {
                        substrait_err!("InPredicate Subquery type must have exactly one Needle expression")
                    } else {
                        let needle_expr = &in_predicate.needles[0];
                        let haystack_expr = &in_predicate.haystack;
                        if let Some(haystack_expr) = haystack_expr {
                            let haystack_expr =
                                from_substrait_rel(ctx, haystack_expr, extensions)
                                    .await?;
                            let outer_refs = haystack_expr.all_out_ref_exprs();
                            Ok(Expr::InSubquery(InSubquery {
                                expr: Box::new(
                                    from_substrait_rex(
                                        ctx,
                                        needle_expr,
                                        input_schema,
                                        extensions,
                                    )
                                    .await?,
                                ),
                                subquery: Subquery {
                                    subquery: Arc::new(haystack_expr),
                                    outer_ref_columns: outer_refs,
                                },
                                negated: false,
                            }))
                        } else {
                            substrait_err!("InPredicate Subquery type must have a Haystack expression")
                        }
                    }
                }
                SubqueryType::Scalar(query) => {
                    let plan = from_substrait_rel(
                        ctx,
                        &(query.input.clone()).unwrap_or_default(),
                        extensions,
                    )
                    .await?;
                    let outer_ref_columns = plan.all_out_ref_exprs();
                    Ok(Expr::ScalarSubquery(Subquery {
                        subquery: Arc::new(plan),
                        outer_ref_columns,
                    }))
                }
                SubqueryType::SetPredicate(predicate) => {
                    match predicate.predicate_op() {
                        // exist
                        PredicateOp::Exists => {
                            let relation = &predicate.tuples;
                            let plan = from_substrait_rel(
                                ctx,
                                &relation.clone().unwrap_or_default(),
                                extensions,
                            )
                            .await?;
                            let outer_ref_columns = plan.all_out_ref_exprs();
                            Ok(Expr::Exists(Exists::new(
                                Subquery {
                                    subquery: Arc::new(plan),
                                    outer_ref_columns,
                                },
                                false,
                            )))
                        }
                        other_type => substrait_err!(
                            "unimplemented type {:?} for set predicate",
                            other_type
                        ),
                    }
                }
                other_type => {
                    substrait_err!("Subquery type {:?} not implemented", other_type)
                }
            },
            None => {
                substrait_err!("Subquery expression without SubqueryType is not allowed")
            }
        },
        _ => not_impl_err!("unsupported rex_type"),
    }
}

pub(crate) fn from_substrait_type_without_names(
    dt: &Type,
    extensions: &Extensions,
) -> Result<DataType> {
    from_substrait_type(dt, extensions, &[], &mut 0)
}

fn from_substrait_type(
    dt: &Type,
    extensions: &Extensions,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt8),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt16),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt32),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            r#type::Kind::Timestamp(ts) => {
                // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
                #[allow(deprecated)]
                match ts.type_variation_reference {
                    TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Second, None))
                    }
                    TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                    }
                    TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                    TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    }
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    ),
                }
            }
            r#type::Kind::PrecisionTimestamp(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond),
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => not_impl_err!(
                        "Unsupported Substrait precision {p} for PrecisionTimestamp"
                    ),
                }?;
                Ok(DataType::Timestamp(unit, None))
            }
            r#type::Kind::PrecisionTimestampTz(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond),
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => not_impl_err!(
                        "Unsupported Substrait precision {p} for PrecisionTimestampTz"
                    ),
                }?;
                Ok(DataType::Timestamp(unit, Some(DEFAULT_TIMEZONE.into())))
            }
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(DataType::Date32),
                DATE_64_TYPE_VARIATION_REF => Ok(DataType::Date64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::BinaryView),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length))
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8View),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::List(list) => {
                let inner_type = list.r#type.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("List type must have inner type")
                })?;
                let field = Arc::new(Field::new_list_field(
                    from_substrait_type(inner_type, extensions, dfs_names, name_idx)?,
                    // We ignore Substrait's nullability here to match to_substrait_literal
                    // which always creates nullable lists
                    true,
                ));
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::List(field)),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeList(field)),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Map(map) => {
                let key_type = map.key.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have key type")
                })?;
                let value_type = map.value.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have value type")
                })?;
                let key_field = Arc::new(Field::new(
                    "key",
                    from_substrait_type(key_type, extensions, dfs_names, name_idx)?,
                    false,
                ));
                let value_field = Arc::new(Field::new(
                    "value",
                    from_substrait_type(value_type, extensions, dfs_names, name_idx)?,
                    true,
                ));
                Ok(DataType::Map(
                    Arc::new(Field::new_struct(
                        "entries",
                        [key_field, value_field],
                        false, // The inner map field is always non-nullable (Arrow #1697),
                    )),
                    false, // whether keys are sorted
                ))
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8))
                }
                DECIMAL_256_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::IntervalYear(_) => {
                Ok(DataType::Interval(IntervalUnit::YearMonth))
            }
            r#type::Kind::IntervalDay(_) => Ok(DataType::Interval(IntervalUnit::DayTime)),
            r#type::Kind::UserDefined(u) => {
                if let Some(name) = extensions.types.get(&u.type_reference) {
                    match name.as_ref() {
                        INTERVAL_MONTH_DAY_NANO_TYPE_NAME => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
                            _ => not_impl_err!(
                                "Unsupported Substrait user defined type with ref {} and variation {}",
                                u.type_reference,
                                u.type_variation_reference
                            ),
                    }
                } else {
                    // Kept for backwards compatibility, new plans should include the extension instead
                    #[allow(deprecated)]
                    match u.type_reference {
                        // Kept for backwards compatibility, use IntervalYear instead
                        INTERVAL_YEAR_MONTH_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::YearMonth))
                        }
                        // Kept for backwards compatibility, use IntervalDay instead
                        INTERVAL_DAY_TIME_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::DayTime))
                        }
                        // Not supported yet by Substrait
                        INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                        }
                        _ => not_impl_err!(
                        "Unsupported Substrait user defined type with ref {} and variation {}",
                        u.type_reference,
                        u.type_variation_reference
                    ),
                    }
                }
            }
            r#type::Kind::Struct(s) => Ok(DataType::Struct(from_substrait_struct_type(
                s, extensions, dfs_names, name_idx,
            )?)),
            r#type::Kind::Varchar(_) => Ok(DataType::Utf8),
            r#type::Kind::FixedChar(_) => Ok(DataType::Utf8),
            _ => not_impl_err!("Unsupported Substrait type: {s_kind:?}"),
        },
        _ => not_impl_err!("`None` Substrait kind is not supported"),
    }
}

fn from_substrait_struct_type(
    s: &r#type::Struct,
    extensions: &Extensions,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<Fields> {
    let mut fields = vec![];
    for (i, f) in s.types.iter().enumerate() {
        let field = Field::new(
            next_struct_field_name(i, dfs_names, name_idx)?,
            from_substrait_type(f, extensions, dfs_names, name_idx)?,
            true, // We assume everything to be nullable since that's easier than ensuring it matches
        );
        fields.push(field);
    }
    Ok(fields.into())
}

fn next_struct_field_name(
    i: usize,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<String> {
    if dfs_names.is_empty() {
        // If names are not given, create dummy names
        // c0, c1, ... align with e.g. SqlToRel::create_named_struct
        Ok(format!("c{i}"))
    } else {
        let name = dfs_names.get(*name_idx).cloned().ok_or_else(|| {
            substrait_datafusion_err!("Named schema must contain names for all fields")
        })?;
        *name_idx += 1;
        Ok(name)
    }
}

/// Convert Substrait NamedStruct to DataFusion DFSchemaRef
pub fn from_substrait_named_struct(
    base_schema: &NamedStruct,
    extensions: &Extensions,
) -> Result<DFSchema> {
    let mut name_idx = 0;
    let fields = from_substrait_struct_type(
        base_schema.r#struct.as_ref().ok_or_else(|| {
            substrait_datafusion_err!("Named struct must contain a struct")
        })?,
        extensions,
        &base_schema.names,
        &mut name_idx,
    );
    if name_idx != base_schema.names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            base_schema.names.len()
        );
    }
    DFSchema::try_from(Schema::new(fields?))
}

fn from_substrait_bound(
    bound: &Option<Bound>,
    is_lower: bool,
) -> Result<WindowFrameBound> {
    match bound {
        Some(b) => match &b.kind {
            Some(k) => match k {
                BoundKind::CurrentRow(SubstraitBound::CurrentRow {}) => {
                    Ok(WindowFrameBound::CurrentRow)
                }
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Preceding bound must be positive");
                    }
                    Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Following(SubstraitBound::Following { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Following bound must be positive");
                    }
                    Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => substrait_err!("WindowFunction missing Substrait Bound kind"),
        },
        None => {
            if is_lower {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            } else {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
        }
    }
}

pub(crate) fn from_substrait_literal_without_names(
    lit: &Literal,
    extensions: &Extensions,
) -> Result<ScalarValue> {
    from_substrait_literal(lit, extensions, &vec![], &mut 0)
}

fn from_substrait_literal(
    lit: &Literal,
    extensions: &Extensions,
    dfs_names: &Vec<String>,
    name_idx: &mut usize,
) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => {
            // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
            #[allow(deprecated)]
            match lit.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampSecond(Some(*t), None)
                }
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMillisecond(Some(*t), None)
                }
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMicrosecond(Some(*t), None)
                }
                TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampNanosecond(Some(*t), None)
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::PrecisionTimestamp(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(Some(pt.value), None),
            3 => ScalarValue::TimestampMillisecond(Some(pt.value), None),
            6 => ScalarValue::TimestampMicrosecond(Some(pt.value), None),
            9 => ScalarValue::TimestampNanosecond(Some(pt.value), None),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::PrecisionTimestampTz(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            3 => ScalarValue::TimestampMillisecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            6 => ScalarValue::TimestampMicrosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            9 => ScalarValue::TimestampNanosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8View(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => {
                ScalarValue::LargeBinary(Some(b.clone()))
            }
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::BinaryView(Some(b.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d
                .value
                .clone()
                .try_into()
                .or(substrait_err!("Failed to parse decimal value"))?;
            let p = d.precision.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal precision: {e}")
            })?;
            let s = d.scale.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal scale: {e}")
            })?;
            ScalarValue::Decimal128(
                Some(std::primitive::i128::from_le_bytes(value)),
                p,
                s,
            )
        }
        Some(LiteralType::List(l)) => {
            // Each element should start the name index from the same value, then we increase it
            // once at the end
            let mut element_name_idx = *name_idx;
            let elements = l
                .values
                .iter()
                .map(|el| {
                    element_name_idx = *name_idx;
                    from_substrait_literal(
                        el,
                        extensions,
                        dfs_names,
                        &mut element_name_idx,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            *name_idx = element_name_idx;
            if elements.is_empty() {
                return substrait_err!(
                    "Empty list must be encoded as EmptyList literal type, not List"
                );
            }
            let element_type = elements[0].data_type();
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::List(
                    ScalarValue::new_list_nullable(elements.as_slice(), &element_type),
                ),
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(elements.as_slice(), &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::EmptyList(l)) => {
            let element_type = from_substrait_type(
                l.r#type.clone().unwrap().as_ref(),
                extensions,
                dfs_names,
                name_idx,
            )?;
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => {
                    ScalarValue::List(ScalarValue::new_list_nullable(&[], &element_type))
                }
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(&[], &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::Map(m)) => {
            // Each entry should start the name index from the same value, then we increase it
            // once at the end
            let mut entry_name_idx = *name_idx;
            let entries = m
                .key_values
                .iter()
                .map(|kv| {
                    entry_name_idx = *name_idx;
                    let key_sv = from_substrait_literal(
                        kv.key.as_ref().unwrap(),
                        extensions,
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    let value_sv = from_substrait_literal(
                        kv.value.as_ref().unwrap(),
                        extensions,
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    ScalarStructBuilder::new()
                        .with_scalar(Field::new("key", key_sv.data_type(), false), key_sv)
                        .with_scalar(
                            Field::new("value", value_sv.data_type(), true),
                            value_sv,
                        )
                        .build()
                })
                .collect::<Result<Vec<_>>>()?;
            *name_idx = entry_name_idx;

            if entries.is_empty() {
                return substrait_err!(
                    "Empty map must be encoded as EmptyMap literal type, not Map"
                );
            }

            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries[0].data_type(), false)),
                OffsetBuffer::new(vec![0, entries.len() as i32].into()),
                ScalarValue::iter_to_array(entries)?.as_struct().to_owned(),
                None,
                false,
            )))
        }
        Some(LiteralType::EmptyMap(m)) => {
            let key = match &m.key {
                Some(k) => Ok(k),
                _ => plan_err!("Missing key type for empty map"),
            }?;
            let value = match &m.value {
                Some(v) => Ok(v),
                _ => plan_err!("Missing value type for empty map"),
            }?;
            let key_type = from_substrait_type(key, extensions, dfs_names, name_idx)?;
            let value_type = from_substrait_type(value, extensions, dfs_names, name_idx)?;

            // new_empty_array on a MapType creates a too empty array
            // We want it to contain an empty struct array to align with an empty MapBuilder one
            let entries = Field::new_struct(
                "entries",
                vec![
                    Field::new("key", key_type, false),
                    Field::new("value", value_type, true),
                ],
                false,
            );
            let struct_array =
                new_empty_array(entries.data_type()).as_struct().to_owned();
            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(entries),
                OffsetBuffer::new(vec![0, 0].into()),
                struct_array,
                None,
                false,
            )))
        }
        Some(LiteralType::Struct(s)) => {
            let mut builder = ScalarStructBuilder::new();
            for (i, field) in s.fields.iter().enumerate() {
                let name = next_struct_field_name(i, dfs_names, name_idx)?;
                let sv = from_substrait_literal(field, extensions, dfs_names, name_idx)?;
                // We assume everything to be nullable, since Arrow's strict about things matching
                // and it's hard to match otherwise.
                builder = builder.with_scalar(Field::new(name, sv.data_type(), true), sv);
            }
            builder.build()?
        }
        Some(LiteralType::Null(ntype)) => {
            from_substrait_null(ntype, extensions, dfs_names, name_idx)?
        }
        Some(LiteralType::IntervalDayToSecond(IntervalDayToSecond {
            days,
            seconds,
            subseconds,
            precision_mode,
        })) => {
            // DF only supports millisecond precision, so for any more granular type we lose precision
            let milliseconds = match precision_mode {
                Some(PrecisionMode::Microseconds(ms)) => ms / 1000,
                None =>
                    if *subseconds != 0 {
                        return substrait_err!("Cannot set subseconds field of IntervalDayToSecond without setting precision");
                    } else {
                        0_i32
                    }
                Some(PrecisionMode::Precision(0)) => *subseconds as i32 * 1000,
                Some(PrecisionMode::Precision(3)) => *subseconds as i32,
                Some(PrecisionMode::Precision(6)) => (subseconds / 1000) as i32,
                Some(PrecisionMode::Precision(9)) => (subseconds / 1000 / 1000) as i32,
                _ => {
                    return not_impl_err!(
                    "Unsupported Substrait interval day to second precision mode: {precision_mode:?}")
                }
            };

            ScalarValue::new_interval_dt(*days, (seconds * 1000) + milliseconds)
        }
        Some(LiteralType::IntervalYearToMonth(IntervalYearToMonth { years, months })) => {
            ScalarValue::new_interval_ym(*years, *months)
        }
        Some(LiteralType::FixedChar(c)) => ScalarValue::Utf8(Some(c.clone())),
        Some(LiteralType::UserDefined(user_defined)) => {
            // Helper function to prevent duplicating this code - can be inlined once the non-extension path is removed
            let interval_month_day_nano =
                |user_defined: &UserDefined| -> Result<ScalarValue> {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval month day nano value is empty");
                    };
                    let value_slice: [u8; 16] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval month day nano value"
                            )
                        })?;
                    let months =
                        i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                    let days = i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                    let nanoseconds =
                        i64::from_le_bytes(value_slice[8..16].try_into().unwrap());
                    Ok(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds,
                        },
                    )))
                };

            if let Some(name) = extensions.types.get(&user_defined.type_reference) {
                match name.as_ref() {
                    INTERVAL_MONTH_DAY_NANO_TYPE_NAME => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                        "Unsupported Substrait user defined type with ref {} and name {}",
                        user_defined.type_reference,
                        name
                    )
                    }
                }
            } else {
                // Kept for backwards compatibility - new plans should include extension instead
                #[allow(deprecated)]
                match user_defined.type_reference {
                    // Kept for backwards compatibility, use IntervalYearToMonth instead
                    INTERVAL_YEAR_MONTH_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval year month value is empty");
                        };
                        let value_slice: [u8; 4] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval year month value"
                                )
                            })?;
                        ScalarValue::IntervalYearMonth(Some(i32::from_le_bytes(
                            value_slice,
                        )))
                    }
                    // Kept for backwards compatibility, use IntervalDayToSecond instead
                    INTERVAL_DAY_TIME_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval day time value is empty");
                        };
                        let value_slice: [u8; 8] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval day time value"
                                )
                            })?;
                        let days =
                            i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                        let milliseconds =
                            i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days,
                            milliseconds,
                        }))
                    }
                    INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                            "Unsupported Substrait user defined type literal with ref {}",
                            user_defined.type_reference
                        )
                    }
                }
            }
        }
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

fn from_substrait_null(
    null_type: &Type,
    extensions: &Extensions,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::Bool(_) => Ok(ScalarValue::Boolean(None)),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int8(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int16(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt16(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int32(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt32(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int64(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(ScalarValue::Float32(None)),
            r#type::Kind::Fp64(_) => Ok(ScalarValue::Float64(None)),
            r#type::Kind::Timestamp(ts) => {
                // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
                #[allow(deprecated)]
                match ts.type_variation_reference {
                    TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                        Ok(ScalarValue::TimestampSecond(None, None))
                    }
                    TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                        Ok(ScalarValue::TimestampMillisecond(None, None))
                    }
                    TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                        Ok(ScalarValue::TimestampMicrosecond(None, None))
                    }
                    TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                        Ok(ScalarValue::TimestampNanosecond(None, None))
                    }
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {kind:?}"
                    ),
                }
            }
            r#type::Kind::PrecisionTimestamp(pts) => match pts.precision {
                0 => Ok(ScalarValue::TimestampSecond(None, None)),
                3 => Ok(ScalarValue::TimestampMillisecond(None, None)),
                6 => Ok(ScalarValue::TimestampMicrosecond(None, None)),
                9 => Ok(ScalarValue::TimestampNanosecond(None, None)),
                p => not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                ),
            },
            r#type::Kind::PrecisionTimestampTz(pts) => match pts.precision {
                0 => Ok(ScalarValue::TimestampSecond(
                    None,
                    Some(DEFAULT_TIMEZONE.into()),
                )),
                3 => Ok(ScalarValue::TimestampMillisecond(
                    None,
                    Some(DEFAULT_TIMEZONE.into()),
                )),
                6 => Ok(ScalarValue::TimestampMicrosecond(
                    None,
                    Some(DEFAULT_TIMEZONE.into()),
                )),
                9 => Ok(ScalarValue::TimestampNanosecond(
                    None,
                    Some(DEFAULT_TIMEZONE.into()),
                )),
                p => not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(ScalarValue::Date32(None)),
                DATE_64_TYPE_VARIATION_REF => Ok(ScalarValue::Date64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::Binary(None)),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeBinary(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            // FixedBinary is not supported because `None` doesn't have length
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::Utf8(None)),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeUtf8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            r#type::Kind::List(l) => {
                let field = Field::new_list_field(
                    from_substrait_type(
                        l.r#type.clone().unwrap().as_ref(),
                        extensions,
                        dfs_names,
                        name_idx,
                    )?,
                    true,
                );
                match l.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::List(
                        Arc::new(GenericListArray::new_null(field.into(), 1)),
                    )),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeList(
                        Arc::new(GenericListArray::new_null(field.into(), 1)),
                    )),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {kind:?}"
                    ),
                }
            }
            r#type::Kind::Map(map) => {
                let key_type = map.key.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have key type")
                })?;
                let value_type = map.value.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have value type")
                })?;

                let key_type =
                    from_substrait_type(key_type, extensions, dfs_names, name_idx)?;
                let value_type =
                    from_substrait_type(value_type, extensions, dfs_names, name_idx)?;
                let entries_field = Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", value_type, true),
                    ],
                    false,
                ));

                DataType::Map(entries_field, false /* keys sorted */).try_into()
            }
            r#type::Kind::Struct(s) => {
                let fields =
                    from_substrait_struct_type(s, extensions, dfs_names, name_idx)?;
                Ok(ScalarStructBuilder::new_null(fields))
            }
            _ => not_impl_err!("Unsupported Substrait type for null: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}

fn from_substrait_field_reference(
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(StructField(x)) => match &x.child.as_ref() {
                Some(_) => not_impl_err!(
                    "Direct reference StructField with child is not supported"
                ),
                None => Ok(Expr::Column(Column::from(
                    input_schema.qualified_field(x.field as usize),
                ))),
            },
            _ => not_impl_err!(
                "Direct reference with types other than StructField is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}

/// Build [`Expr`] from its name and required inputs.
struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub async fn build(
        self,
        ctx: &SessionContext,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &Extensions,
    ) -> Result<Expr> {
        match self.expr_name.as_str() {
            "like" => {
                Self::build_like_expr(ctx, false, f, input_schema, extensions).await
            }
            "ilike" => {
                Self::build_like_expr(ctx, true, f, input_schema, extensions).await
            }
            "not" | "negative" | "is_null" | "is_not_null" | "is_true" | "is_false"
            | "is_not_true" | "is_not_false" | "is_unknown" | "is_not_unknown" => {
                Self::build_unary_expr(ctx, &self.expr_name, f, input_schema, extensions)
                    .await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(
        ctx: &SessionContext,
        fn_name: &str,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &Extensions,
    ) -> Result<Expr> {
        if f.arguments.len() != 1 {
            return substrait_err!("Expect one argument for {fn_name} expr");
        }
        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for {fn_name} expr");
        };
        let arg =
            from_substrait_rex(ctx, expr_substrait, input_schema, extensions).await?;
        let arg = Box::new(arg);

        let expr = match fn_name {
            "not" => Expr::Not(arg),
            "negative" => Expr::Negative(arg),
            "is_null" => Expr::IsNull(arg),
            "is_not_null" => Expr::IsNotNull(arg),
            "is_true" => Expr::IsTrue(arg),
            "is_false" => Expr::IsFalse(arg),
            "is_not_true" => Expr::IsNotTrue(arg),
            "is_not_false" => Expr::IsNotFalse(arg),
            "is_unknown" => Expr::IsUnknown(arg),
            "is_not_unknown" => Expr::IsNotUnknown(arg),
            _ => return not_impl_err!("Unsupported builtin expression: {}", fn_name),
        };

        Ok(expr)
    }

    async fn build_like_expr(
        ctx: &SessionContext,
        case_insensitive: bool,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &Extensions,
    ) -> Result<Expr> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if f.arguments.len() != 2 && f.arguments.len() != 3 {
            return substrait_err!("Expect two or three arguments for `{fn_name}` expr");
        }

        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let expr =
            from_substrait_rex(ctx, expr_substrait, input_schema, extensions).await?;
        let Some(ArgType::Value(pattern_substrait)) = &f.arguments[1].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let pattern =
            from_substrait_rex(ctx, pattern_substrait, input_schema, extensions).await?;

        // Default case: escape character is Literal(Utf8(None))
        let escape_char = if f.arguments.len() == 3 {
            let Some(ArgType::Value(escape_char_substrait)) = &f.arguments[2].arg_type
            else {
                return substrait_err!("Invalid arguments type for `{fn_name}` expr");
            };

            let escape_char_expr =
                from_substrait_rex(ctx, escape_char_substrait, input_schema, extensions)
                    .await?;

            match escape_char_expr {
                Expr::Literal(ScalarValue::Utf8(escape_char_string)) => {
                    // Convert Option<String> to Option<char>
                    escape_char_string.and_then(|s| s.chars().next())
                }
                _ => {
                    return substrait_err!(
                    "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
                )
                }
            }
        } else {
            None
        };

        Ok(Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char,
            case_insensitive,
        }))
    }
}

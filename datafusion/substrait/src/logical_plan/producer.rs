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

use itertools::Itertools;
use std::sync::Arc;

use arrow_buffer::ToByteSlice;
use datafusion::arrow::datatypes::IntervalUnit;
use datafusion::logical_expr::{
    CrossJoin, Distinct, Like, Partitioning, WindowFrameUnits,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    error::{DataFusionError, Result},
    logical_expr::{WindowFrame, WindowFrameBound},
    prelude::{JoinType, SessionContext},
    scalar::ScalarValue,
};

use crate::extensions::Extensions;
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    INTERVAL_MONTH_DAY_NANO_TYPE_NAME, LARGE_CONTAINER_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF, VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use datafusion::arrow::array::{Array, GenericListArray, OffsetSizeTrait};
use datafusion::common::{
    exec_err, internal_err, not_impl_err, plan_err, substrait_datafusion_err,
    substrait_err, DFSchemaRef, ToDFSchema,
};
#[allow(unused_imports)]
use datafusion::logical_expr::expr::{
    Alias, BinaryExpr, Case, Cast, GroupingSet, InList, InSubquery, Sort, WindowFunction,
};
use datafusion::logical_expr::{expr, Between, JoinConstraint, LogicalPlan, Operator};
use datafusion::prelude::Expr;
use pbjson_types::Any as ProtoAny;
use substrait::proto::exchange_rel::{ExchangeKind, RoundRobin, ScatterFields};
use substrait::proto::expression::literal::interval_day_to_second::PrecisionMode;
use substrait::proto::expression::literal::map::KeyValue;
use substrait::proto::expression::literal::{
    user_defined, IntervalDayToSecond, IntervalYearToMonth, List, Map,
    PrecisionTimestamp, Struct, UserDefined,
};
use substrait::proto::expression::subquery::InPredicate;
use substrait::proto::expression::window_function::BoundsType;
use substrait::proto::read_rel::VirtualTable;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::rel_common::EmitKind::Emit;
use substrait::proto::{rel_common, CrossRel, ExchangeRel, RelCommon};
use substrait::{
    proto::{
        aggregate_function::AggregationInvocation,
        aggregate_rel::{Grouping, Measure},
        expression::{
            field_reference::ReferenceType,
            if_then::IfClause,
            literal::{Decimal, LiteralType},
            mask_expression::{StructItem, StructSelect},
            reference_segment,
            window_function::bound as SubstraitBound,
            window_function::bound::Kind as BoundKind,
            window_function::Bound,
            FieldReference, IfThen, Literal, MaskExpression, ReferenceSegment, RexType,
            ScalarFunction, SingularOrList, Subquery,
            WindowFunction as SubstraitWindowFunction,
        },
        function_argument::ArgType,
        join_rel, plan_rel, r#type,
        read_rel::{NamedTable, ReadType},
        rel::RelType,
        set_rel,
        sort_field::{SortDirection, SortKind},
        AggregateFunction, AggregateRel, AggregationPhase, Expression, ExtensionLeafRel,
        ExtensionMultiRel, ExtensionSingleRel, FetchRel, FilterRel, FunctionArgument,
        JoinRel, NamedStruct, Plan, PlanRel, ProjectRel, ReadRel, Rel, RelRoot, SetRel,
        SortField, SortRel,
    },
    version,
};

/// Convert DataFusion LogicalPlan to Substrait Plan
pub fn to_substrait_plan(plan: &LogicalPlan, ctx: &SessionContext) -> Result<Box<Plan>> {
    let mut extensions = Extensions::default();
    // Parse relation nodes
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(RelRoot {
            input: Some(*to_substrait_rel(plan, ctx, &mut extensions)?),
            names: to_substrait_named_struct(plan.schema(), &mut extensions)?.names,
        })),
    }];

    // Return parsed plan
    Ok(Box::new(Plan {
        version: Some(version::version_with_producer("datafusion")),
        extension_uris: vec![],
        extensions: extensions.into(),
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
    }))
}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(
    plan: &LogicalPlan,
    ctx: &SessionContext,
    extensions: &mut Extensions,
) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let projection = scan.projection.as_ref().map(|p| {
                p.iter()
                    .map(|i| StructItem {
                        field: *i as i32,
                        child: None,
                    })
                    .collect()
            });

            let projection = projection.map(|struct_items| MaskExpression {
                select: Some(StructSelect { struct_items }),
                maintain_singular_struct: false,
            });

            let table_schema = scan.source.schema().to_dfschema_ref()?;
            let base_schema = to_substrait_named_struct(&table_schema, extensions)?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: Some(base_schema),
                    filter: None,
                    best_effort_filter: None,
                    projection,
                    advanced_extension: None,
                    read_type: Some(ReadType::NamedTable(NamedTable {
                        names: scan.table_name.to_vec(),
                        advanced_extension: None,
                    })),
                }))),
            }))
        }
        LogicalPlan::EmptyRelation(e) => {
            if e.produce_one_row {
                return not_impl_err!(
                    "Producing a row from empty relation is unsupported"
                );
            }
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: Some(to_substrait_named_struct(&e.schema, extensions)?),
                    filter: None,
                    best_effort_filter: None,
                    projection: None,
                    advanced_extension: None,
                    read_type: Some(ReadType::VirtualTable(VirtualTable {
                        values: vec![],
                    })),
                }))),
            }))
        }
        LogicalPlan::Values(v) => {
            let values = v
                .values
                .iter()
                .map(|row| {
                    let fields = row
                        .iter()
                        .map(|v| match v {
                            Expr::Literal(sv) => to_substrait_literal(sv, extensions),
                            Expr::Alias(alias) => match alias.expr.as_ref() {
                                // The schema gives us the names, so we can skip aliases
                                Expr::Literal(sv) => to_substrait_literal(sv, extensions),
                                _ => Err(substrait_datafusion_err!(
                                    "Only literal types can be aliased in Virtual Tables, got: {}", alias.expr.variant_name()
                                )),
                            },
                            _ => Err(substrait_datafusion_err!(
                                "Only literal types and aliases are supported in Virtual Tables, got: {}", v.variant_name()
                            )),
                        })
                        .collect::<Result<_>>()?;
                    Ok(Struct { fields })
                })
                .collect::<Result<_>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: Some(to_substrait_named_struct(&v.schema, extensions)?),
                    filter: None,
                    best_effort_filter: None,
                    projection: None,
                    advanced_extension: None,
                    read_type: Some(ReadType::VirtualTable(VirtualTable { values })),
                }))),
            }))
        }
        LogicalPlan::Projection(p) => {
            let expressions = p
                .expr
                .iter()
                .map(|e| to_substrait_rex(ctx, e, p.input.schema(), 0, extensions))
                .collect::<Result<Vec<_>>>()?;

            let emit_kind = create_project_remapping(
                expressions.len(),
                p.input.as_ref().schema().fields().len(),
            );
            let common = RelCommon {
                emit_kind: Some(emit_kind),
                hint: None,
                advanced_extension: None,
            };

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: Some(common),
                    input: Some(to_substrait_rel(p.input.as_ref(), ctx, extensions)?),
                    expressions,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let input = to_substrait_rel(filter.input.as_ref(), ctx, extensions)?;
            let filter_expr = to_substrait_rex(
                ctx,
                &filter.predicate,
                filter.input.schema(),
                0,
                extensions,
            )?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Filter(Box::new(FilterRel {
                    common: None,
                    input: Some(input),
                    condition: Some(Box::new(filter_expr)),
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Limit(limit) => {
            let input = to_substrait_rel(limit.input.as_ref(), ctx, extensions)?;
            // Since protobuf can't directly distinguish `None` vs `0` encode `None` as `MAX`
            let limit_fetch = limit.fetch.unwrap_or(usize::MAX);
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                    common: None,
                    input: Some(input),
                    offset: limit.skip as i64,
                    count: limit_fetch as i64,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Sort(sort) => {
            let input = to_substrait_rel(sort.input.as_ref(), ctx, extensions)?;
            let sort_fields = sort
                .expr
                .iter()
                .map(|e| substrait_sort_field(ctx, e, sort.input.schema(), extensions))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Sort(Box::new(SortRel {
                    common: None,
                    input: Some(input),
                    sorts: sort_fields,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Aggregate(agg) => {
            let input = to_substrait_rel(agg.input.as_ref(), ctx, extensions)?;
            let groupings = to_substrait_groupings(
                ctx,
                &agg.group_expr,
                agg.input.schema(),
                extensions,
            )?;
            let measures = agg
                .aggr_expr
                .iter()
                .map(|e| to_substrait_agg_measure(ctx, e, agg.input.schema(), extensions))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    groupings,
                    measures,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Distinct(Distinct::All(plan)) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = to_substrait_rel(plan.as_ref(), ctx, extensions)?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..plan.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                    }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Join(join) => {
            let left = to_substrait_rel(join.left.as_ref(), ctx, extensions)?;
            let right = to_substrait_rel(join.right.as_ref(), ctx, extensions)?;
            let join_type = to_substrait_jointype(join.join_type);
            // we only support basic joins so return an error for anything not yet supported
            match join.join_constraint {
                JoinConstraint::On => {}
                JoinConstraint::Using => {
                    return not_impl_err!("join constraint: `using`")
                }
            }
            // parse filter if exists
            let in_join_schema = join.left.schema().join(join.right.schema())?;
            let join_filter = match &join.filter {
                Some(filter) => Some(to_substrait_rex(
                    ctx,
                    filter,
                    &Arc::new(in_join_schema),
                    0,
                    extensions,
                )?),
                None => None,
            };

            // map the left and right columns to binary expressions in the form `l = r`
            // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
            let eq_op = if join.null_equals_null {
                Operator::IsNotDistinctFrom
            } else {
                Operator::Eq
            };
            let join_on = to_substrait_join_expr(
                ctx,
                &join.on,
                eq_op,
                join.left.schema(),
                join.right.schema(),
                extensions,
            )?;

            // create conjunction between `join_on` and `join_filter` to embed all join conditions,
            // whether equal or non-equal in a single expression
            let join_expr = match &join_on {
                Some(on_expr) => match &join_filter {
                    Some(filter) => Some(Box::new(make_binary_op_scalar_func(
                        on_expr,
                        filter,
                        Operator::And,
                        extensions,
                    ))),
                    None => join_on.map(Box::new), // the join expression will only contain `join_on` if filter doesn't exist
                },
                None => match &join_filter {
                    Some(_) => join_filter.map(Box::new), // the join expression will only contain `join_filter` if the `on` condition doesn't exist
                    None => None,
                },
            };

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Join(Box::new(JoinRel {
                    common: None,
                    left: Some(left),
                    right: Some(right),
                    r#type: join_type as i32,
                    expression: join_expr,
                    post_join_filter: None,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let CrossJoin {
                left,
                right,
                schema: _,
            } = cross_join;
            let left = to_substrait_rel(left.as_ref(), ctx, extensions)?;
            let right = to_substrait_rel(right.as_ref(), ctx, extensions)?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Cross(Box::new(CrossRel {
                    common: None,
                    left: Some(left),
                    right: Some(right),
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            // Do nothing if encounters SubqueryAlias
            // since there is no corresponding relation type in Substrait
            to_substrait_rel(alias.input.as_ref(), ctx, extensions)
        }
        LogicalPlan::Union(union) => {
            let input_rels = union
                .inputs
                .iter()
                .map(|input| to_substrait_rel(input.as_ref(), ctx, extensions))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|ptr| *ptr)
                .collect();
            Ok(Box::new(Rel {
                rel_type: Some(substrait::proto::rel::RelType::Set(SetRel {
                    common: None,
                    inputs: input_rels,
                    op: set_rel::SetOp::UnionAll as i32, // UNION DISTINCT gets translated to AGGREGATION + UNION ALL
                    advanced_extension: None,
                })),
            }))
        }
        LogicalPlan::Window(window) => {
            let input = to_substrait_rel(window.input.as_ref(), ctx, extensions)?;

            // create a field reference for each input field
            let mut expressions = (0..window.input.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<Result<Vec<_>>>()?;

            // process and add each window function expression
            for expr in &window.window_expr {
                expressions.push(to_substrait_rex(
                    ctx,
                    expr,
                    window.input.schema(),
                    0,
                    extensions,
                )?);
            }

            let emit_kind = create_project_remapping(
                expressions.len(),
                window.input.schema().fields().len(),
            );
            let common = RelCommon {
                emit_kind: Some(emit_kind),
                hint: None,
                advanced_extension: None,
            };
            let project_rel = Box::new(ProjectRel {
                common: Some(common),
                input: Some(input),
                expressions,
                advanced_extension: None,
            });

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(project_rel)),
            }))
        }
        LogicalPlan::Repartition(repartition) => {
            let input = to_substrait_rel(repartition.input.as_ref(), ctx, extensions)?;
            let partition_count = match repartition.partitioning_scheme {
                Partitioning::RoundRobinBatch(num) => num,
                Partitioning::Hash(_, num) => num,
                Partitioning::DistributeBy(_) => {
                    return not_impl_err!(
                        "Physical plan does not support DistributeBy partitioning"
                    )
                }
            };
            // ref: https://substrait.io/relations/physical_relations/#exchange-types
            let exchange_kind = match &repartition.partitioning_scheme {
                Partitioning::RoundRobinBatch(_) => {
                    ExchangeKind::RoundRobin(RoundRobin::default())
                }
                Partitioning::Hash(exprs, _) => {
                    let fields = exprs
                        .iter()
                        .map(|e| {
                            try_to_substrait_field_reference(
                                e,
                                repartition.input.schema(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    ExchangeKind::ScatterByFields(ScatterFields { fields })
                }
                Partitioning::DistributeBy(_) => {
                    return not_impl_err!(
                        "Physical plan does not support DistributeBy partitioning"
                    )
                }
            };
            let exchange_rel = ExchangeRel {
                common: None,
                input: Some(input),
                exchange_kind: Some(exchange_kind),
                advanced_extension: None,
                partition_count: partition_count as i32,
                targets: vec![],
            };
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Exchange(Box::new(exchange_rel))),
            }))
        }
        LogicalPlan::Extension(extension_plan) => {
            let extension_bytes = ctx
                .state()
                .serializer_registry()
                .serialize_logical_plan(extension_plan.node.as_ref())?;
            let detail = ProtoAny {
                type_url: extension_plan.node.name().to_string(),
                value: extension_bytes.into(),
            };
            let mut inputs_rel = extension_plan
                .node
                .inputs()
                .into_iter()
                .map(|plan| to_substrait_rel(plan, ctx, extensions))
                .collect::<Result<Vec<_>>>()?;
            let rel_type = match inputs_rel.len() {
                0 => RelType::ExtensionLeaf(ExtensionLeafRel {
                    common: None,
                    detail: Some(detail),
                }),
                1 => RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                    common: None,
                    detail: Some(detail),
                    input: Some(inputs_rel.pop().unwrap()),
                })),
                _ => RelType::ExtensionMulti(ExtensionMultiRel {
                    common: None,
                    detail: Some(detail),
                    inputs: inputs_rel.into_iter().map(|r| *r).collect(),
                }),
            };
            Ok(Box::new(Rel {
                rel_type: Some(rel_type),
            }))
        }
        _ => not_impl_err!("Unsupported operator: {plan}"),
    }
}

/// By default, a Substrait Project outputs all input fields followed by all expressions.
/// A DataFusion Projection only outputs expressions. In order to keep the Substrait
/// plan consistent with DataFusion, we must apply an output mapping that skips the input
/// fields so that the Substrait Project will only output the expression fields.
fn create_project_remapping(expr_count: usize, input_field_count: usize) -> EmitKind {
    let expression_field_start = input_field_count;
    let expression_field_end = expression_field_start + expr_count;
    let output_mapping = (expression_field_start..expression_field_end)
        .map(|i| i as i32)
        .collect();
    Emit(rel_common::Emit { output_mapping })
}

fn to_substrait_named_struct(
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<NamedStruct> {
    // Substrait wants a list of all field names, including nested fields from structs,
    // also from within e.g. lists and maps. However, it does not want the list and map field names
    // themselves - only proper structs fields are considered to have useful names.
    fn names_dfs(dtype: &DataType) -> Result<Vec<String>> {
        match dtype {
            DataType::Struct(fields) => {
                let mut names = Vec::new();
                for field in fields {
                    names.push(field.name().to_string());
                    names.extend(names_dfs(field.data_type())?);
                }
                Ok(names)
            }
            DataType::List(l) => names_dfs(l.data_type()),
            DataType::LargeList(l) => names_dfs(l.data_type()),
            DataType::Map(m, _) => match m.data_type() {
                DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                    let key_names =
                        names_dfs(key_and_value.first().unwrap().data_type())?;
                    let value_names =
                        names_dfs(key_and_value.last().unwrap().data_type())?;
                    Ok([key_names, value_names].concat())
                }
                _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
            },
            _ => Ok(Vec::new()),
        }
    }

    let names = schema
        .fields()
        .iter()
        .map(|f| {
            let mut names = vec![f.name().to_string()];
            names.extend(names_dfs(f.data_type())?);
            Ok(names)
        })
        .flatten_ok()
        .collect::<Result<_>>()?;

    let field_types = r#type::Struct {
        types: schema
            .fields()
            .iter()
            .map(|f| to_substrait_type(f.data_type(), f.is_nullable(), extensions))
            .collect::<Result<_>>()?,
        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
        nullability: r#type::Nullability::Unspecified as i32,
    };

    Ok(NamedStruct {
        names,
        r#struct: Some(field_types),
    })
}

fn to_substrait_join_expr(
    ctx: &SessionContext,
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: Operator,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<Option<Expression>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<Expression> = vec![];
    for (left, right) in join_conditions {
        // Parse left
        let l = to_substrait_rex(ctx, left, left_schema, 0, extensions)?;
        // Parse right
        let r = to_substrait_rex(
            ctx,
            right,
            right_schema,
            left_schema.fields().len(), // offset to return the correct index
            extensions,
        )?;
        // AND with existing expression
        exprs.push(make_binary_op_scalar_func(&l, &r, eq_op, extensions));
    }
    let join_expr: Option<Expression> =
        exprs.into_iter().reduce(|acc: Expression, e: Expression| {
            make_binary_op_scalar_func(&acc, &e, Operator::And, extensions)
        });
    Ok(join_expr)
}

fn to_substrait_jointype(join_type: JoinType) -> join_rel::JoinType {
    match join_type {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::LeftAnti => join_rel::JoinType::LeftAnti,
        JoinType::LeftSemi => join_rel::JoinType::LeftSemi,
        JoinType::RightAnti | JoinType::RightSemi => unimplemented!(),
    }
}

pub fn operator_to_name(op: Operator) -> &'static str {
    match op {
        Operator::Eq => "equal",
        Operator::NotEq => "not_equal",
        Operator::Lt => "lt",
        Operator::LtEq => "lte",
        Operator::Gt => "gt",
        Operator::GtEq => "gte",
        Operator::Plus => "add",
        Operator::Minus => "subtract",
        Operator::Multiply => "multiply",
        Operator::Divide => "divide",
        Operator::Modulo => "mod",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::IsDistinctFrom => "is_distinct_from",
        Operator::IsNotDistinctFrom => "is_not_distinct_from",
        Operator::RegexMatch => "regex_match",
        Operator::RegexIMatch => "regex_imatch",
        Operator::RegexNotMatch => "regex_not_match",
        Operator::RegexNotIMatch => "regex_not_imatch",
        Operator::LikeMatch => "like_match",
        Operator::ILikeMatch => "like_imatch",
        Operator::NotLikeMatch => "like_not_match",
        Operator::NotILikeMatch => "like_not_imatch",
        Operator::BitwiseAnd => "bitwise_and",
        Operator::BitwiseOr => "bitwise_or",
        Operator::StringConcat => "str_concat",
        Operator::AtArrow => "at_arrow",
        Operator::ArrowAt => "arrow_at",
        Operator::BitwiseXor => "bitwise_xor",
        Operator::BitwiseShiftRight => "bitwise_shift_right",
        Operator::BitwiseShiftLeft => "bitwise_shift_left",
    }
}

pub fn parse_flat_grouping_exprs(
    ctx: &SessionContext,
    exprs: &[Expr],
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<Grouping> {
    let grouping_expressions = exprs
        .iter()
        .map(|e| to_substrait_rex(ctx, e, schema, 0, extensions))
        .collect::<Result<Vec<_>>>()?;
    Ok(Grouping {
        grouping_expressions,
    })
}

pub fn to_substrait_groupings(
    ctx: &SessionContext,
    exprs: &[Expr],
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<Vec<Grouping>> {
    match exprs.len() {
        1 => match &exprs[0] {
            Expr::GroupingSet(gs) => match gs {
                GroupingSet::Cube(_) => Err(DataFusionError::NotImplemented(
                    "GroupingSet CUBE is not yet supported".to_string(),
                )),
                GroupingSet::GroupingSets(sets) => Ok(sets
                    .iter()
                    .map(|set| parse_flat_grouping_exprs(ctx, set, schema, extensions))
                    .collect::<Result<Vec<_>>>()?),
                GroupingSet::Rollup(set) => {
                    let mut sets: Vec<Vec<Expr>> = vec![vec![]];
                    for i in 0..set.len() {
                        sets.push(set[..=i].to_vec());
                    }
                    Ok(sets
                        .iter()
                        .rev()
                        .map(|set| {
                            parse_flat_grouping_exprs(ctx, set, schema, extensions)
                        })
                        .collect::<Result<Vec<_>>>()?)
                }
            },
            _ => Ok(vec![parse_flat_grouping_exprs(
                ctx, exprs, schema, extensions,
            )?]),
        },
        _ => Ok(vec![parse_flat_grouping_exprs(
            ctx, exprs, schema, extensions,
        )?]),
    }
}

#[allow(deprecated)]
pub fn to_substrait_agg_measure(
    ctx: &SessionContext,
    expr: &Expr,
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<Measure> {
    match expr {
        Expr::AggregateFunction(expr::AggregateFunction { func, args, distinct, filter, order_by, null_treatment: _, }) => {
                    let sorts = if let Some(order_by) = order_by {
                        order_by.iter().map(|expr| to_substrait_sort_field(ctx, expr, schema, extensions)).collect::<Result<Vec<_>>>()?
                    } else {
                        vec![]
                    };
                    let mut arguments: Vec<FunctionArgument> = vec![];
                    for arg in args {
                        arguments.push(FunctionArgument { arg_type: Some(ArgType::Value(to_substrait_rex(ctx, arg, schema, 0, extensions)?)) });
                    }
                    let function_anchor = extensions.register_function(func.name().to_string());
                    Ok(Measure {
                        measure: Some(AggregateFunction {
                            function_reference: function_anchor,
                            arguments,
                            sorts,
                            output_type: None,
                            invocation: match distinct {
                                true => AggregationInvocation::Distinct as i32,
                                false => AggregationInvocation::All as i32,
                            },
                            phase: AggregationPhase::Unspecified as i32,
                            args: vec![],
                            options: vec![],
                        }),
                        filter: match filter {
                            Some(f) => Some(to_substrait_rex(ctx, f, schema, 0, extensions)?),
                            None => None
                        }
                    })

        }
        Expr::Alias(Alias{expr,..})=> {
            to_substrait_agg_measure(ctx, expr, schema, extensions)
        }
        _ => internal_err!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. ExpressionType: {:?}",
            expr,
            expr.variant_name()
        ),
    }
}

/// Converts sort expression to corresponding substrait `SortField`
fn to_substrait_sort_field(
    ctx: &SessionContext,
    sort: &Sort,
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<SortField> {
    let sort_kind = match (sort.asc, sort.nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(to_substrait_rex(ctx, &sort.expr, schema, 0, extensions)?),
        sort_kind: Some(SortKind::Direction(sort_kind.into())),
    })
}

/// Return Substrait scalar function with two arguments
#[allow(deprecated)]
pub fn make_binary_op_scalar_func(
    lhs: &Expression,
    rhs: &Expression,
    op: Operator,
    extensions: &mut Extensions,
) -> Expression {
    let function_anchor = extensions.register_function(operator_to_name(op).to_string());
    Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                FunctionArgument {
                    arg_type: Some(ArgType::Value(lhs.clone())),
                },
                FunctionArgument {
                    arg_type: Some(ArgType::Value(rhs.clone())),
                },
            ],
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    }
}

/// Convert DataFusion Expr to Substrait Rex
///
/// # Arguments
///
/// * `expr` - DataFusion expression to be parse into a Substrait expression
/// * `schema` - DataFusion input schema for looking up field qualifiers
/// * `col_ref_offset` - Offset for calculating Substrait field reference indices.
///                     This should only be set by caller with more than one input relations i.e. Join.
///                     Substrait expects one set of indices when joining two relations.
///                     Let's say `left` and `right` have `m` and `n` columns, respectively. The `right`
///                     relation will have column indices from `0` to `n-1`, however, Substrait will expect
///                     the `right` indices to be offset by the `left`. This means Substrait will expect to
///                     evaluate the join condition expression on indices [0 .. n-1, n .. n+m-1]. For example:
///                     ```SELECT *
///                        FROM t1
///                        JOIN t2
///                        ON t1.c1 = t2.c0;```
///                     where t1 consists of columns [c0, c1, c2], and t2 = columns [c0, c1]
///                     the join condition should become
///                     `col_ref(1) = col_ref(3 + 0)`
///                     , where `3` is the number of `left` columns (`col_ref_offset`) and `0` is the index
///                     of the join key column from `right`
/// * `extensions` - Substrait extension info. Contains registered function information
#[allow(deprecated)]
pub fn to_substrait_rex(
    ctx: &SessionContext,
    expr: &Expr,
    schema: &DFSchemaRef,
    col_ref_offset: usize,
    extensions: &mut Extensions,
) -> Result<Expression> {
    match expr {
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let substrait_list = list
                .iter()
                .map(|x| to_substrait_rex(ctx, x, schema, col_ref_offset, extensions))
                .collect::<Result<Vec<Expression>>>()?;
            let substrait_expr =
                to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)?;

            let substrait_or_list = Expression {
                rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
                    value: Some(Box::new(substrait_expr)),
                    options: substrait_list,
                }))),
            };

            if *negated {
                let function_anchor = extensions.register_function("not".to_string());

                Ok(Expression {
                    rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                        function_reference: function_anchor,
                        arguments: vec![FunctionArgument {
                            arg_type: Some(ArgType::Value(substrait_or_list)),
                        }],
                        output_type: None,
                        args: vec![],
                        options: vec![],
                    })),
                })
            } else {
                Ok(substrait_or_list)
            }
        }
        Expr::ScalarFunction(fun) => {
            let mut arguments: Vec<FunctionArgument> = vec![];
            for arg in &fun.args {
                arguments.push(FunctionArgument {
                    arg_type: Some(ArgType::Value(to_substrait_rex(
                        ctx,
                        arg,
                        schema,
                        col_ref_offset,
                        extensions,
                    )?)),
                });
            }

            let function_anchor = extensions.register_function(fun.name().to_string());
            Ok(Expression {
                rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                    function_reference: function_anchor,
                    arguments,
                    output_type: None,
                    args: vec![],
                    options: vec![],
                })),
            })
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if *negated {
                // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
                let substrait_expr =
                    to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)?;
                let substrait_low =
                    to_substrait_rex(ctx, low, schema, col_ref_offset, extensions)?;
                let substrait_high =
                    to_substrait_rex(ctx, high, schema, col_ref_offset, extensions)?;

                let l_expr = make_binary_op_scalar_func(
                    &substrait_expr,
                    &substrait_low,
                    Operator::Lt,
                    extensions,
                );
                let r_expr = make_binary_op_scalar_func(
                    &substrait_high,
                    &substrait_expr,
                    Operator::Lt,
                    extensions,
                );

                Ok(make_binary_op_scalar_func(
                    &l_expr,
                    &r_expr,
                    Operator::Or,
                    extensions,
                ))
            } else {
                // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
                let substrait_expr =
                    to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)?;
                let substrait_low =
                    to_substrait_rex(ctx, low, schema, col_ref_offset, extensions)?;
                let substrait_high =
                    to_substrait_rex(ctx, high, schema, col_ref_offset, extensions)?;

                let l_expr = make_binary_op_scalar_func(
                    &substrait_low,
                    &substrait_expr,
                    Operator::LtEq,
                    extensions,
                );
                let r_expr = make_binary_op_scalar_func(
                    &substrait_expr,
                    &substrait_high,
                    Operator::LtEq,
                    extensions,
                );

                Ok(make_binary_op_scalar_func(
                    &l_expr,
                    &r_expr,
                    Operator::And,
                    extensions,
                ))
            }
        }
        Expr::Column(col) => {
            let index = schema.index_of_column(col)?;
            substrait_field_ref(index + col_ref_offset)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = to_substrait_rex(ctx, left, schema, col_ref_offset, extensions)?;
            let r = to_substrait_rex(ctx, right, schema, col_ref_offset, extensions)?;

            Ok(make_binary_op_scalar_func(&l, &r, *op, extensions))
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            let mut ifs: Vec<IfClause> = vec![];
            // Parse base
            if let Some(e) = expr {
                // Base expression exists
                ifs.push(IfClause {
                    r#if: Some(to_substrait_rex(
                        ctx,
                        e,
                        schema,
                        col_ref_offset,
                        extensions,
                    )?),
                    then: None,
                });
            }
            // Parse `when`s
            for (r#if, then) in when_then_expr {
                ifs.push(IfClause {
                    r#if: Some(to_substrait_rex(
                        ctx,
                        r#if,
                        schema,
                        col_ref_offset,
                        extensions,
                    )?),
                    then: Some(to_substrait_rex(
                        ctx,
                        then,
                        schema,
                        col_ref_offset,
                        extensions,
                    )?),
                });
            }

            // Parse outer `else`
            let r#else: Option<Box<Expression>> = match else_expr {
                Some(e) => Some(Box::new(to_substrait_rex(
                    ctx,
                    e,
                    schema,
                    col_ref_offset,
                    extensions,
                )?)),
                None => None,
            };

            Ok(Expression {
                rex_type: Some(RexType::IfThen(Box::new(IfThen { ifs, r#else }))),
            })
        }
        Expr::Cast(Cast { expr, data_type }) => {
            Ok(Expression {
                rex_type: Some(RexType::Cast(Box::new(
                    substrait::proto::expression::Cast {
                        r#type: Some(to_substrait_type(data_type, true, extensions)?),
                        input: Some(Box::new(to_substrait_rex(
                            ctx,
                            expr,
                            schema,
                            col_ref_offset,
                            extensions,
                        )?)),
                        failure_behavior: 0, // FAILURE_BEHAVIOR_UNSPECIFIED
                    },
                ))),
            })
        }
        Expr::Literal(value) => to_substrait_literal_expr(value, extensions),
        Expr::Alias(Alias { expr, .. }) => {
            to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)
        }
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
            null_treatment: _,
        }) => {
            // function reference
            let function_anchor = extensions.register_function(fun.to_string());
            // arguments
            let mut arguments: Vec<FunctionArgument> = vec![];
            for arg in args {
                arguments.push(FunctionArgument {
                    arg_type: Some(ArgType::Value(to_substrait_rex(
                        ctx,
                        arg,
                        schema,
                        col_ref_offset,
                        extensions,
                    )?)),
                });
            }
            // partition by expressions
            let partition_by = partition_by
                .iter()
                .map(|e| to_substrait_rex(ctx, e, schema, col_ref_offset, extensions))
                .collect::<Result<Vec<_>>>()?;
            // order by expressions
            let order_by = order_by
                .iter()
                .map(|e| substrait_sort_field(ctx, e, schema, extensions))
                .collect::<Result<Vec<_>>>()?;
            // window frame
            let bounds = to_substrait_bounds(window_frame)?;
            let bound_type = to_substrait_bound_type(window_frame)?;
            Ok(make_substrait_window_function(
                function_anchor,
                arguments,
                partition_by,
                order_by,
                bounds,
                bound_type,
            ))
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => make_substrait_like_expr(
            ctx,
            *case_insensitive,
            *negated,
            expr,
            pattern,
            *escape_char,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => {
            let substrait_expr =
                to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)?;

            let subquery_plan =
                to_substrait_rel(subquery.subquery.as_ref(), ctx, extensions)?;

            let substrait_subquery = Expression {
                rex_type: Some(RexType::Subquery(Box::new(Subquery {
                    subquery_type: Some(
                        substrait::proto::expression::subquery::SubqueryType::InPredicate(
                            Box::new(InPredicate {
                                needles: (vec![substrait_expr]),
                                haystack: Some(subquery_plan),
                            }),
                        ),
                    ),
                }))),
            };
            if *negated {
                let function_anchor = extensions.register_function("not".to_string());

                Ok(Expression {
                    rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                        function_reference: function_anchor,
                        arguments: vec![FunctionArgument {
                            arg_type: Some(ArgType::Value(substrait_subquery)),
                        }],
                        output_type: None,
                        args: vec![],
                        options: vec![],
                    })),
                })
            } else {
                Ok(substrait_subquery)
            }
        }
        Expr::Not(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "not",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsNull(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_null",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsNotNull(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_not_null",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsTrue(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_true",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsFalse(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_false",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsUnknown(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_unknown",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsNotTrue(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_not_true",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsNotFalse(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_not_false",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::IsNotUnknown(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "is_not_unknown",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        Expr::Negative(arg) => to_substrait_unary_scalar_fn(
            ctx,
            "negative",
            arg,
            schema,
            col_ref_offset,
            extensions,
        ),
        _ => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
    }
}

fn to_substrait_type(
    dt: &DataType,
    nullable: bool,
    extensions: &mut Extensions,
) -> Result<substrait::proto::Type> {
    let nullability = if nullable {
        r#type::Nullability::Nullable as i32
    } else {
        r#type::Nullability::Required as i32
    };
    match dt {
        DataType::Null => internal_err!("Null cast is not valid"),
        DataType::Boolean => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        // Float16 is not supported in Substrait
        DataType::Float32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp32(r#type::Fp32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Float64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp64(r#type::Fp64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            let kind = match tz {
                None => r#type::Kind::PrecisionTimestamp(r#type::PrecisionTimestamp {
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                    precision,
                }),
                Some(_) => {
                    // If timezone is present, no matter what the actual tz value is, it indicates the
                    // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
                    // As the timezone is lost, this conversion may be lossy for downstream use of the value.
                    r#type::Kind::PrecisionTimestampTz(r#type::PrecisionTimestampTz {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                        precision,
                    })
                }
            };
            Ok(substrait::proto::Type { kind: Some(kind) })
        }
        DataType::Date32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_32_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Date64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_64_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Interval(interval_unit) => {
            match interval_unit {
                IntervalUnit::YearMonth => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalYear(r#type::IntervalYear {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                    })),
                }),
                IntervalUnit::DayTime => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalDay(r#type::IntervalDay {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                        precision: Some(3), // DayTime precision is always milliseconds
                    })),
                }),
                IntervalUnit::MonthDayNano => {
                    // Substrait doesn't currently support this type, so we represent it as a UDT
                    Ok(substrait::proto::Type {
                        kind: Some(r#type::Kind::UserDefined(r#type::UserDefined {
                            type_reference: extensions.register_type(
                                INTERVAL_MONTH_DAY_NANO_TYPE_NAME.to_string(),
                            ),
                            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                            nullability,
                            type_parameters: vec![],
                        })),
                    })
                }
            }
        }
        DataType::Binary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::FixedSizeBinary(length) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::FixedBinary(r#type::FixedBinary {
                length: *length,
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeBinary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::BinaryView => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeUtf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8View => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::List(inner) => {
            let inner_type =
                to_substrait_type(inner.data_type(), inner.is_nullable(), extensions)?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::LargeList(inner) => {
            let inner_type =
                to_substrait_type(inner.data_type(), inner.is_nullable(), extensions)?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::Map(inner, _) => match inner.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                let key_type = to_substrait_type(
                    key_and_value[0].data_type(),
                    key_and_value[0].is_nullable(),
                    extensions,
                )?;
                let value_type = to_substrait_type(
                    key_and_value[1].data_type(),
                    key_and_value[1].is_nullable(),
                    extensions,
                )?;
                Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::Map(Box::new(r#type::Map {
                        key: Some(Box::new(key_type)),
                        value: Some(Box::new(value_type)),
                        type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                        nullability,
                    }))),
                })
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        DataType::Struct(fields) => {
            let field_types = fields
                .iter()
                .map(|field| {
                    to_substrait_type(field.data_type(), field.is_nullable(), extensions)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::Struct(r#type::Struct {
                    types: field_types,
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                })),
            })
        }
        DataType::Decimal128(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_128_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        DataType::Decimal256(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_256_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        _ => not_impl_err!("Unsupported cast type: {dt:?}"),
    }
}

#[allow(deprecated)]
fn make_substrait_window_function(
    function_reference: u32,
    arguments: Vec<FunctionArgument>,
    partitions: Vec<Expression>,
    sorts: Vec<SortField>,
    bounds: (Bound, Bound),
    bounds_type: BoundsType,
) -> Expression {
    Expression {
        rex_type: Some(RexType::WindowFunction(SubstraitWindowFunction {
            function_reference,
            arguments,
            partitions,
            sorts,
            options: vec![],
            output_type: None,
            phase: 0,      // default to AGGREGATION_PHASE_UNSPECIFIED
            invocation: 0, // TODO: fix
            lower_bound: Some(bounds.0),
            upper_bound: Some(bounds.1),
            args: vec![],
            bounds_type: bounds_type as i32,
        })),
    }
}

#[allow(deprecated)]
#[allow(clippy::too_many_arguments)]
fn make_substrait_like_expr(
    ctx: &SessionContext,
    ignore_case: bool,
    negated: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<char>,
    schema: &DFSchemaRef,
    col_ref_offset: usize,
    extensions: &mut Extensions,
) -> Result<Expression> {
    let function_anchor = if ignore_case {
        extensions.register_function("ilike".to_string())
    } else {
        extensions.register_function("like".to_string())
    };
    let expr = to_substrait_rex(ctx, expr, schema, col_ref_offset, extensions)?;
    let pattern = to_substrait_rex(ctx, pattern, schema, col_ref_offset, extensions)?;
    let escape_char = to_substrait_literal_expr(
        &ScalarValue::Utf8(escape_char.map(|c| c.to_string())),
        extensions,
    )?;
    let arguments = vec![
        FunctionArgument {
            arg_type: Some(ArgType::Value(expr)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(pattern)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(escape_char)),
        },
    ];

    let substrait_like = Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    };

    if negated {
        let function_anchor = extensions.register_function("not".to_string());

        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_like)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_like)
    }
}

fn to_substrait_bound(bound: &WindowFrameBound) -> Bound {
    match bound {
        WindowFrameBound::CurrentRow => Bound {
            kind: Some(BoundKind::CurrentRow(SubstraitBound::CurrentRow {})),
        },
        WindowFrameBound::Preceding(s) => match s {
            ScalarValue::UInt8(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt16(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt32(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt64(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int8(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int16(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int32(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int64(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v,
                })),
            },
            _ => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
        WindowFrameBound::Following(s) => match s {
            ScalarValue::UInt8(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt16(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt32(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt64(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int8(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int16(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int32(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int64(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v,
                })),
            },
            _ => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
    }
}

fn to_substrait_bound_type(window_frame: &WindowFrame) -> Result<BoundsType> {
    match window_frame.units {
        WindowFrameUnits::Rows => Ok(BoundsType::Rows), // ROWS
        WindowFrameUnits::Range => Ok(BoundsType::Range), // RANGE
        // TODO: Support GROUPS
        unit => not_impl_err!("Unsupported window frame unit: {unit:?}"),
    }
}

fn to_substrait_bounds(window_frame: &WindowFrame) -> Result<(Bound, Bound)> {
    Ok((
        to_substrait_bound(&window_frame.start_bound),
        to_substrait_bound(&window_frame.end_bound),
    ))
}

fn to_substrait_literal(
    value: &ScalarValue,
    extensions: &mut Extensions,
) -> Result<Literal> {
    if value.is_null() {
        return Ok(Literal {
            nullable: true,
            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
            literal_type: Some(LiteralType::Null(to_substrait_type(
                &value.data_type(),
                true,
                extensions,
            )?)),
        });
    }
    let (literal_type, type_variation_reference) = match value {
        ScalarValue::Boolean(Some(b)) => {
            (LiteralType::Boolean(*b), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Int8(Some(n)) => {
            (LiteralType::I8(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt8(Some(n)) => (
            LiteralType::I8(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int16(Some(n)) => {
            (LiteralType::I16(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt16(Some(n)) => (
            LiteralType::I16(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int32(Some(n)) => (LiteralType::I32(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt32(Some(n)) => (
            LiteralType::I32(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int64(Some(n)) => (LiteralType::I64(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt64(Some(n)) => (
            LiteralType::I64(*n as i64),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Float32(Some(f)) => {
            (LiteralType::Fp32(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Float64(Some(f)) => {
            (LiteralType::Fp64(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::TimestampSecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        // If timezone is present, no matter what the actual tz value is, it indicates the
        // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
        // As the timezone is lost, this conversion may be lossy for downstream use of the value.
        ScalarValue::TimestampSecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Date32(Some(d)) => {
            (LiteralType::Date(*d), DATE_32_TYPE_VARIATION_REF)
        }
        // Date64 literal is not supported in Substrait
        ScalarValue::IntervalYearMonth(Some(i)) => (
            LiteralType::IntervalYearToMonth(IntervalYearToMonth {
                // DF only tracks total months, but there should always be 12 months in a year
                years: *i / 12,
                months: *i % 12,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::IntervalMonthDayNano(Some(i)) => {
            // IntervalMonthDayNano is internally represented as a 128-bit integer, containing
            // months (32bit), days (32bit), and nanoseconds (64bit)
            let bytes = i.to_byte_slice();
            (
                LiteralType::UserDefined(UserDefined {
                    type_reference: extensions
                        .register_type(INTERVAL_MONTH_DAY_NANO_TYPE_NAME.to_string()),
                    type_parameters: vec![],
                    val: Some(user_defined::Val::Value(ProtoAny {
                        type_url: INTERVAL_MONTH_DAY_NANO_TYPE_NAME.to_string(),
                        value: bytes.to_vec().into(),
                    })),
                }),
                DEFAULT_TYPE_VARIATION_REF,
            )
        }
        ScalarValue::IntervalDayTime(Some(i)) => (
            LiteralType::IntervalDayToSecond(IntervalDayToSecond {
                days: i.days,
                seconds: i.milliseconds / 1000,
                subseconds: (i.milliseconds % 1000) as i64,
                precision_mode: Some(PrecisionMode::Precision(3)), // 3 for milliseconds
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Binary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeBinary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::BinaryView(Some(b)) => (
            LiteralType::Binary(b.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::FixedSizeBinary(_, Some(b)) => (
            LiteralType::FixedBinary(b.clone()),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8(Some(s)) => (
            LiteralType::String(s.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeUtf8(Some(s)) => (
            LiteralType::String(s.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8View(Some(s)) => (
            LiteralType::String(s.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Decimal128(v, p, s) if v.is_some() => (
            LiteralType::Decimal(Decimal {
                value: v.unwrap().to_le_bytes().to_vec(),
                precision: *p as i32,
                scale: *s as i32,
            }),
            DECIMAL_128_TYPE_VARIATION_REF,
        ),
        ScalarValue::List(l) => (
            convert_array_to_literal_list(l, extensions)?,
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeList(l) => (
            convert_array_to_literal_list(l, extensions)?,
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Map(m) => {
            let map = if m.is_empty() || m.value(0).is_empty() {
                let mt = to_substrait_type(m.data_type(), m.is_nullable(), extensions)?;
                let mt = match mt {
                    substrait::proto::Type {
                        kind: Some(r#type::Kind::Map(mt)),
                    } => Ok(mt.as_ref().to_owned()),
                    _ => exec_err!("Unexpected type for a map: {mt:?}"),
                }?;
                LiteralType::EmptyMap(mt)
            } else {
                let keys = (0..m.keys().len())
                    .map(|i| {
                        to_substrait_literal(
                            &ScalarValue::try_from_array(&m.keys(), i)?,
                            extensions,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let values = (0..m.values().len())
                    .map(|i| {
                        to_substrait_literal(
                            &ScalarValue::try_from_array(&m.values(), i)?,
                            extensions,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let key_values = keys
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(k, v)| {
                        Ok(KeyValue {
                            key: Some(k),
                            value: Some(v),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                LiteralType::Map(Map { key_values })
            };
            (map, DEFAULT_CONTAINER_TYPE_VARIATION_REF)
        }
        ScalarValue::Struct(s) => (
            LiteralType::Struct(Struct {
                fields: s
                    .columns()
                    .iter()
                    .map(|col| {
                        to_substrait_literal(
                            &ScalarValue::try_from_array(col, 0)?,
                            extensions,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        _ => (
            not_impl_err!("Unsupported literal: {value:?}")?,
            DEFAULT_TYPE_VARIATION_REF,
        ),
    };

    Ok(Literal {
        nullable: false,
        type_variation_reference,
        literal_type: Some(literal_type),
    })
}

fn convert_array_to_literal_list<T: OffsetSizeTrait>(
    array: &GenericListArray<T>,
    extensions: &mut Extensions,
) -> Result<LiteralType> {
    assert_eq!(array.len(), 1);
    let nested_array = array.value(0);

    let values = (0..nested_array.len())
        .map(|i| {
            to_substrait_literal(
                &ScalarValue::try_from_array(&nested_array, i)?,
                extensions,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    if values.is_empty() {
        let lt = match to_substrait_type(
            array.data_type(),
            array.is_nullable(),
            extensions,
        )? {
            substrait::proto::Type {
                kind: Some(r#type::Kind::List(lt)),
            } => lt.as_ref().to_owned(),
            _ => unreachable!(),
        };
        Ok(LiteralType::EmptyList(lt))
    } else {
        Ok(LiteralType::List(List { values }))
    }
}

fn to_substrait_literal_expr(
    value: &ScalarValue,
    extensions: &mut Extensions,
) -> Result<Expression> {
    let literal = to_substrait_literal(value, extensions)?;
    Ok(Expression {
        rex_type: Some(RexType::Literal(literal)),
    })
}

/// Util to generate substrait [RexType::ScalarFunction] with one argument
fn to_substrait_unary_scalar_fn(
    ctx: &SessionContext,
    fn_name: &str,
    arg: &Expr,
    schema: &DFSchemaRef,
    col_ref_offset: usize,
    extensions: &mut Extensions,
) -> Result<Expression> {
    let function_anchor = extensions.register_function(fn_name.to_string());
    let substrait_expr = to_substrait_rex(ctx, arg, schema, col_ref_offset, extensions)?;

    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(substrait_expr)),
            }],
            output_type: None,
            options: vec![],
            ..Default::default()
        })),
    })
}

/// Try to convert an [Expr] to a [FieldReference].
/// Returns `Err` if the [Expr] is not a [Expr::Column].
fn try_to_substrait_field_reference(
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<FieldReference> {
    match expr {
        Expr::Column(col) => {
            let index = schema.index_of_column(col)?;
            Ok(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(
                        Box::new(reference_segment::StructField {
                            field: index as i32,
                            child: None,
                        }),
                    )),
                })),
                root_type: None,
            })
        }
        _ => substrait_err!("Expect a `Column` expr, but found {expr:?}"),
    }
}

fn substrait_sort_field(
    ctx: &SessionContext,
    sort: &Sort,
    schema: &DFSchemaRef,
    extensions: &mut Extensions,
) -> Result<SortField> {
    let Sort {
        expr,
        asc,
        nulls_first,
    } = sort;
    let e = to_substrait_rex(ctx, expr, schema, 0, extensions)?;
    let d = match (asc, nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(e),
        sort_kind: Some(SortKind::Direction(d as i32)),
    })
}

fn substrait_field_ref(index: usize) -> Result<Expression> {
    Ok(Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(
                    Box::new(reference_segment::StructField {
                        field: index as i32,
                        child: None,
                    }),
                )),
            })),
            root_type: None,
        }))),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::consumer::{
        from_substrait_literal_without_names, from_substrait_type_without_names,
    };
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
    use datafusion::arrow::array::{
        GenericListArray, Int64Builder, MapBuilder, StringBuilder,
    };
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::scalar::ScalarStructBuilder;
    use std::collections::HashMap;

    #[test]
    fn round_trip_literals() -> Result<()> {
        round_trip_literal(ScalarValue::Boolean(None))?;
        round_trip_literal(ScalarValue::Boolean(Some(true)))?;
        round_trip_literal(ScalarValue::Boolean(Some(false)))?;

        round_trip_literal(ScalarValue::Int8(None))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MIN)))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MAX)))?;
        round_trip_literal(ScalarValue::UInt8(None))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MIN)))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MAX)))?;

        round_trip_literal(ScalarValue::Int16(None))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MIN)))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MAX)))?;
        round_trip_literal(ScalarValue::UInt16(None))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MIN)))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MAX)))?;

        round_trip_literal(ScalarValue::Int32(None))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MIN)))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MAX)))?;
        round_trip_literal(ScalarValue::UInt32(None))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MIN)))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MAX)))?;

        round_trip_literal(ScalarValue::Int64(None))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MIN)))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MAX)))?;
        round_trip_literal(ScalarValue::UInt64(None))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MIN)))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MAX)))?;

        for (ts, tz) in [
            (Some(12345), None),
            (None, None),
            (Some(12345), Some("UTC".into())),
            (None, Some("UTC".into())),
        ] {
            round_trip_literal(ScalarValue::TimestampSecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMillisecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMicrosecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampNanosecond(ts, tz))?;
        }

        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(Arc::new(GenericListArray::new_null(
            Field::new_list_field(DataType::Float32, true).into(),
            1,
        ))))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(Arc::new(
            GenericListArray::new_null(
                Field::new_list_field(DataType::Float32, true).into(),
                1,
            ),
        )))?;

        // Null map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(false)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Empty map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Valid map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.keys().append_value("key1");
        map_builder.keys().append_value("key2");
        map_builder.values().append_value(1);
        map_builder.values().append_value(2);
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        let c0 = Field::new("c0", DataType::Boolean, true);
        let c1 = Field::new("c1", DataType::Int32, true);
        let c2 = Field::new("c2", DataType::Utf8, true);
        round_trip_literal(
            ScalarStructBuilder::new()
                .with_scalar(c0.to_owned(), ScalarValue::Boolean(Some(true)))
                .with_scalar(c1.to_owned(), ScalarValue::Int32(Some(1)))
                .with_scalar(c2.to_owned(), ScalarValue::Utf8(None))
                .build()?,
        )?;
        round_trip_literal(ScalarStructBuilder::new_null(vec![c0, c1, c2]))?;

        round_trip_literal(ScalarValue::IntervalYearMonth(Some(17)))?;
        round_trip_literal(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(17, 25, 1234567890),
        )))?;
        round_trip_literal(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            57, 123456,
        ))))?;

        Ok(())
    }

    fn round_trip_literal(scalar: ScalarValue) -> Result<()> {
        println!("Checking round trip of {scalar:?}");

        let mut extensions = Extensions::default();
        let substrait_literal = to_substrait_literal(&scalar, &mut extensions)?;
        let roundtrip_scalar =
            from_substrait_literal_without_names(&substrait_literal, &extensions)?;
        assert_eq!(scalar, roundtrip_scalar);
        Ok(())
    }

    #[test]
    fn custom_type_literal_extensions() -> Result<()> {
        let mut extensions = Extensions::default();
        // IntervalMonthDayNano is represented as a custom type in Substrait
        let scalar = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            17, 25, 1234567890,
        )));
        let substrait_literal = to_substrait_literal(&scalar, &mut extensions)?;
        let roundtrip_scalar =
            from_substrait_literal_without_names(&substrait_literal, &extensions)?;
        assert_eq!(scalar, roundtrip_scalar);

        assert_eq!(
            extensions,
            Extensions {
                functions: HashMap::new(),
                types: HashMap::from([(
                    0,
                    INTERVAL_MONTH_DAY_NANO_TYPE_NAME.to_string()
                )]),
                type_variations: HashMap::new(),
            }
        );

        // Check we fail if we don't propagate extensions
        assert!(from_substrait_literal_without_names(
            &substrait_literal,
            &Extensions::default()
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn round_trip_types() -> Result<()> {
        round_trip_type(DataType::Boolean)?;
        round_trip_type(DataType::Int8)?;
        round_trip_type(DataType::UInt8)?;
        round_trip_type(DataType::Int16)?;
        round_trip_type(DataType::UInt16)?;
        round_trip_type(DataType::Int32)?;
        round_trip_type(DataType::UInt32)?;
        round_trip_type(DataType::Int64)?;
        round_trip_type(DataType::UInt64)?;
        round_trip_type(DataType::Float32)?;
        round_trip_type(DataType::Float64)?;

        for tz in [None, Some("UTC".into())] {
            round_trip_type(DataType::Timestamp(TimeUnit::Second, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Nanosecond, tz))?;
        }

        round_trip_type(DataType::Date32)?;
        round_trip_type(DataType::Date64)?;
        round_trip_type(DataType::Binary)?;
        round_trip_type(DataType::FixedSizeBinary(10))?;
        round_trip_type(DataType::LargeBinary)?;
        round_trip_type(DataType::BinaryView)?;
        round_trip_type(DataType::Utf8)?;
        round_trip_type(DataType::LargeUtf8)?;
        round_trip_type(DataType::Utf8View)?;
        round_trip_type(DataType::Decimal128(10, 2))?;
        round_trip_type(DataType::Decimal256(30, 2))?;

        round_trip_type(DataType::List(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;
        round_trip_type(DataType::LargeList(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;

        round_trip_type(DataType::Map(
            Field::new_struct(
                "entries",
                [
                    Field::new("key", DataType::Utf8, false).into(),
                    Field::new("value", DataType::Int32, true).into(),
                ],
                false,
            )
            .into(),
            false,
        ))?;

        round_trip_type(DataType::Struct(
            vec![
                Field::new("c0", DataType::Int32, true),
                Field::new("c1", DataType::Utf8, true),
            ]
            .into(),
        ))?;

        round_trip_type(DataType::Interval(IntervalUnit::YearMonth))?;
        round_trip_type(DataType::Interval(IntervalUnit::MonthDayNano))?;
        round_trip_type(DataType::Interval(IntervalUnit::DayTime))?;

        Ok(())
    }

    fn round_trip_type(dt: DataType) -> Result<()> {
        println!("Checking round trip of {dt:?}");

        let mut extensions = Extensions::default();

        // As DataFusion doesn't consider nullability as a property of the type, but field,
        // it doesn't matter if we set nullability to true or false here.
        let substrait = to_substrait_type(&dt, true, &mut extensions)?;
        let roundtrip_dt = from_substrait_type_without_names(&substrait, &extensions)?;
        assert_eq!(dt, roundtrip_dt);
        Ok(())
    }

    #[test]
    fn custom_type_extensions() -> Result<()> {
        let mut extensions = Extensions::default();
        // IntervalMonthDayNano is represented as a custom type in Substrait
        let dt = DataType::Interval(IntervalUnit::MonthDayNano);

        let substrait = to_substrait_type(&dt, true, &mut extensions)?;
        let roundtrip_dt = from_substrait_type_without_names(&substrait, &extensions)?;
        assert_eq!(dt, roundtrip_dt);

        assert_eq!(
            extensions,
            Extensions {
                functions: HashMap::new(),
                types: HashMap::from([(
                    0,
                    INTERVAL_MONTH_DAY_NANO_TYPE_NAME.to_string()
                )]),
                type_variations: HashMap::new(),
            }
        );

        // Check we fail if we don't propagate extensions
        assert!(
            from_substrait_type_without_names(&substrait, &Extensions::default())
                .is_err()
        );

        Ok(())
    }
}

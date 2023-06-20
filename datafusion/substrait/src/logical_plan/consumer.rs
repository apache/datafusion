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

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{
    aggregate_function, window_function::find_df_window_func, BinaryExpr,
    BuiltinScalarFunction, Case, Expr, LogicalPlan, Operator,
};
use datafusion::logical_expr::{build_join_schema, Extension, LogicalPlanBuilder};
use datafusion::logical_expr::{expr, Cast, WindowFrameBound, WindowFrameUnits};
use datafusion::prelude::JoinType;
use datafusion::sql::TableReference;
use datafusion::{
    error::{DataFusionError, Result},
    optimizer::utils::split_conjunction,
    prelude::{Column, SessionContext},
    scalar::ScalarValue,
};
use substrait::proto::expression::Literal;
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind, window_function::Bound,
        MaskExpression, RexType,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, Expression, Plan, Rel, Type,
};
use substrait::proto::{FunctionArgument, SortField};

use datafusion::logical_expr::expr::{InList, Sort};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::variation_const::{
    DATE_32_TYPE_REF, DATE_64_TYPE_REF, DECIMAL_128_TYPE_REF, DECIMAL_256_TYPE_REF,
    DEFAULT_CONTAINER_TYPE_REF, DEFAULT_TYPE_REF, LARGE_CONTAINER_TYPE_REF,
    TIMESTAMP_MICRO_TYPE_REF, TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF,
    TIMESTAMP_SECOND_TYPE_REF, UNSIGNED_INTEGER_TYPE_REF,
};

enum ScalarFunctionType {
    Builtin(BuiltinScalarFunction),
    Op(Operator),
    // logical negation
    Not,
}

pub fn name_to_op(name: &str) -> Result<Operator> {
    match name {
        "equal" => Ok(Operator::Eq),
        "not_equal" => Ok(Operator::NotEq),
        "lt" => Ok(Operator::Lt),
        "lte" => Ok(Operator::LtEq),
        "gt" => Ok(Operator::Gt),
        "gte" => Ok(Operator::GtEq),
        "add" => Ok(Operator::Plus),
        "subtract" => Ok(Operator::Minus),
        "multiply" => Ok(Operator::Multiply),
        "divide" => Ok(Operator::Divide),
        "mod" => Ok(Operator::Modulo),
        "and" => Ok(Operator::And),
        "or" => Ok(Operator::Or),
        "is_distinct_from" => Ok(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Ok(Operator::IsNotDistinctFrom),
        "regex_match" => Ok(Operator::RegexMatch),
        "regex_imatch" => Ok(Operator::RegexIMatch),
        "regex_not_match" => Ok(Operator::RegexNotMatch),
        "regex_not_imatch" => Ok(Operator::RegexNotIMatch),
        "bitwise_and" => Ok(Operator::BitwiseAnd),
        "bitwise_or" => Ok(Operator::BitwiseOr),
        "str_concat" => Ok(Operator::StringConcat),
        "bitwise_xor" => Ok(Operator::BitwiseXor),
        "bitwise_shift_right" => Ok(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Ok(Operator::BitwiseShiftLeft),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported function name: {name:?}"
        ))),
    }
}

fn name_to_op_or_scalar_function(name: &str) -> Result<ScalarFunctionType> {
    if let Ok(op) = name_to_op(name) {
        return Ok(ScalarFunctionType::Op(op));
    }

    if let Ok(fun) = BuiltinScalarFunction::from_str(name) {
        return Ok(ScalarFunctionType::Builtin(fun));
    }

    Err(DataFusionError::NotImplemented(format!(
        "Unsupported function name: {name:?}"
    )))
}

fn scalar_function_or_not(name: &str) -> Result<ScalarFunctionType> {
    if let Ok(fun) = BuiltinScalarFunction::from_str(name) {
        return Ok(ScalarFunctionType::Builtin(fun));
    }

    if name == "not" {
        return Ok(ScalarFunctionType::Not);
    }

    Err(DataFusionError::NotImplemented(format!(
        "Unsupported function name: {name:?}"
    )))
}

/// Convert Substrait Plan to DataFusion DataFrame
pub async fn from_substrait_plan(
    ctx: &mut SessionContext,
    plan: &Plan,
) -> Result<LogicalPlan> {
    // Register function extension
    let function_extension = plan
        .extensions
        .iter()
        .map(|e| match &e.mapping_type {
            Some(ext) => match ext {
                MappingType::ExtensionFunction(ext_f) => {
                    Ok((ext_f.function_anchor, &ext_f.name))
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Extension type not supported: {ext:?}"
                ))),
            },
            None => Err(DataFusionError::NotImplemented(
                "Cannot parse empty extension".to_string(),
            )),
        })
        .collect::<Result<HashMap<_, _>>>()?;
    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, rel, &function_extension).await?)
                    },
                    plan_rel::RelType::Root(root) => {
                        Ok(from_substrait_rel(ctx, root.input.as_ref().unwrap(), &function_extension).await?)
                    }
                },
                None => Err(DataFusionError::Internal("Cannot parse plan relation: None".to_string()))
            }
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )))
    }
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    ctx: &mut SessionContext,
    rel: &Rel,
    extensions: &HashMap<u32, &String>,
) -> Result<LogicalPlan> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            if let Some(input) = p.input.as_ref() {
                let mut input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x =
                        from_substrait_rex(e, input.clone().schema(), extensions).await?;
                    // if the expression is WindowFunction, wrap in a Window relation
                    //   before returning and do not add to list of this Projection's expression list
                    // otherwise, add expression to the Projection's expression list
                    match &*x {
                        Expr::WindowFunction(_) => {
                            input = input.window(vec![x.as_ref().clone()])?;
                            exprs.push(x.as_ref().clone());
                        }
                        _ => {
                            exprs.push(x.as_ref().clone());
                        }
                    }
                }
                input.project(exprs)?.build()
            } else {
                Err(DataFusionError::NotImplemented(
                    "Projection without an input is not supported".to_string(),
                ))
            }
        }
        Some(RelType::Filter(filter)) => {
            if let Some(input) = filter.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                if let Some(condition) = filter.condition.as_ref() {
                    let expr =
                        from_substrait_rex(condition, input.schema(), extensions).await?;
                    input.filter(expr.as_ref().clone())?.build()
                } else {
                    Err(DataFusionError::NotImplemented(
                        "Filter without an condition is not valid".to_string(),
                    ))
                }
            } else {
                Err(DataFusionError::NotImplemented(
                    "Filter without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Fetch(fetch)) => {
            if let Some(input) = fetch.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let offset = fetch.offset as usize;
                let count = fetch.count as usize;
                input.limit(offset, Some(count))?.build()
            } else {
                Err(DataFusionError::NotImplemented(
                    "Fetch without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Sort(sort)) => {
            if let Some(input) = sort.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let sorts =
                    from_substrait_sorts(&sort.sorts, input.schema(), extensions).await?;
                input.sort(sorts)?.build()
            } else {
                Err(DataFusionError::NotImplemented(
                    "Sort without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Aggregate(agg)) => {
            if let Some(input) = agg.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut group_expr = vec![];
                let mut aggr_expr = vec![];

                let groupings = match agg.groupings.len() {
                    1 => Ok(&agg.groupings[0]),
                    _ => Err(DataFusionError::NotImplemented(
                        "Aggregate with multiple grouping sets is not supported"
                            .to_string(),
                    )),
                };

                for e in &groupings?.grouping_expressions {
                    let x = from_substrait_rex(e, input.schema(), extensions).await?;
                    group_expr.push(x.as_ref().clone());
                }

                for m in &agg.measures {
                    let filter = match &m.filter {
                        Some(fil) => Some(Box::new(
                            from_substrait_rex(fil, input.schema(), extensions)
                                .await?
                                .as_ref()
                                .clone(),
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
                        None => Err(DataFusionError::NotImplemented(
                            "Aggregate without aggregate function is not supported"
                                .to_string(),
                        )),
                    };
                    aggr_expr.push(agg_func?.as_ref().clone());
                }

                input.aggregate(group_expr, aggr_expr)?.build()
            } else {
                Err(DataFusionError::NotImplemented(
                    "Aggregate without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Join(join)) => {
            let left = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.left.as_ref().unwrap(), extensions).await?,
            );
            let right = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.right.as_ref().unwrap(), extensions).await?,
            );
            let join_type = from_substrait_jointype(join.r#type)?;
            // The join condition expression needs full input schema and not the output schema from join since we lose columns from
            // certain join types such as semi and anti joins
            // - if left and right schemas are different, we combine (join) the schema to include all fields
            // - if left and right schemas are the same, we handle the duplicate fields by using `build_join_schema()`, which discard the unused schema
            // TODO: Handle duplicate fields error for other join types (non-semi/anti). The current approach does not work due to Substrait's inability
            //       to encode aliases
            let join_schema = match left.schema().join(right.schema()) {
                Ok(schema) => Ok(schema),
                Err(DataFusionError::SchemaError(
                    datafusion::common::SchemaError::DuplicateQualifiedField {
                        qualifier: _,
                        name: _,
                    },
                )) => build_join_schema(left.schema(), right.schema(), &join_type),
                Err(e) => Err(e),
            };
            let on = from_substrait_rex(
                join.expression.as_ref().unwrap(),
                &join_schema?,
                extensions,
            )
            .await?;
            let predicates = split_conjunction(&on);
            // TODO: collect only one null_eq_null
            let join_exprs: Vec<(Column, Column, bool)> = predicates
                .iter()
                .map(|p| match p {
                    Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                        match (left.as_ref(), right.as_ref()) {
                            (Expr::Column(l), Expr::Column(r)) => match op {
                                Operator::Eq => Ok((l.clone(), r.clone(), false)),
                                Operator::IsNotDistinctFrom => {
                                    Ok((l.clone(), r.clone(), true))
                                }
                                _ => Err(DataFusionError::Internal(
                                    "invalid join condition op".to_string(),
                                )),
                            },
                            _ => Err(DataFusionError::Internal(
                                "invalid join condition expression".to_string(),
                            )),
                        }
                    }
                    _ => Err(DataFusionError::Internal(
                        "Non-binary expression is not supported in join condition"
                            .to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            let (left_cols, right_cols, null_eq_nulls): (Vec<_>, Vec<_>, Vec<_>) =
                itertools::multiunzip(join_exprs);
            left.join_detailed(
                right.build()?,
                join_type,
                (left_cols, right_cols),
                None,
                null_eq_nulls[0],
            )?
            .build()
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_reference = match nt.names.len() {
                    0 => {
                        return Err(DataFusionError::Internal(
                            "No table name found in NamedTable".to_string(),
                        ));
                    }
                    1 => TableReference::Bare {
                        table: (&nt.names[0]).into(),
                    },
                    2 => TableReference::Partial {
                        schema: (&nt.names[0]).into(),
                        table: (&nt.names[1]).into(),
                    },
                    _ => TableReference::Full {
                        catalog: (&nt.names[0]).into(),
                        schema: (&nt.names[1]).into(),
                        table: (&nt.names[2]).into(),
                    },
                };
                let t = ctx.table(table_reference).await?;
                let t = t.into_optimized_plan()?;
                match &read.projection {
                    Some(MaskExpression { select, .. }) => match &select.as_ref() {
                        Some(projection) => {
                            let column_indices: Vec<usize> = projection
                                .struct_items
                                .iter()
                                .map(|item| item.field as usize)
                                .collect();
                            match &t {
                                LogicalPlan::TableScan(scan) => {
                                    let fields: Vec<DFField> = column_indices
                                        .iter()
                                        .map(|i| scan.projected_schema.field(*i).clone())
                                        .collect();
                                    let mut scan = scan.clone();
                                    scan.projection = Some(column_indices);
                                    scan.projected_schema =
                                        DFSchemaRef::new(DFSchema::new_with_metadata(
                                            fields,
                                            HashMap::new(),
                                        )?);
                                    Ok(LogicalPlan::TableScan(scan))
                                }
                                _ => Err(DataFusionError::Internal(
                                    "unexpected plan for table".to_string(),
                                )),
                            }
                        }
                        _ => Ok(t),
                    },
                    _ => Ok(t),
                }
            }
            _ => Err(DataFusionError::NotImplemented(
                "Only NamedTable reads are supported".to_string(),
            )),
        },
        Some(RelType::ExtensionLeaf(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return Err(DataFusionError::Substrait(
                    "Unexpected empty detail in ExtensionLeafRel".to_string(),
                ));
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionSingle(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return Err(DataFusionError::Substrait(
                    "Unexpected empty detail in ExtensionSingleRel".to_string(),
                ));
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            let Some(input_rel) = &extension.input else {
                return Err(DataFusionError::Substrait(
                    "ExtensionSingleRel doesn't contains input rel. Try use ExtensionLeafRel instead".to_string()
                ));
            };
            let input_plan = from_substrait_rel(ctx, input_rel, extensions).await?;
            let plan = plan.from_template(&plan.expressions(), &[input_plan]);
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionMulti(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return Err(DataFusionError::Substrait(
                    "Unexpected empty detail in ExtensionSingleRel".to_string(),
                ));
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
            let plan = plan.from_template(&plan.expressions(), &inputs);
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported RelType: {:?}",
            rel.rel_type
        ))),
    }
}

fn from_substrait_jointype(join_type: i32) -> Result<JoinType> {
    if let Some(substrait_join_type) = join_rel::JoinType::from_i32(join_type) {
        match substrait_join_type {
            join_rel::JoinType::Inner => Ok(JoinType::Inner),
            join_rel::JoinType::Left => Ok(JoinType::Left),
            join_rel::JoinType::Right => Ok(JoinType::Right),
            join_rel::JoinType::Outer => Ok(JoinType::Full),
            join_rel::JoinType::Anti => Ok(JoinType::LeftAnti),
            join_rel::JoinType::Semi => Ok(JoinType::LeftSemi),
            _ => Err(DataFusionError::Internal(format!(
                "unsupported join type {substrait_join_type:?}"
            ))),
        }
    } else {
        Err(DataFusionError::Internal(format!(
            "invalid join type variant {join_type:?}"
        )))
    }
}

/// Convert Substrait Sorts to DataFusion Exprs
pub async fn from_substrait_sorts(
    substrait_sorts: &Vec<SortField>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut sorts: Vec<Expr> = vec![];
    for s in substrait_sorts {
        let expr = from_substrait_rex(s.expr.as_ref().unwrap(), input_schema, extensions)
            .await?;
        let asc_nullfirst = match &s.sort_kind {
            Some(k) => match k {
                Direction(d) => {
                    let Some(direction) = SortDirection::from_i32(*d) else {
                        return Err(DataFusionError::NotImplemented(
                            format!("Unsupported Substrait SortDirection value {d}"),
                        ))
                    };

                    match direction {
                        SortDirection::AscNullsFirst => Ok((true, true)),
                        SortDirection::AscNullsLast => Ok((true, false)),
                        SortDirection::DescNullsFirst => Ok((false, true)),
                        SortDirection::DescNullsLast => Ok((false, false)),
                        SortDirection::Clustered => Err(DataFusionError::NotImplemented(
                            "Sort with direction clustered is not yet supported"
                                .to_string(),
                        )),
                        SortDirection::Unspecified => {
                            Err(DataFusionError::NotImplemented(
                                "Unspecified sort direction is invalid".to_string(),
                            ))
                        }
                    }
                }
                ComparisonFunctionReference(_) => Err(DataFusionError::NotImplemented(
                    "Sort using comparison function reference is not supported"
                        .to_string(),
                )),
            },
            None => Err(DataFusionError::NotImplemented(
                "Sort without sort kind is invalid".to_string(),
            )),
        };
        let (asc, nulls_first) = asc_nullfirst.unwrap();
        sorts.push(Expr::Sort(Sort {
            expr: Box::new(expr.as_ref().clone()),
            asc,
            nulls_first,
        }));
    }
    Ok(sorts)
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = from_substrait_rex(expr, input_schema, extensions).await?;
        expressions.push(expression.as_ref().clone());
    }
    Ok(expressions)
}

/// Convert Substrait FunctionArguments to DataFusion Exprs
pub async fn from_substriat_func_args(
    arguments: &Vec<FunctionArgument>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => {
                from_substrait_rex(e, input_schema, extensions).await
            }
            _ => Err(DataFusionError::NotImplemented(
                "Aggregated function argument non-Value type not supported".to_string(),
            )),
        };
        args.push(arg_expr?.as_ref().clone());
    }
    Ok(args)
}

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    f: &AggregateFunction,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
    filter: Option<Box<Expr>>,
    order_by: Option<Vec<Expr>>,
    distinct: bool,
) -> Result<Arc<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in &f.arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => {
                from_substrait_rex(e, input_schema, extensions).await
            }
            _ => Err(DataFusionError::NotImplemented(
                "Aggregated function argument non-Value type not supported".to_string(),
            )),
        };
        args.push(arg_expr?.as_ref().clone());
    }

    let fun = match extensions.get(&f.function_reference) {
        Some(function_name) => {
            aggregate_function::AggregateFunction::from_str(function_name)
        }
        None => Err(DataFusionError::NotImplemented(format!(
            "Aggregated function not found: function anchor = {:?}",
            f.function_reference
        ))),
    };

    Ok(Arc::new(Expr::AggregateFunction(expr::AggregateFunction {
        fun: fun.unwrap(),
        args,
        distinct,
        filter,
        order_by,
    })))
}

/// Convert Substrait Rex to DataFusion Expr
#[async_recursion]
pub async fn from_substrait_rex(
    e: &Expression,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Arc<Expr>> {
    match &e.rex_type {
        Some(RexType::SingularOrList(s)) => {
            let substrait_expr = s.value.as_ref().unwrap();
            let substrait_list = s.options.as_ref();
            Ok(Arc::new(Expr::InList(InList {
                expr: Box::new(
                    from_substrait_rex(substrait_expr, input_schema, extensions)
                        .await?
                        .as_ref()
                        .clone(),
                ),
                list: from_substrait_rex_vec(substrait_list, input_schema, extensions)
                    .await?,
                negated: false,
            })))
        }
        Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
            Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
                Some(StructField(x)) => match &x.child.as_ref() {
                    Some(_) => Err(DataFusionError::NotImplemented(
                        "Direct reference StructField with child is not supported"
                            .to_string(),
                    )),
                    None => {
                        let column =
                            input_schema.field(x.field as usize).qualified_column();
                        Ok(Arc::new(Expr::Column(Column {
                            relation: column.relation,
                            name: column.name,
                        })))
                    }
                },
                _ => Err(DataFusionError::NotImplemented(
                    "Direct reference with types other than StructField is not supported"
                        .to_string(),
                )),
            },
            _ => Err(DataFusionError::NotImplemented(
                "unsupported field ref type".to_string(),
            )),
        },
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
                                if_expr.r#if.as_ref().unwrap(),
                                input_schema,
                                extensions,
                            )
                            .await?
                            .as_ref()
                            .clone(),
                        ));
                        continue;
                    }
                }
                when_then_expr.push((
                    Box::new(
                        from_substrait_rex(
                            if_expr.r#if.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?
                        .as_ref()
                        .clone(),
                    ),
                    Box::new(
                        from_substrait_rex(
                            if_expr.then.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?
                        .as_ref()
                        .clone(),
                    ),
                ));
            }
            // Parse `else`
            let else_expr = match &if_then.r#else {
                Some(e) => Some(Box::new(
                    from_substrait_rex(e, input_schema, extensions)
                        .await?
                        .as_ref()
                        .clone(),
                )),
                None => None,
            };
            Ok(Arc::new(Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            })))
        }
        Some(RexType::ScalarFunction(f)) => match f.arguments.len() {
            // BinaryExpr or ScalarFunction
            2 => match (&f.arguments[0].arg_type, &f.arguments[1].arg_type) {
                (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                    let op_or_fun = match extensions.get(&f.function_reference) {
                        Some(fname) => name_to_op_or_scalar_function(fname),
                        None => Err(DataFusionError::NotImplemented(format!(
                            "Aggregated function not found: function reference = {:?}",
                            f.function_reference
                        ))),
                    };
                    match op_or_fun {
                        Ok(ScalarFunctionType::Op(op)) => {
                            return Ok(Arc::new(Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(
                                    from_substrait_rex(l, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                                op,
                                right: Box::new(
                                    from_substrait_rex(r, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                            })))
                        }
                        Ok(ScalarFunctionType::Builtin(fun)) => {
                            Ok(Arc::new(Expr::ScalarFunction(expr::ScalarFunction {
                                fun,
                                args: vec![
                                    from_substrait_rex(l, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                    from_substrait_rex(r, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ],
                            })))
                        }
                        Ok(ScalarFunctionType::Not) => {
                            Err(DataFusionError::NotImplemented(
                                "Not expected function type: Not".to_string(),
                            ))
                        }
                        Err(e) => Err(e),
                    }
                }
                (l, r) => Err(DataFusionError::NotImplemented(format!(
                    "Invalid arguments for binary expression: {l:?} and {r:?}"
                ))),
            },
            // ScalarFunction or Expr::Not
            1 => {
                let fun = match extensions.get(&f.function_reference) {
                    Some(fname) => scalar_function_or_not(fname),
                    None => Err(DataFusionError::NotImplemented(format!(
                        "Function not found: function reference = {:?}",
                        f.function_reference
                    ))),
                };

                match fun {
                    Ok(ScalarFunctionType::Op(_)) => {
                        Err(DataFusionError::NotImplemented(
                            "Not expected function type: Op".to_string(),
                        ))
                    }
                    Ok(scalar_function_type) => {
                        match &f.arguments.first().unwrap().arg_type {
                            Some(ArgType::Value(e)) => {
                                let expr =
                                    from_substrait_rex(e, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone();
                                match scalar_function_type {
                                    ScalarFunctionType::Builtin(fun) => Ok(Arc::new(
                                        Expr::ScalarFunction(expr::ScalarFunction {
                                            fun,
                                            args: vec![expr],
                                        }),
                                    )),
                                    ScalarFunctionType::Not => {
                                        Ok(Arc::new(Expr::Not(Box::new(expr))))
                                    }
                                    _ => Err(DataFusionError::NotImplemented(
                                        "Invalid arguments for Not expression"
                                            .to_string(),
                                    )),
                                }
                            }
                            _ => Err(DataFusionError::NotImplemented(
                                "Invalid arguments for Not expression".to_string(),
                            )),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            // ScalarFunction
            _ => {
                let fun = match extensions.get(&f.function_reference) {
                    Some(fname) => BuiltinScalarFunction::from_str(fname),
                    None => Err(DataFusionError::NotImplemented(format!(
                        "Aggregated function not found: function reference = {:?}",
                        f.function_reference
                    ))),
                };

                let mut args: Vec<Expr> = vec![];
                for arg in f.arguments.iter() {
                    match &arg.arg_type {
                        Some(ArgType::Value(e)) => {
                            args.push(
                                from_substrait_rex(e, input_schema, extensions)
                                    .await?
                                    .as_ref()
                                    .clone(),
                            );
                        }
                        e => {
                            return Err(DataFusionError::NotImplemented(format!(
                                "Invalid arguments for scalar function: {e:?}"
                            )))
                        }
                    }
                }

                Ok(Arc::new(Expr::ScalarFunction(expr::ScalarFunction {
                    fun: fun?,
                    args,
                })))
            }
        },
        Some(RexType::Literal(lit)) => {
            let scalar_value = from_substrait_literal(lit)?;
            Ok(Arc::new(Expr::Literal(scalar_value)))
        }
        Some(RexType::Cast(cast)) => match cast.as_ref().r#type.as_ref() {
            Some(output_type) => Ok(Arc::new(Expr::Cast(Cast::new(
                Box::new(
                    from_substrait_rex(
                        cast.as_ref().input.as_ref().unwrap().as_ref(),
                        input_schema,
                        extensions,
                    )
                    .await?
                    .as_ref()
                    .clone(),
                ),
                from_substrait_type(output_type)?,
            )))),
            None => Err(DataFusionError::Substrait(
                "Cast experssion without output type is not allowed".to_string(),
            )),
        },
        Some(RexType::WindowFunction(window)) => {
            let fun = match extensions.get(&window.function_reference) {
                Some(function_name) => Ok(find_df_window_func(function_name)),
                None => Err(DataFusionError::NotImplemented(format!(
                    "Window function not found: function anchor = {:?}",
                    &window.function_reference
                ))),
            };
            let order_by =
                from_substrait_sorts(&window.sorts, input_schema, extensions).await?;
            // Substrait does not encode WindowFrameUnits so we're using a simple logic to determine the units
            // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
            // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
            // TODO: Consider the cases where window frame is specified in query and is different from default
            let units = if order_by.is_empty() {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Range
            };
            Ok(Arc::new(Expr::WindowFunction(expr::WindowFunction {
                fun: fun?.unwrap(),
                args: from_substriat_func_args(
                    &window.arguments,
                    input_schema,
                    extensions,
                )
                .await?,
                partition_by: from_substrait_rex_vec(
                    &window.partitions,
                    input_schema,
                    extensions,
                )
                .await?,
                order_by,
                window_frame: datafusion::logical_expr::WindowFrame {
                    units,
                    start_bound: from_substrait_bound(&window.lower_bound, true)?,
                    end_bound: from_substrait_bound(&window.upper_bound, false)?,
                },
            })))
        }
        _ => Err(DataFusionError::NotImplemented(
            "unsupported rex_type".to_string(),
        )),
    }
}

fn from_substrait_type(dt: &substrait::proto::Type) -> Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt8),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt16),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt32),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt64),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Second, None))
                }
                TIMESTAMP_MILLI_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                }
                TIMESTAMP_MICRO_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                TIMESTAMP_NANO_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                }
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(DataType::Date32),
                DATE_64_TYPE_REF => Ok(DataType::Date64),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeBinary),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length))
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeUtf8),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::List(list) => {
                let inner_type =
                    from_substrait_type(list.r#type.as_ref().ok_or_else(|| {
                        DataFusionError::Substrait(
                            "List type must have inner type".to_string(),
                        )
                    })?)?;
                let field = Arc::new(Field::new("list_item", inner_type, true));
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::List(field)),
                    LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeList(field)),
                    v => Err(DataFusionError::NotImplemented(format!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )))?,
                }
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8))
                }
                DECIMAL_256_TYPE_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8))
                }
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported Substrait type: {s_kind:?}"
            ))),
        },
        _ => Err(DataFusionError::NotImplemented(
            "`None` Substrait kind is not supported".to_string(),
        )),
    }
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
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => Ok(
                    WindowFrameBound::Preceding(ScalarValue::Int64(Some(*offset))),
                ),
                BoundKind::Following(SubstraitBound::Following { offset }) => Ok(
                    WindowFrameBound::Following(ScalarValue::Int64(Some(*offset))),
                ),
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => Err(DataFusionError::Substrait(
                "WindowFunction missing Substrait Bound kind".to_string(),
            )),
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

pub(crate) fn from_substrait_literal(lit: &Literal) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_REF => ScalarValue::TimestampSecond(Some(*t), None),
            TIMESTAMP_MILLI_TYPE_REF => ScalarValue::TimestampMillisecond(Some(*t), None),
            TIMESTAMP_MICRO_TYPE_REF => ScalarValue::TimestampMicrosecond(Some(*t), None),
            TIMESTAMP_NANO_TYPE_REF => ScalarValue::TimestampNanosecond(Some(*t), None),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeBinary(Some(b.clone())),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}",
                )));
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] =
                d.value
                    .clone()
                    .try_into()
                    .or(Err(DataFusionError::Substrait(
                        "Failed to parse decimal value".to_string(),
                    )))?;
            let p = d.precision.try_into().map_err(|e| {
                DataFusionError::Substrait(format!(
                    "Failed to parse decimal precision: {e}"
                ))
            })?;
            let s = d.scale.try_into().map_err(|e| {
                DataFusionError::Substrait(format!("Failed to parse decimal scale: {e}"))
            })?;
            ScalarValue::Decimal128(
                Some(std::primitive::i128::from_le_bytes(value)),
                p,
                s,
            )
        }
        Some(LiteralType::Null(ntype)) => from_substrait_null(ntype)?,
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported literal_type: {:?}",
                lit.literal_type
            )))
        }
    };

    Ok(scalar_value)
}

fn from_substrait_null(null_type: &Type) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::Bool(_) => Ok(ScalarValue::Boolean(None)),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int8(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt8(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int16(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt16(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int32(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt32(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int64(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt64(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::Fp32(_) => Ok(ScalarValue::Float32(None)),
            r#type::Kind::Fp64(_) => Ok(ScalarValue::Float64(None)),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => Ok(ScalarValue::TimestampSecond(None, None)),
                TIMESTAMP_MILLI_TYPE_REF => {
                    Ok(ScalarValue::TimestampMillisecond(None, None))
                }
                TIMESTAMP_MICRO_TYPE_REF => {
                    Ok(ScalarValue::TimestampMicrosecond(None, None))
                }
                TIMESTAMP_NANO_TYPE_REF => {
                    Ok(ScalarValue::TimestampNanosecond(None, None))
                }
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(ScalarValue::Date32(None)),
                DATE_64_TYPE_REF => Ok(ScalarValue::Date64(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Binary(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeBinary(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            // FixedBinary is not supported because `None` doesn't have length
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Utf8(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeUtf8(None)),
                v => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ))),
            },
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported Substrait type: {kind:?}"
            ))),
        }
    } else {
        Err(DataFusionError::NotImplemented(
            "Null type without kind is not supported".to_string(),
        ))
    }
}

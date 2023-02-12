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
use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::logical_expr::expr;
use datafusion::logical_expr::{
    aggregate_function, BinaryExpr, Case, Expr, LogicalPlan, Operator,
};
use datafusion::logical_expr::{build_join_schema, LogicalPlanBuilder};
use datafusion::prelude::JoinType;
use datafusion::sql::TableReference;
use datafusion::{
    error::{DataFusionError, Result},
    optimizer::utils::split_conjunction,
    prelude::{Column, SessionContext},
    scalar::ScalarValue,
};
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField, MaskExpression, RexType,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, Expression, Plan, Rel, Type,
};

use datafusion::logical_expr::expr::Sort;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

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
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x = from_substrait_rex(e, input.schema(), extensions).await?;
                    exprs.push(x.as_ref().clone());
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
                let mut sorts: Vec<Expr> = vec![];
                for s in &sort.sorts {
                    let expr = from_substrait_rex(
                        s.expr.as_ref().unwrap(),
                        input.schema(),
                        extensions,
                    )
                    .await?;
                    let asc_nullfirst = match &s.sort_kind {
                        Some(k) => match k {
                            Direction(d) => {
                                let direction : SortDirection = unsafe {
                                    ::std::mem::transmute(*d)
                                };
                                match direction {
                                    SortDirection::AscNullsFirst => Ok((true, true)),
                                    SortDirection::AscNullsLast => Ok((true, false)),
                                    SortDirection::DescNullsFirst => Ok((false, true)),
                                    SortDirection::DescNullsLast => Ok((false, false)),
                                    SortDirection::Clustered =>
                                        Err(DataFusionError::NotImplemented("Sort with direction clustered is not yet supported".to_string()))
                                    ,
                                    SortDirection::Unspecified =>
                                        Err(DataFusionError::NotImplemented("Unspecified sort direction is invalid".to_string()))
                                }
                            }
                            ComparisonFunctionReference(_) => {
                                Err(DataFusionError::NotImplemented("Sort using comparison function reference is not supported".to_string()))
                            },
                        },
                        None => Err(DataFusionError::NotImplemented("Sort without sort kind is invalid".to_string()))
                    };
                    let (asc, nulls_first) = asc_nullfirst.unwrap();
                    sorts.push(Expr::Sort(Sort {
                        expr: Box::new(expr.as_ref().clone()),
                        asc,
                        nulls_first,
                    }));
                }
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
            let schema =
                build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;
            let on = from_substrait_rex(
                join.expression.as_ref().unwrap(),
                &schema,
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
                                "invalid join condition expresssion".to_string(),
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
                                    // clippy thinks this clone is redundant but it is not
                                    #[allow(clippy::redundant_clone)]
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

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    f: &AggregateFunction,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
    filter: Option<Box<Expr>>,
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
        Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
            Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
                Some(StructField(x)) => match &x.child.as_ref() {
                    Some(_) => Err(DataFusionError::NotImplemented(
                        "Direct reference StructField with child is not supported"
                            .to_string(),
                    )),
                    None => Ok(Arc::new(Expr::Column(Column {
                        relation: None,
                        name: input_schema.field(x.field as usize).name().to_string(),
                    }))),
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
        Some(RexType::ScalarFunction(f)) => {
            assert!(f.arguments.len() == 2);
            let op = match extensions.get(&f.function_reference) {
                Some(fname) => name_to_op(fname),
                None => Err(DataFusionError::NotImplemented(format!(
                    "Aggregated function not found: function reference = {:?}",
                    f.function_reference
                ))),
            };
            match (&f.arguments[0].arg_type, &f.arguments[1].arg_type) {
                (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                    Ok(Arc::new(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(
                            from_substrait_rex(l, input_schema, extensions)
                                .await?
                                .as_ref()
                                .clone(),
                        ),
                        op: op?,
                        right: Box::new(
                            from_substrait_rex(r, input_schema, extensions)
                                .await?
                                .as_ref()
                                .clone(),
                        ),
                    })))
                }
                (l, r) => Err(DataFusionError::NotImplemented(format!(
                    "Invalid arguments for binary expression: {l:?} and {r:?}"
                ))),
            }
        }
        Some(RexType::Literal(lit)) => {
            match &lit.literal_type {
                Some(LiteralType::I8(n)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Int8(Some(*n as i8)))))
                }
                Some(LiteralType::I16(n)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Int16(Some(*n as i16)))))
                }
                Some(LiteralType::I32(n)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Int32(Some(*n)))))
                }
                Some(LiteralType::I64(n)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Int64(Some(*n)))))
                }
                Some(LiteralType::Boolean(b)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Boolean(Some(*b)))))
                }
                Some(LiteralType::Date(d)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Date32(Some(*d)))))
                }
                Some(LiteralType::Fp32(f)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Float32(Some(*f)))))
                }
                Some(LiteralType::Fp64(f)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Float64(Some(*f)))))
                }
                Some(LiteralType::Decimal(d)) => {
                    let value: [u8; 16] = d.value.clone().try_into().or(Err(
                        DataFusionError::Substrait(
                            "Failed to parse decimal value".to_string(),
                        ),
                    ))?;
                    let p = d.precision.try_into().map_err(|e| {
                        DataFusionError::Substrait(format!(
                            "Failed to parse decimal precision: {e}"
                        ))
                    })?;
                    let s = d.scale.try_into().map_err(|e| {
                        DataFusionError::Substrait(format!(
                            "Failed to parse decimal scale: {e}"
                        ))
                    })?;
                    Ok(Arc::new(Expr::Literal(ScalarValue::Decimal128(
                        Some(std::primitive::i128::from_le_bytes(value)),
                        p,
                        s,
                    ))))
                }
                Some(LiteralType::String(s)) => {
                    Ok(Arc::new(Expr::Literal(ScalarValue::Utf8(Some(s.clone())))))
                }
                Some(LiteralType::Binary(b)) => Ok(Arc::new(Expr::Literal(
                    ScalarValue::Binary(Some(b.clone())),
                ))),
                Some(LiteralType::Null(ntype)) => {
                    Ok(Arc::new(Expr::Literal(from_substrait_null(ntype)?)))
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported literal_type: {:?}",
                    lit.literal_type
                ))),
            }
        }
        _ => Err(DataFusionError::NotImplemented(
            "unsupported rex_type".to_string(),
        )),
    }
}

fn from_substrait_null(null_type: &Type) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::I8(_) => Ok(ScalarValue::Int8(None)),
            r#type::Kind::I16(_) => Ok(ScalarValue::Int16(None)),
            r#type::Kind::I32(_) => Ok(ScalarValue::Int32(None)),
            r#type::Kind::I64(_) => Ok(ScalarValue::Int64(None)),
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported null kind: {kind:?}"
            ))),
        }
    } else {
        Err(DataFusionError::NotImplemented(
            "Null type without kind is not supported".to_string(),
        ))
    }
}

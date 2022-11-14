use async_recursion::async_recursion;
use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{LogicalPlan, aggregate_function};
use datafusion::logical_plan::build_join_schema;
use datafusion::prelude::JoinType;
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{Expr, Operator},
    optimizer::utils::split_conjunction,
    prelude::{Column, DataFrame, SessionContext},
    scalar::ScalarValue,
};

use substrait::protobuf::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::MaskedReference, literal::LiteralType, MaskExpression,
        RexType,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument::ArgType,
    read_rel::ReadType,
    rel::RelType,
    sort_field::{SortKind::*, SortDirection},
    AggregateFunction, Expression, Plan, Rel,
};

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
        "like" => Ok(Operator::Like),
        "not_like" => Ok(Operator::NotLike),
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
            "Unsupported function name: {:?}",
            name
        ))),
    }
}

/// Convert Substrait Plan to DataFusion DataFrame
pub async fn from_substrait_plan(ctx: &mut SessionContext, plan: &Plan) -> Result<Arc<DataFrame>> {
    // Register function extension
    let function_extension = plan.extensions
        .iter()
        .map(|e| match &e.mapping_type {
            Some(ext) => {
                match ext {
                    MappingType::ExtensionFunction(ext_f) => Ok((ext_f.function_anchor, &ext_f.name)),
                    _ => Err(DataFusionError::NotImplemented(format!("Extension type not supported: {:?}", ext)))
                }
            }
            None => Err(DataFusionError::NotImplemented("Cannot parse empty extension".to_string()))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    substrait::protobuf::plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, &rel, &function_extension).await?)
                    },
                    substrait::protobuf::plan_rel::RelType::Root(_) => Err(DataFusionError::NotImplemented(
                        "RootRel not supported".to_string()
                    )),
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
pub async fn from_substrait_rel(ctx: &mut SessionContext, rel: &Rel, extensions: &HashMap<u32, &String>) -> Result<Arc<DataFrame>> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            if let Some(input) = p.input.as_ref() {
                let input = from_substrait_rel(ctx, input, extensions).await?;
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x = from_substrait_rex(e, &input.schema(), extensions).await?;
                    exprs.push(x.as_ref().clone());
                }
                input.select(exprs)
            } else {
                Err(DataFusionError::NotImplemented(
                    "Projection without an input is not supported".to_string(),
                ))
            }
        }
        Some(RelType::Filter(filter)) => {
            if let Some(input) = filter.input.as_ref() {
                let input = from_substrait_rel(ctx, input, extensions).await?;
                if let Some(condition) = filter.condition.as_ref() {
                    let expr = from_substrait_rex(condition, &input.schema(), extensions).await?;
                    input.filter(expr.as_ref().clone())
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
                let input = from_substrait_rel(ctx, input, extensions).await?;
                let offset = fetch.offset as usize;
                let count = fetch.count as usize;
                input.limit(offset, Some(count))
            } else {
                Err(DataFusionError::NotImplemented(
                    "Fetch without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Sort(sort)) => {
            if let Some(input) = sort.input.as_ref() {
                let input = from_substrait_rel(ctx, input, extensions).await?;
                let mut sorts: Vec<Expr> = vec![];
                for s in &sort.sorts {
                    let expr = from_substrait_rex(&s.expr.as_ref().unwrap(), &input.schema(), extensions).await?;
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
                                    SortDirection::Clustered => {
                                        Err(DataFusionError::NotImplemented(
                                            "Sort with direction clustered is not yet supported".to_string(),
                                        ))  
                                    },
                                    SortDirection::Unspecified => {
                                        Err(DataFusionError::NotImplemented(
                                            "Unspecified sort direction is invalid".to_string(),
                                        ))  
                                    }
                                }
                            }
                            ComparisonFunctionReference(_) => {
                                Err(DataFusionError::NotImplemented(
                                    "Sort using comparison function reference is not supported".to_string(),
                                ))
                            },
                        },
                        None => {
                            Err(DataFusionError::NotImplemented(
                                "Sort without sort kind is invalid".to_string(),
                            ))
                        },
                    };
                    let (asc, nulls_first) = asc_nullfirst.unwrap();
                    sorts.push(Expr::Sort { expr: Box::new(expr.as_ref().clone()), asc: asc, nulls_first: nulls_first });
                }
                input.sort(sorts)
            } else {
                Err(DataFusionError::NotImplemented(
                    "Sort without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Aggregate(agg)) => {
            if let Some(input) = agg.input.as_ref() {
                let input = from_substrait_rel(ctx, input, extensions).await?;
                let mut group_expr = vec![];
                let mut aggr_expr = vec![];

                let groupings = match agg.groupings.len() {
                    1 => { Ok(&agg.groupings[0]) },
                    _ => {
                        Err(DataFusionError::NotImplemented(
                            "Aggregate with multiple grouping sets is not supported".to_string(),
                        ))
                    }
                };

                for e in &groupings?.grouping_expressions {
                    let x = from_substrait_rex(&e, &input.schema(), extensions).await?;
                    group_expr.push(x.as_ref().clone());
                }

                for m in &agg.measures {
                    let filter = match &m.filter {
                        Some(fil) => Some(Box::new(from_substrait_rex(fil, &input.schema(), extensions).await?.as_ref().clone())),
                        None => None
                    };
                    let agg_func = match &m.measure {
                        Some(f) => {
                            let distinct = match f.invocation  {
                                _ if f.invocation == AggregationInvocation::Distinct as i32 => true,
                                _ if f.invocation == AggregationInvocation::All as i32 => false,
                                _ => false
                            };
                            from_substrait_agg_func(&f, &input.schema(), extensions, filter, distinct).await
                        },
                        None => Err(DataFusionError::NotImplemented(
                            "Aggregate without aggregate function is not supported".to_string(),
                        )),
                    };
                    aggr_expr.push(agg_func?.as_ref().clone());
                }

                input.aggregate(group_expr, aggr_expr)
            } else {
                Err(DataFusionError::NotImplemented(
                    "Aggregate without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Join(join)) => {
            let left = from_substrait_rel(ctx, &join.left.as_ref().unwrap(), extensions).await?;
            let right = from_substrait_rel(ctx, &join.right.as_ref().unwrap(), extensions).await?;
            let join_type = match join.r#type {
                1 => JoinType::Inner,
                2 => JoinType::Left,
                3 => JoinType::Right,
                4 => JoinType::Full,
                5 => JoinType::Anti,
                6 => JoinType::Semi,
                _ => return Err(DataFusionError::Internal("invalid join type".to_string())),
            };
            let mut predicates = vec![];
            let schema = build_join_schema(&left.schema(), &right.schema(), &JoinType::Inner)?;
            let on = from_substrait_rex(&join.expression.as_ref().unwrap(), &schema, extensions).await?;
            split_conjunction(&on, &mut predicates);
            let pairs = predicates
                .iter()
                .map(|p| match p {
                    Expr::BinaryExpr {
                        left,
                        op: Operator::Eq,
                        right,
                    } => match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(l), Expr::Column(r)) => Ok((l.flat_name(), r.flat_name())),
                        _ => {
                            return Err(DataFusionError::Internal(
                                "invalid join condition".to_string(),
                            ))
                        }
                    },
                    _ => {
                        return Err(DataFusionError::Internal(
                            "invalid join condition".to_string(),
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let left_cols: Vec<&str> = pairs.iter().map(|(l, _)| l.as_str()).collect();
            let right_cols: Vec<&str> = pairs.iter().map(|(_, r)| r.as_str()).collect();
            left.join(right, join_type, &left_cols, &right_cols, None)
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_name: String = nt.names[0].clone();
                let t = ctx.table(&*table_name)?;
                match &read.projection {
                    Some(MaskExpression { select, .. }) => match &select.as_ref() {
                        Some(projection) => {
                            let column_indices: Vec<usize> = projection
                                .struct_items
                                .iter()
                                .map(|item| item.field as usize)
                                .collect();
                            match t.to_logical_plan()? {
                                LogicalPlan::TableScan(scan) => {
                                    let mut scan = scan.clone();
                                    let fields: Vec<DFField> = column_indices
                                        .iter()
                                        .map(|i| scan.projected_schema.field(*i).clone())
                                        .collect();
                                    scan.projection = Some(column_indices);
                                    scan.projected_schema = DFSchemaRef::new(
                                        DFSchema::new_with_metadata(fields, HashMap::new())?,
                                    );
                                    let plan = LogicalPlan::TableScan(scan);
                                    Ok(Arc::new(DataFrame::new(ctx.state.clone(), &plan)))
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

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    f: &AggregateFunction,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
    filter: Option<Box<Expr>>,
    distinct: bool
) -> Result<Arc<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in &f.arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => from_substrait_rex(e, input_schema, extensions).await,
            _ => Err(DataFusionError::NotImplemented(
                    "Aggregated function argument non-Value type not supported".to_string(),
                ))
        };
        args.push(arg_expr?.as_ref().clone());
    }

    let fun = match extensions.get(&f.function_reference) {
        Some(function_name) => aggregate_function::AggregateFunction::from_str(function_name),
        None => Err(DataFusionError::NotImplemented(format!(
                "Aggregated function not found: function anchor = {:?}",
                f.function_reference
            )
        ))
    };

    Ok(
        Arc::new(
            Expr::AggregateFunction {
                fun: fun.unwrap(),
                args: args,
                distinct: distinct,
                filter: filter
            }
        )
    )
}

/// Convert Substrait Rex to DataFusion Expr
#[async_recursion]
pub async fn from_substrait_rex(e: &Expression, input_schema: &DFSchema, extensions: &HashMap<u32, &String>) -> Result<Arc<Expr>> {
    match &e.rex_type {
        Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
            Some(MaskedReference(mask)) => match &mask.select.as_ref() {
                Some(x) if x.struct_items.len() == 1 => Ok(Arc::new(Expr::Column(Column {
                    relation: None,
                    name: input_schema
                        .field(x.struct_items[0].field as usize)
                        .name()
                        .to_string(),
                }))),
                _ => Err(DataFusionError::NotImplemented(
                    "invalid field reference".to_string(),
                )),
            },
            _ => Err(DataFusionError::NotImplemented(
                "unsupported field ref type".to_string(),
            )),
        },
        Some(RexType::ScalarFunction(f)) => {
            assert!(f.arguments.len() == 2);
            let op = match extensions.get(&f.function_reference) {
                    Some(fname) => name_to_op(fname),
                    None => Err(DataFusionError::NotImplemented(format!(
                        "Aggregated function not found: function reference = {:?}",
                        f.function_reference
                    )
                ))
            };
            match (&f.arguments[0].arg_type, &f.arguments[1].arg_type) {
                (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                    Ok(Arc::new(Expr::BinaryExpr {
                        left: Box::new(from_substrait_rex(l, input_schema, extensions).await?.as_ref().clone()),
                        op: op?,
                        right: Box::new(
                            from_substrait_rex(r, input_schema, extensions).await?.as_ref().clone(),
                        ),
                    }))
                }
                (l, r) => Err(DataFusionError::NotImplemented(format!(
                    "Invalid arguments for binary expression: {:?} and {:?}",
                    l, r
                ))),
            }
        }
        Some(RexType::Literal(lit)) => match &lit.literal_type {
            Some(LiteralType::I8(n)) => {
                Ok(Arc::new(Expr::Literal(ScalarValue::Int8(Some(*n as i8)))))
            }
            Some(LiteralType::I16(n)) => {
                Ok(Arc::new(Expr::Literal(ScalarValue::Int16(Some(*n as i16)))))
            }
            Some(LiteralType::I32(n)) => {
                Ok(Arc::new(Expr::Literal(ScalarValue::Int32(Some(*n as i32)))))
            }
            Some(LiteralType::I64(n)) => {
                Ok(Arc::new(Expr::Literal(ScalarValue::Int64(Some(*n as i64)))))
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
            Some(LiteralType::String(s)) => Ok(Arc::new(Expr::Literal(ScalarValue::LargeUtf8(
                Some(s.clone()),
            )))),
            Some(LiteralType::Binary(b)) => Ok(Arc::new(Expr::Literal(ScalarValue::Binary(Some(
                b.clone(),
            ))))),
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported literal_type: {:?}",
                    lit.literal_type
                )))
            }
        },
        _ => Err(DataFusionError::NotImplemented(
            "unsupported rex_type".to_string(),
        )),
    }
}

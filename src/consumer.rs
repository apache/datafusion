use async_recursion::async_recursion;
use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_plan::build_join_schema;
use datafusion::prelude::JoinType;
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{Expr, Operator},
    optimizer::utils::split_conjunction,
    prelude::{Column, DataFrame, SessionContext},
    scalar::ScalarValue,
};
use std::collections::HashMap;
use std::sync::Arc;
use substrait::protobuf::{
    expression::{
        field_reference::ReferenceType::MaskedReference, literal::LiteralType, MaskExpression,
        RexType,
    },
    function_argument::ArgType,
    read_rel::ReadType,
    rel::RelType,
    Expression, Rel,
};

pub fn reference_to_op(reference: u32) -> Result<Operator> {
    match reference {
        1 => Ok(Operator::Eq),
        2 => Ok(Operator::NotEq),
        3 => Ok(Operator::Lt),
        4 => Ok(Operator::LtEq),
        5 => Ok(Operator::Gt),
        6 => Ok(Operator::GtEq),
        7 => Ok(Operator::Plus),
        8 => Ok(Operator::Minus),
        9 => Ok(Operator::Multiply),
        10 => Ok(Operator::Divide),
        11 => Ok(Operator::Modulo),
        12 => Ok(Operator::And),
        13 => Ok(Operator::Or),
        14 => Ok(Operator::Like),
        15 => Ok(Operator::NotLike),
        16 => Ok(Operator::IsDistinctFrom),
        17 => Ok(Operator::IsNotDistinctFrom),
        18 => Ok(Operator::RegexMatch),
        19 => Ok(Operator::RegexIMatch),
        20 => Ok(Operator::RegexNotMatch),
        21 => Ok(Operator::RegexNotIMatch),
        22 => Ok(Operator::BitwiseAnd),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported function_reference: {:?}",
            reference
        ))),
    }
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(ctx: &mut SessionContext, rel: &Rel) -> Result<Arc<DataFrame>> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            if let Some(input) = p.input.as_ref() {
                let input = from_substrait_rel(ctx, input).await?;
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x = from_substrait_rex(e, &input.schema()).await?;
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
                let input = from_substrait_rel(ctx, input).await?;
                if let Some(condition) = filter.condition.as_ref() {
                    let expr = from_substrait_rex(condition, &input.schema()).await?;
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
                let input = from_substrait_rel(ctx, input).await?;
                let offset = fetch.offset as usize;
                let count = fetch.count as usize;
                input.limit(Some(offset), Some(count))
            } else {
                Err(DataFusionError::NotImplemented(
                    "Fetch without an input is not valid".to_string(),
                ))
            }
        }
        Some(RelType::Join(join)) => {
            let left = from_substrait_rel(ctx, &join.left.as_ref().unwrap()).await?;
            let right = from_substrait_rel(ctx, &join.right.as_ref().unwrap()).await?;
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
            let on = from_substrait_rex(&join.expression.as_ref().unwrap(), &schema).await?;
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

/// Convert Substrait Rex to DataFusion Expr
#[async_recursion]
pub async fn from_substrait_rex(e: &Expression, input_schema: &DFSchema) -> Result<Arc<Expr>> {
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
            let op = reference_to_op(f.function_reference)?;
            match (&f.arguments[0].arg_type, &f.arguments[1].arg_type) {
                (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                    Ok(Arc::new(Expr::BinaryExpr {
                        left: Box::new(from_substrait_rex(l, input_schema).await?.as_ref().clone()),
                        op,
                        right: Box::new(
                            from_substrait_rex(r, input_schema).await?.as_ref().clone(),
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

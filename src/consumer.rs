use std::sync::Arc;

use async_recursion::async_recursion;

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{Expr, Operator},
    prelude::{Column, DataFrame, SessionContext},
    scalar::ScalarValue,
};

use substrait::protobuf::{
    expression::{field_reference::ReferenceType::MaskedReference, literal::LiteralType, RexType},
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
                    let x = from_substrait_rex(e, input.as_ref()).await?;
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
                    let expr = from_substrait_rex(condition, input.as_ref()).await?;
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
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_name: String = nt.names[0].clone();
                ctx.table(&*table_name)
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
pub async fn from_substrait_rex(e: &Expression, input: &DataFrame) -> Result<Arc<Expr>> {
    match &e.rex_type {
        Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
            Some(MaskedReference(mask)) => match &mask.select.as_ref() {
                Some(x) if x.struct_items.len() == 1 => Ok(Arc::new(Expr::Column(Column {
                    relation: None,
                    name: input
                        .schema()
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
            assert!(f.args.len() == 2);
            let op = reference_to_op(f.function_reference)?;
            Ok(Arc::new(Expr::BinaryExpr {
                left: Box::new(
                    from_substrait_rex(&f.args[0], input)
                        .await?
                        .as_ref()
                        .clone(),
                ),
                op,
                right: Box::new(
                    from_substrait_rex(&f.args[1], input)
                        .await?
                        .as_ref()
                        .clone(),
                ),
            }))
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

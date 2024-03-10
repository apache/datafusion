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

use datafusion_common::{not_impl_err, Column, Result, ScalarValue};
use datafusion_expr::{
    expr::{Alias, InList, ScalarFunction, WindowFunction},
    Between, BinaryExpr, Case, Cast, Expr, Like, Operator,
};
use sqlparser::ast;

use super::Unparser;

/// Convert a DataFusion [`Expr`] to `sqlparser::ast::Expr`
///
/// This function is the opposite of `SqlToRel::sql_to_expr` and can
/// be used to, among other things, convert `Expr`s to strings.
///
/// # Example
/// ```
/// use datafusion_expr::{col, lit};
/// use datafusion_sql::unparser::expr_to_sql;
/// let expr = col("a").gt(lit(4));
/// let sql = expr_to_sql(&expr).unwrap();
///
/// assert_eq!(format!("{}", sql), "a > 4")
/// ```
pub fn expr_to_sql(expr: &Expr) -> Result<ast::Expr> {
    let unparser = Unparser::default();
    unparser.expr_to_sql(expr)
}

impl Unparser<'_> {
    pub fn expr_to_sql(&self, expr: &Expr) -> Result<ast::Expr> {
        match expr {
            Expr::InList(InList {
                expr,
                list: _,
                negated: _,
            }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::ScalarFunction(ScalarFunction { .. }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::Between(Between {
                expr,
                negated: _,
                low: _,
                high: _,
            }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::Column(col) => self.col_to_sql(col),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.expr_to_sql(left.as_ref())?;
                let r = self.expr_to_sql(right.as_ref())?;
                let op = self.op_to_sql(op)?;

                Ok(self.binary_op_to_sql(l, r, op))
            }
            Expr::Case(Case {
                expr,
                when_then_expr: _,
                else_expr: _,
            }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::Cast(Cast { expr, data_type: _ }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::Literal(value) => Ok(ast::Expr::Value(self.scalar_to_sql(value)?)),
            Expr::Alias(Alias { expr, name: _, .. }) => self.expr_to_sql(expr),
            Expr::WindowFunction(WindowFunction {
                fun: _,
                args: _,
                partition_by: _,
                order_by: _,
                window_frame: _,
                null_treatment: _,
            }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            Expr::Like(Like {
                negated: _,
                expr,
                pattern: _,
                escape_char: _,
                case_insensitive: _,
            }) => {
                not_impl_err!("Unsupported expression: {expr:?}")
            }
            _ => not_impl_err!("Unsupported expression: {expr:?}"),
        }
    }

    fn col_to_sql(&self, col: &Column) -> Result<ast::Expr> {
        if let Some(table_ref) = &col.relation {
            let mut id = table_ref.to_vec();
            id.push(col.name.to_string());
            return Ok(ast::Expr::CompoundIdentifier(
                id.iter().map(|i| self.new_ident(i.to_string())).collect(),
            ));
        }
        Ok(ast::Expr::Identifier(self.new_ident(col.name.to_string())))
    }

    fn new_ident(&self, str: String) -> ast::Ident {
        ast::Ident {
            value: str,
            quote_style: self.dialect.identifier_quote_style(),
        }
    }

    fn binary_op_to_sql(
        &self,
        lhs: ast::Expr,
        rhs: ast::Expr,
        op: ast::BinaryOperator,
    ) -> ast::Expr {
        ast::Expr::BinaryOp {
            left: Box::new(lhs),
            op,
            right: Box::new(rhs),
        }
    }

    fn op_to_sql(&self, op: &Operator) -> Result<ast::BinaryOperator> {
        match op {
            Operator::Eq => Ok(ast::BinaryOperator::Eq),
            Operator::NotEq => Ok(ast::BinaryOperator::NotEq),
            Operator::Lt => Ok(ast::BinaryOperator::Lt),
            Operator::LtEq => Ok(ast::BinaryOperator::LtEq),
            Operator::Gt => Ok(ast::BinaryOperator::Gt),
            Operator::GtEq => Ok(ast::BinaryOperator::GtEq),
            Operator::Plus => Ok(ast::BinaryOperator::Plus),
            Operator::Minus => Ok(ast::BinaryOperator::Minus),
            Operator::Multiply => Ok(ast::BinaryOperator::Multiply),
            Operator::Divide => Ok(ast::BinaryOperator::Divide),
            Operator::Modulo => Ok(ast::BinaryOperator::Modulo),
            Operator::And => Ok(ast::BinaryOperator::And),
            Operator::Or => Ok(ast::BinaryOperator::Or),
            Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
            Operator::RegexMatch => Ok(ast::BinaryOperator::PGRegexMatch),
            Operator::RegexIMatch => Ok(ast::BinaryOperator::PGRegexIMatch),
            Operator::RegexNotMatch => Ok(ast::BinaryOperator::PGRegexNotMatch),
            Operator::RegexNotIMatch => Ok(ast::BinaryOperator::PGRegexNotIMatch),
            Operator::ILikeMatch => Ok(ast::BinaryOperator::PGILikeMatch),
            Operator::NotLikeMatch => Ok(ast::BinaryOperator::PGNotLikeMatch),
            Operator::LikeMatch => Ok(ast::BinaryOperator::PGLikeMatch),
            Operator::NotILikeMatch => Ok(ast::BinaryOperator::PGNotILikeMatch),
            Operator::BitwiseAnd => Ok(ast::BinaryOperator::BitwiseAnd),
            Operator::BitwiseOr => Ok(ast::BinaryOperator::BitwiseOr),
            Operator::BitwiseXor => Ok(ast::BinaryOperator::BitwiseXor),
            Operator::BitwiseShiftRight => Ok(ast::BinaryOperator::PGBitwiseShiftRight),
            Operator::BitwiseShiftLeft => Ok(ast::BinaryOperator::PGBitwiseShiftLeft),
            Operator::StringConcat => Ok(ast::BinaryOperator::StringConcat),
            Operator::AtArrow => not_impl_err!("unsupported operation: {op:?}"),
            Operator::ArrowAt => not_impl_err!("unsupported operation: {op:?}"),
        }
    }

    fn scalar_to_sql(&self, v: &ScalarValue) -> Result<ast::Value> {
        match v {
            ScalarValue::Null => Ok(ast::Value::Null),
            ScalarValue::Boolean(Some(b)) => Ok(ast::Value::Boolean(b.to_owned())),
            ScalarValue::Boolean(None) => Ok(ast::Value::Null),
            ScalarValue::Float32(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
            ScalarValue::Float32(None) => Ok(ast::Value::Null),
            ScalarValue::Float64(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
            ScalarValue::Float64(None) => Ok(ast::Value::Null),
            ScalarValue::Decimal128(Some(_), ..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Decimal128(None, ..) => Ok(ast::Value::Null),
            ScalarValue::Decimal256(Some(_), ..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Decimal256(None, ..) => Ok(ast::Value::Null),
            ScalarValue::Int8(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
            ScalarValue::Int8(None) => Ok(ast::Value::Null),
            ScalarValue::Int16(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
            ScalarValue::Int16(None) => Ok(ast::Value::Null),
            ScalarValue::Int32(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
            ScalarValue::Int32(None) => Ok(ast::Value::Null),
            ScalarValue::Int64(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
            ScalarValue::Int64(None) => Ok(ast::Value::Null),
            ScalarValue::UInt8(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
            ScalarValue::UInt8(None) => Ok(ast::Value::Null),
            ScalarValue::UInt16(Some(ui)) => {
                Ok(ast::Value::Number(ui.to_string(), false))
            }
            ScalarValue::UInt16(None) => Ok(ast::Value::Null),
            ScalarValue::UInt32(Some(ui)) => {
                Ok(ast::Value::Number(ui.to_string(), false))
            }
            ScalarValue::UInt32(None) => Ok(ast::Value::Null),
            ScalarValue::UInt64(Some(ui)) => {
                Ok(ast::Value::Number(ui.to_string(), false))
            }
            ScalarValue::UInt64(None) => Ok(ast::Value::Null),
            ScalarValue::Utf8(Some(str)) => {
                Ok(ast::Value::SingleQuotedString(str.to_string()))
            }
            ScalarValue::Utf8(None) => Ok(ast::Value::Null),
            ScalarValue::LargeUtf8(Some(str)) => {
                Ok(ast::Value::SingleQuotedString(str.to_string()))
            }
            ScalarValue::LargeUtf8(None) => Ok(ast::Value::Null),
            ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Binary(None) => Ok(ast::Value::Null),
            ScalarValue::FixedSizeBinary(..) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::LargeBinary(Some(_)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::LargeBinary(None) => Ok(ast::Value::Null),
            ScalarValue::FixedSizeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::List(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::LargeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Date32(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Date32(None) => Ok(ast::Value::Null),
            ScalarValue::Date64(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Date64(None) => Ok(ast::Value::Null),
            ScalarValue::Time32Second(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time32Second(None) => Ok(ast::Value::Null),
            ScalarValue::Time32Millisecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time32Millisecond(None) => Ok(ast::Value::Null),
            ScalarValue::Time64Microsecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time64Microsecond(None) => Ok(ast::Value::Null),
            ScalarValue::Time64Nanosecond(Some(_t)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::Time64Nanosecond(None) => Ok(ast::Value::Null),
            ScalarValue::TimestampSecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampSecond(None, _) => Ok(ast::Value::Null),
            ScalarValue::TimestampMillisecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampMillisecond(None, _) => Ok(ast::Value::Null),
            ScalarValue::TimestampMicrosecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampMicrosecond(None, _) => Ok(ast::Value::Null),
            ScalarValue::TimestampNanosecond(Some(_ts), _) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::TimestampNanosecond(None, _) => Ok(ast::Value::Null),
            ScalarValue::IntervalYearMonth(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::IntervalYearMonth(None) => Ok(ast::Value::Null),
            ScalarValue::IntervalDayTime(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::IntervalDayTime(None) => Ok(ast::Value::Null),
            ScalarValue::IntervalMonthDayNano(Some(_i)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::IntervalMonthDayNano(None) => Ok(ast::Value::Null),
            ScalarValue::DurationSecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationSecond(None) => Ok(ast::Value::Null),
            ScalarValue::DurationMillisecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMillisecond(None) => Ok(ast::Value::Null),
            ScalarValue::DurationMicrosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationMicrosecond(None) => Ok(ast::Value::Null),
            ScalarValue::DurationNanosecond(Some(_d)) => {
                not_impl_err!("Unsupported scalar: {v:?}")
            }
            ScalarValue::DurationNanosecond(None) => Ok(ast::Value::Null),
            ScalarValue::Struct(_) => not_impl_err!("Unsupported scalar: {v:?}"),
            ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::TableReference;
    use datafusion_expr::{col, lit};

    use crate::unparser::dialect::CustomDialect;

    use super::*;

    #[test]
    fn expr_to_sql_ok() -> Result<()> {
        let tests: Vec<(Expr, &str)> = vec![
            (col("a").gt(lit(4)), r#"a > 4"#),
            (
                Expr::Column(Column {
                    relation: Some(TableReference::partial("a", "b")),
                    name: "c".to_string(),
                })
                .gt(lit(4)),
                r#"a.b.c > 4"#,
            ),
        ];

        for (expr, expected) in tests {
            let ast = expr_to_sql(&expr)?;

            let actual = format!("{}", ast);

            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn custom_dialect() -> Result<()> {
        let dialect = CustomDialect::new(Some('\''));
        let unparser = Unparser::new(&dialect);

        let expr = col("a").gt(lit(4));
        let ast = unparser.expr_to_sql(&expr)?;

        let actual = format!("{}", ast);

        let expected = r#"'a' > 4"#;
        assert_eq!(actual, expected);

        Ok(())
    }
}

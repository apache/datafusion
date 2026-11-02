// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::planner::{ContextProvider, SqlToRel};
use datafusion_common::{Result, internal_datafusion_err, not_impl_err};
use datafusion_expr::Operator;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{BinaryExpr, Expr};
use sqlparser::ast::BinaryOperator;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn parse_sql_binary_op(&self, op: &BinaryOperator) -> Result<Operator> {
        match op {
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulo => Ok(Operator::Modulo),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            BinaryOperator::PGLikeMatch => Ok(Operator::LikeMatch),
            BinaryOperator::PGILikeMatch => Ok(Operator::ILikeMatch),
            BinaryOperator::PGNotLikeMatch => Ok(Operator::NotLikeMatch),
            BinaryOperator::PGNotILikeMatch => Ok(Operator::NotILikeMatch),
            BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            BinaryOperator::Xor => Ok(Operator::BitwiseXor),
            BinaryOperator::BitwiseXor => Ok(Operator::BitwiseXor),
            BinaryOperator::PGBitwiseXor => Ok(Operator::BitwiseXor),
            BinaryOperator::PGBitwiseShiftRight => Ok(Operator::BitwiseShiftRight),
            BinaryOperator::PGBitwiseShiftLeft => Ok(Operator::BitwiseShiftLeft),
            BinaryOperator::StringConcat => Ok(Operator::StringConcat),
            BinaryOperator::ArrowAt => Ok(Operator::ArrowAt),
            BinaryOperator::AtArrow => Ok(Operator::AtArrow),
            BinaryOperator::Arrow => Ok(Operator::Arrow),
            BinaryOperator::LongArrow => Ok(Operator::LongArrow),
            BinaryOperator::HashArrow => Ok(Operator::HashArrow),
            BinaryOperator::HashLongArrow => Ok(Operator::HashLongArrow),
            BinaryOperator::AtAt => Ok(Operator::AtAt),
            BinaryOperator::Spaceship => Ok(Operator::IsNotDistinctFrom),
            BinaryOperator::DuckIntegerDivide | BinaryOperator::MyIntegerDivide => {
                Ok(Operator::IntegerDivide)
            }
            BinaryOperator::HashMinus => Ok(Operator::HashMinus),
            BinaryOperator::AtQuestion => Ok(Operator::AtQuestion),
            BinaryOperator::Question => Ok(Operator::Question),
            BinaryOperator::QuestionAnd => Ok(Operator::QuestionAnd),
            BinaryOperator::QuestionPipe => Ok(Operator::QuestionPipe),
            BinaryOperator::Custom(s) if s == ":" => Ok(Operator::Colon),
            _ => not_impl_err!("Unsupported binary operator: {:?}", op),
        }
    }

    pub(crate) fn build_binary_expr(
        &self,
        op: &BinaryOperator,
        left: Expr,
        right: Expr,
    ) -> Result<Expr> {
        if matches!(op, BinaryOperator::PGExp) {
            let fun_name = "power";
            let fun = self
                .context_provider
                .get_function_meta(fun_name)
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Unable to find expected '{fun_name}' function"
                    )
                })?;

            return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                fun,
                vec![left, right],
            )));
        }

        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            self.parse_sql_binary_op(op)?,
            Box::new(right),
        )))
    }
}

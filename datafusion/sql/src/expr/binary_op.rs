// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::borrow::Cow;

use crate::ast::BinaryOp;
use crate::planner::{ContextProvider, SqlToRel};
use datafusion_common::{Result, not_impl_err};
use datafusion_expr::Operator;
use datafusion_expr::{BinaryExpr, Expr};

/// Detaches a binary operator from the SQL text it was parsed from, so that
/// it can be handed to an [`ExprPlanner`](datafusion_expr::planner::ExprPlanner).
pub(crate) fn binary_op_to_static(op: BinaryOp<'_>) -> BinaryOp<'static> {
    match op {
        BinaryOp::Or => BinaryOp::Or,
        BinaryOp::And => BinaryOp::And,
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NotEq => BinaryOp::NotEq,
        BinaryOp::Lt => BinaryOp::Lt,
        BinaryOp::LtEq => BinaryOp::LtEq,
        BinaryOp::Gt => BinaryOp::Gt,
        BinaryOp::GtEq => BinaryOp::GtEq,
        BinaryOp::Add => BinaryOp::Add,
        BinaryOp::Sub => BinaryOp::Sub,
        BinaryOp::Mul => BinaryOp::Mul,
        BinaryOp::Div => BinaryOp::Div,
        BinaryOp::Mod => BinaryOp::Mod,
        BinaryOp::StringConcat => BinaryOp::StringConcat,
        BinaryOp::BitOr => BinaryOp::BitOr,
        BinaryOp::BitAnd => BinaryOp::BitAnd,
        BinaryOp::BitXor => BinaryOp::BitXor,
        BinaryOp::ShiftLeft => BinaryOp::ShiftLeft,
        BinaryOp::ShiftRight => BinaryOp::ShiftRight,
        BinaryOp::Named(name) => BinaryOp::Named(Cow::Owned(name.into_owned())),
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn parse_sql_binary_op(&self, op: &BinaryOp<'_>) -> Result<Operator> {
        match op {
            BinaryOp::Gt => Ok(Operator::Gt),
            BinaryOp::GtEq => Ok(Operator::GtEq),
            BinaryOp::Lt => Ok(Operator::Lt),
            BinaryOp::LtEq => Ok(Operator::LtEq),
            BinaryOp::Eq => Ok(Operator::Eq),
            BinaryOp::NotEq => Ok(Operator::NotEq),
            BinaryOp::Add => Ok(Operator::Plus),
            BinaryOp::Sub => Ok(Operator::Minus),
            BinaryOp::Mul => Ok(Operator::Multiply),
            BinaryOp::Div => Ok(Operator::Divide),
            BinaryOp::Mod => Ok(Operator::Modulo),
            BinaryOp::And => Ok(Operator::And),
            BinaryOp::Or => Ok(Operator::Or),
            BinaryOp::BitAnd => Ok(Operator::BitwiseAnd),
            BinaryOp::BitOr => Ok(Operator::BitwiseOr),
            BinaryOp::BitXor => Ok(Operator::BitwiseXor),
            BinaryOp::ShiftRight => Ok(Operator::BitwiseShiftRight),
            BinaryOp::ShiftLeft => Ok(Operator::BitwiseShiftLeft),
            BinaryOp::StringConcat => Ok(Operator::StringConcat),
            BinaryOp::Named(name) => match name.as_ref() {
                "~" => Ok(Operator::RegexMatch),
                "~*" => Ok(Operator::RegexIMatch),
                "!~" => Ok(Operator::RegexNotMatch),
                "!~*" => Ok(Operator::RegexNotIMatch),
                "~~" => Ok(Operator::LikeMatch),
                "~~*" => Ok(Operator::ILikeMatch),
                "!~~" => Ok(Operator::NotLikeMatch),
                "!~~*" => Ok(Operator::NotILikeMatch),
                "XOR" | "#" => Ok(Operator::BitwiseXor),
                "<@" => Ok(Operator::ArrowAt),
                "@>" => Ok(Operator::AtArrow),
                "->" => Ok(Operator::Arrow),
                "->>" => Ok(Operator::LongArrow),
                "#>" => Ok(Operator::HashArrow),
                "#>>" => Ok(Operator::HashLongArrow),
                "@@" => Ok(Operator::AtAt),
                "<=>" => Ok(Operator::IsNotDistinctFrom),
                "//" | "DIV" => Ok(Operator::IntegerDivide),
                "#-" => Ok(Operator::HashMinus),
                "@?" => Ok(Operator::AtQuestion),
                "?" => Ok(Operator::Question),
                "?&" => Ok(Operator::QuestionAnd),
                "?|" => Ok(Operator::QuestionPipe),
                ":" => Ok(Operator::Colon),
                _ => not_impl_err!("Unsupported binary operator: {op}"),
            },
        }
    }

    pub(crate) fn build_binary_expr(
        &self,
        op: &BinaryOp<'_>,
        left: Expr,
        right: Expr,
    ) -> Result<Expr> {
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            self.parse_sql_binary_op(op)?,
            Box::new(right),
        )))
    }
}

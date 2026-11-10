// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The SQL syntax tree that DataFusion plans.
//!
//! [`sqlparser`]'s tree is generic over the dialect's extensions. DataFusion
//! parses with built-in dialects, which all share [`Builtin`], so these
//! aliases fix that parameter.

pub use sqlparser::ast::{
    AngleArguments, ArrayType, BinaryOp, CastStyle, DataType, DataTypeSuffix, FrameUnits,
    GroupingKind, Ident, JoinKind, LikeKind, Literal, NullTreatment, ObjectName,
    ObjectType, OffsetRows, Quantifier, SetOperator, SetQuantifier, Span, Spanned,
    StringLiteral, StructField, TableAlias, TrimSide, TypedString, UnaryOp,
};
pub use sqlparser::dialect::Builtin;
pub use sqlparser::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement};

pub type ExprKind<'a> = sqlparser::ast::ExprKind<'a, Builtin>;
pub type Expr<'a> = sqlparser::ast::Expr<'a, Builtin>;
pub type Interval<'a> = sqlparser::ast::Interval<'a, Builtin>;
pub type SpecialExpr<'a> = sqlparser::ast::SpecialExpr<'a, Builtin>;
pub type SelectItemKind<'a> = sqlparser::ast::SelectItemKind<'a, Builtin>;
pub type SelectItem<'a> = sqlparser::ast::SelectItem<'a, Builtin>;
pub type WildcardOptions<'a> = sqlparser::ast::WildcardOptions<'a, Builtin>;
pub type FunctionArg<'a> = sqlparser::ast::FunctionArg<'a, Builtin>;
pub type TableFactor<'a> = sqlparser::ast::TableFactor<'a, Builtin>;
pub type TimeTravel<'a> = sqlparser::ast::TimeTravel<'a, Builtin>;
pub type MatchRecognize<'a> = sqlparser::ast::MatchRecognize<'a, Builtin>;
pub type ExprWithAlias<'a> = sqlparser::ast::ExprWithAlias<'a, Builtin>;
pub type TableSample<'a> = sqlparser::ast::TableSample<'a, Builtin>;
pub type SampleClause<'a> = sqlparser::ast::SampleClause<'a, Builtin>;
pub type SampleAmount<'a> = sqlparser::ast::SampleAmount<'a, Builtin>;
pub type Pivot<'a> = sqlparser::ast::Pivot<'a, Builtin>;
pub type PivotClause<'a> = sqlparser::ast::PivotClause<'a, Builtin>;
pub type PivotSource<'a> = sqlparser::ast::PivotSource<'a, Builtin>;
pub type Unpivot<'a> = sqlparser::ast::Unpivot<'a, Builtin>;
pub type UnpivotClause<'a> = sqlparser::ast::UnpivotClause<'a, Builtin>;
pub type TableRef<'a> = sqlparser::ast::TableRef<'a, Builtin>;
pub type JoinConstraint<'a> = sqlparser::ast::JoinConstraint<'a, Builtin>;
pub type Join<'a> = sqlparser::ast::Join<'a, Builtin>;
pub type TableWithJoins<'a> = sqlparser::ast::TableWithJoins<'a, Builtin>;
pub type With<'a> = sqlparser::ast::With<'a, Builtin>;
pub type Cte<'a> = sqlparser::ast::Cte<'a, Builtin>;
pub type OrderByItem<'a> = sqlparser::ast::OrderByItem<'a, Builtin>;
pub type Limit<'a> = sqlparser::ast::Limit<'a, Builtin>;
pub type Fetch<'a> = sqlparser::ast::Fetch<'a, Builtin>;
pub type Query<'a> = sqlparser::ast::Query<'a, Builtin>;
pub type PipeOperator<'a> = sqlparser::ast::PipeOperator<'a, Builtin>;
pub type SetExpr<'a> = sqlparser::ast::SetExpr<'a, Builtin>;
pub type Select<'a> = sqlparser::ast::Select<'a, Builtin>;
pub type SelectInto<'a> = sqlparser::ast::SelectInto<'a, Builtin>;
pub type LateralView<'a> = sqlparser::ast::LateralView<'a, Builtin>;
pub type QueryDistribution<'a> = sqlparser::ast::QueryDistribution<'a, Builtin>;
pub type ConnectBy<'a> = sqlparser::ast::ConnectBy<'a, Builtin>;
pub type NamedWindow<'a> = sqlparser::ast::NamedWindow<'a, Builtin>;
pub type StatementKind<'a> = sqlparser::ast::StatementKind<'a, Builtin>;
pub type Statement<'a> = sqlparser::ast::Statement<'a, Builtin>;
pub type InsertSource<'a> = sqlparser::ast::InsertSource<'a, Builtin>;
pub type Insert<'a> = sqlparser::ast::Insert<'a, Builtin>;
pub type Assignment<'a> = sqlparser::ast::Assignment<'a, Builtin>;
pub type Update<'a> = sqlparser::ast::Update<'a, Builtin>;
pub type Delete<'a> = sqlparser::ast::Delete<'a, Builtin>;
pub type ColumnOption<'a> = sqlparser::ast::ColumnOption<'a, Builtin>;
pub type ColumnDef<'a> = sqlparser::ast::ColumnDef<'a, Builtin>;
pub type TableConstraintKind<'a> = sqlparser::ast::TableConstraintKind<'a, Builtin>;
pub type TableConstraint<'a> = sqlparser::ast::TableConstraint<'a, Builtin>;
pub type CreateTable<'a> = sqlparser::ast::CreateTable<'a, Builtin>;
pub type IsTest<'a> = sqlparser::ast::IsTest<'a, Builtin>;
pub type CaseWhen<'a> = sqlparser::ast::CaseWhen<'a, Builtin>;
pub type CaseExpr<'a> = sqlparser::ast::CaseExpr<'a, Builtin>;
pub type FunctionOptions<'a> = sqlparser::ast::FunctionOptions<'a, Builtin>;
pub type WindowOver<'a> = sqlparser::ast::WindowOver<'a, Builtin>;
pub type WindowSpec<'a> = sqlparser::ast::WindowSpec<'a, Builtin>;
pub type FrameBound<'a> = sqlparser::ast::FrameBound<'a, Builtin>;
pub type WindowFrame<'a> = sqlparser::ast::WindowFrame<'a, Builtin>;
pub type Merge<'a> = sqlparser::ast::Merge<'a, Builtin>;
pub type MergeClause<'a> = sqlparser::ast::MergeClause<'a, Builtin>;
pub type MergeAction<'a> = sqlparser::ast::MergeAction<'a, Builtin>;
pub type Show<'a> = sqlparser::ast::Show<'a, Builtin>;
pub type ShowClause<'a> = sqlparser::ast::ShowClause<'a, Builtin>;
pub type ExplainTarget<'a> = sqlparser::ast::ExplainTarget<'a, Builtin>;
pub type Explain<'a> = sqlparser::ast::Explain<'a, Builtin>;
pub type SetAssignment<'a> = sqlparser::ast::SetAssignment<'a, Builtin>;
pub type SetStatement<'a> = sqlparser::ast::SetStatement<'a, Builtin>;
pub type AlterTable<'a> = sqlparser::ast::AlterTable<'a, Builtin>;
pub type CreateView<'a> = sqlparser::ast::CreateView<'a, Builtin>;
pub type CreateIndex<'a> = sqlparser::ast::CreateIndex<'a, Builtin>;
pub type IndexColumn<'a> = sqlparser::ast::IndexColumn<'a, Builtin>;
pub type PipeAggregate<'a> = sqlparser::ast::PipeAggregate<'a, Builtin>;

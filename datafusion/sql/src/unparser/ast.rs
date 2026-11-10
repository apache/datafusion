// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Builders for the SQL syntax tree that the unparser produces, and helpers
//! that make its nodes.
//!
//! Every tree built here owns its text, so it is `'static`.

use core::fmt;
use std::borrow::Cow;
use std::ops::ControlFlow;

use sqlparser::ast::{StringPrefix, visit_expressions_mut};
use sqlparser::dialect::bigquery::{BigQueryTableFactor, BigQueryWithOffset};
use sqlparser::dialect::builtin::{BuiltinQueryTail, BuiltinTableFactor};

use crate::ast::{
    ArrayType, BinaryOp, BuiltinSelect, DataType, DataTypeSuffix, Expr, ExprKind, Fetch,
    FunctionArg, Ident, Join, LateralView, Limit, Literal, NamedWindow, ObjectName,
    OffsetRows, OrderByItem, Query, Select, SelectInto, SelectItem, SelectItemKind,
    SetExpr, SetQuantifier, Span, StringLiteral, TableAlias, TableFactor, TableRef,
    TableWithJoins, With,
};

/// An identifier with no quotes.
pub fn ident(text: impl Into<String>) -> Ident<'static> {
    Ident {
        text: Cow::Owned(text.into()),
        quote: None,
        span: Span::default(),
    }
}

/// An identifier whose value is `value`, quoted with `quote` if it is `Some`.
///
/// Any closing quote in `value` is doubled, so that the identifier renders as
/// SQL that means `value`.
pub fn quoted_ident(value: impl Into<String>, quote: Option<char>) -> Ident<'static> {
    let value = value.into();
    let text = match quote {
        Some(quote) => {
            let close = if quote == '[' { ']' } else { quote };
            if value.contains(close) {
                value.replace(close, &format!("{close}{close}"))
            } else {
                value
            }
        }
        None => value,
    };
    Ident {
        text: Cow::Owned(text),
        quote,
        span: Span::default(),
    }
}

/// A possibly qualified name.
pub fn object_name(parts: Vec<Ident<'static>>) -> ObjectName<'static> {
    ObjectName { parts }
}

/// A column or other name: an identifier, or a compound name if it has more
/// than one part.
pub fn name_expr(mut parts: Vec<Ident<'static>>) -> Expr<'static> {
    if parts.len() == 1 {
        ExprKind::Ident(parts.remove(0)).into()
    } else {
        ExprKind::Compound(ObjectName { parts }).into()
    }
}

/// A number, written as `text`.
pub fn number(text: impl Into<String>) -> Expr<'static> {
    ExprKind::Literal(Literal::Number(Cow::Owned(text.into()))).into()
}

/// A single-quoted string literal whose value is `value`.
pub fn string_literal(value: &str) -> StringLiteral<'static> {
    StringLiteral {
        text: Cow::Owned(value.replace('\'', "''")),
        quote: '\'',
        backslash_escapes: false,
        national: false,
        prefix: StringPrefix::None,
        triple: false,
    }
}

/// A single-quoted string whose value is `value`.
pub fn string(value: &str) -> Expr<'static> {
    ExprKind::Literal(Literal::String(string_literal(value))).into()
}

/// `NULL`
pub fn null() -> Expr<'static> {
    ExprKind::Literal(Literal::Null).into()
}

/// A binary operation.
pub fn binary(
    left: Expr<'static>,
    op: BinaryOp<'static>,
    right: Expr<'static>,
) -> Expr<'static> {
    ExprKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
    .into()
}

/// An expression in parentheses.
pub fn nested(expr: Expr<'static>) -> Expr<'static> {
    ExprKind::Nested(Box::new(expr)).into()
}

/// A call of the function `name` with `args` and no other clauses.
pub fn function(name: &str, args: Vec<FunctionArg<'static>>) -> Expr<'static> {
    ExprKind::Function {
        name: object_name(vec![ident(name)]),
        args,
        options: None,
    }
    .into()
}

/// A data type written as `name`, with no arguments.
pub fn data_type(name: &str) -> DataType<'static> {
    DataType {
        qualifier: Vec::new(),
        name: ident(name),
        angle: None,
        args: Vec::new(),
        suffix: DataTypeSuffix::None,
        array: ArrayType::None,
    }
}

/// A data type written as `name` followed by `args` in parentheses.
pub fn data_type_with_args(name: &str, args: &[impl ToString]) -> DataType<'static> {
    DataType {
        args: args.iter().map(|arg| Cow::Owned(arg.to_string())).collect(),
        ..data_type(name)
    }
}

/// A data type written as `name` followed by the words of `suffix`.
pub fn data_type_with_suffix(name: &str, suffix: DataTypeSuffix) -> DataType<'static> {
    DataType {
        suffix,
        ..data_type(name)
    }
}

/// An item of a select list with no alias.
pub fn select_expr(expr: Expr<'static>) -> SelectItem<'static> {
    SelectItemKind::Expr { expr, alias: None }.into()
}

/// An item of a select list with an alias.
pub fn select_expr_with_alias(
    expr: Expr<'static>,
    alias: Ident<'static>,
) -> SelectItem<'static> {
    SelectItemKind::Expr {
        expr,
        alias: Some(alias),
    }
    .into()
}

/// `*` in a select list.
pub fn select_wildcard() -> SelectItem<'static> {
    SelectItemKind::Wildcard.into()
}

/// A table alias, written with `AS`.
pub fn table_alias(
    name: Ident<'static>,
    columns: Vec<Ident<'static>>,
) -> TableAlias<'static> {
    TableAlias {
        name,
        explicit: true,
        columns,
        column_types: Vec::new(),
    }
}

/// A query whose body is `body` and that has no other clauses.
pub fn query(body: SetExpr<'static>) -> Query<'static> {
    Query {
        with: None,
        body,
        order_by: Vec::new(),
        distribution: None,
        limit: None,
        fetch: None,
        locks: Vec::new(),
        tail: BuiltinQueryTail::default(),
        pipes: Vec::new(),
    }
}

/// How a [`SelectBuilder`] removes duplicate rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Distinct {
    /// `SELECT DISTINCT`
    Distinct,
    /// `SELECT DISTINCT ON (exprs)`
    On(Vec<Expr<'static>>),
}

#[derive(Clone)]
pub struct QueryBuilder {
    with: Option<With<'static>>,
    body: Option<Box<SetExpr<'static>>>,
    order_by: Option<Vec<OrderByItem<'static>>>,
    limit: Option<Expr<'static>>,
    limit_by: Vec<Expr<'static>>,
    offset: Option<Expr<'static>>,
    fetch: Option<Fetch<'static>>,
    tail: BuiltinQueryTail<'static>,
    // If true, we need to unparse LogicalPlan::Union as a SQL `UNION` rather than a `UNION ALL`.
    distinct_union: bool,
}

impl QueryBuilder {
    pub fn with(&mut self, value: Option<With<'static>>) -> &mut Self {
        self.with = value;
        self
    }
    pub fn body(&mut self, value: Box<SetExpr<'static>>) -> &mut Self {
        self.body = Some(value);
        self
    }
    pub fn take_body(&mut self) -> Option<Box<SetExpr<'static>>> {
        self.body.take()
    }
    pub fn order_by(&mut self, value: Vec<OrderByItem<'static>>) -> &mut Self {
        self.order_by = Some(value);
        self
    }
    pub fn limit(&mut self, value: Option<Expr<'static>>) -> &mut Self {
        self.limit = value;
        self
    }
    pub fn limit_by(&mut self, value: Vec<Expr<'static>>) -> &mut Self {
        self.limit_by = value;
        self
    }
    pub fn offset(&mut self, value: Option<Expr<'static>>) -> &mut Self {
        self.offset = value;
        self
    }
    pub fn fetch(&mut self, value: Option<Fetch<'static>>) -> &mut Self {
        self.fetch = value;
        self
    }
    /// Dialect-specific clauses at the end of the query, such as Microsoft
    /// SQL Server's `FOR XML`.
    pub fn tail(&mut self, value: BuiltinQueryTail<'static>) -> &mut Self {
        self.tail = value;
        self
    }
    pub fn distinct_union(&mut self) -> &mut Self {
        self.distinct_union = true;
        self
    }
    pub fn is_distinct_union(&self) -> bool {
        self.distinct_union
    }
    pub fn build(&self) -> Result<Query<'static>, BuilderError> {
        let body = match self.body {
            Some(ref value) => value.as_ref().clone(),
            None => return Err(Into::into(UninitializedFieldError::from("body"))),
        };
        let limit =
            (self.limit.is_some() || self.offset.is_some() || !self.limit_by.is_empty())
                .then(|| {
                    Box::new(Limit {
                        count: self.limit.clone(),
                        offset: self.offset.clone(),
                        offset_rows: OffsetRows::None,
                        offset_comma: false,
                        all: false,
                        by: self.limit_by.clone(),
                    })
                });
        Ok(Query {
            with: self.with.clone().map(Box::new),
            order_by: self.order_by.clone().unwrap_or_default(),
            limit,
            fetch: self.fetch.clone().map(Box::new),
            tail: self.tail.clone(),
            ..query(body)
        })
    }
    fn create_empty() -> Self {
        Self {
            with: Default::default(),
            body: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            limit_by: Default::default(),
            offset: Default::default(),
            fetch: Default::default(),
            tail: Default::default(),
            distinct_union: false,
        }
    }
}
impl Default for QueryBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
pub struct SelectBuilder {
    distinct: Option<Distinct>,
    modifiers: BuiltinSelect<'static>,
    /// Projection items for the SELECT clause.
    ///
    /// This field uses `Option` to distinguish between three distinct states:
    /// - `None`: No projection has been set (not yet initialized)
    /// - `Some(vec![])`: Empty projection explicitly set (generates `SELECT FROM ...` or `SELECT 1 FROM ...`)
    /// - `Some(vec![SelectItemKind::Wildcard])`: Wildcard projection (generates `SELECT * FROM ...`)
    /// - `Some(vec![...])`: Non-empty projection with specific columns/expressions
    ///
    /// Use `projection()` to set this field and `already_projected()` to check if it has been set.
    projection: Option<Vec<SelectItem<'static>>>,
    into: Option<SelectInto<'static>>,
    from: Vec<TableWithJoinsBuilder>,
    lateral_views: Vec<LateralView<'static>>,
    selection: Option<Expr<'static>>,
    group_by: Option<Vec<Expr<'static>>>,
    having: Option<Expr<'static>>,
    named_window: Vec<NamedWindow<'static>>,
    qualify: Option<Expr<'static>>,
    /// Counter for generating unique LATERAL FLATTEN aliases within this SELECT.
    flatten_alias_counter: usize,
    /// Table aliases that correspond to LATERAL FLATTEN relations.
    /// Column references into these aliases must use `VALUE` as the column name.
    flatten_table_aliases: Vec<String>,
    /// Depth of explicitly named subqueries currently being rendered.
    subquery_alias_depth: usize,
}

/// Prefix used for auto-generated LATERAL FLATTEN table aliases.
const FLATTEN_ALIAS_PREFIX: &str = "_unnest";

impl SelectBuilder {
    /// Generate a unique alias for a LATERAL FLATTEN relation
    /// (`_unnest_1`, `_unnest_2`, …). Each call returns a fresh name.
    pub fn next_flatten_alias(&mut self) -> String {
        self.flatten_alias_counter += 1;
        format!("{FLATTEN_ALIAS_PREFIX}_{}", self.flatten_alias_counter)
    }

    /// Register a table alias as pointing to a LATERAL FLATTEN relation.
    pub fn add_flatten_table_alias(&mut self, alias: String) {
        self.flatten_table_aliases.push(alias);
    }

    /// Returns true if no FLATTEN table aliases have been registered.
    pub fn flatten_table_aliases_empty(&self) -> bool {
        self.flatten_table_aliases.is_empty()
    }

    /// Returns true if the given table alias refers to a FLATTEN relation.
    pub fn is_flatten_table_alias(&self, alias: &str) -> bool {
        self.flatten_table_aliases.iter().any(|a| a == alias)
    }

    pub(super) fn enter_subquery_alias(&mut self) {
        self.subquery_alias_depth += 1;
    }

    pub(super) fn exit_subquery_alias(&mut self) {
        debug_assert!(self.subquery_alias_depth > 0);
        self.subquery_alias_depth -= 1;
    }

    pub(super) fn inside_subquery_alias(&self) -> bool {
        self.subquery_alias_depth > 0
    }

    /// Returns the most recently generated flatten alias, or `None` if
    /// `next_flatten_alias` has not been called yet.
    pub fn current_flatten_alias(&self) -> Option<String> {
        if self.flatten_alias_counter > 0 {
            Some(format!(
                "{FLATTEN_ALIAS_PREFIX}_{}",
                self.flatten_alias_counter
            ))
        } else {
            None
        }
    }

    pub fn distinct(&mut self, value: Option<Distinct>) -> &mut Self {
        self.distinct = value;
        self
    }
    /// Dialect-specific syntax directly after `SELECT`, such as `TOP`.
    pub fn modifiers(&mut self, value: BuiltinSelect<'static>) -> &mut Self {
        self.modifiers = value;
        self
    }
    pub fn projection(&mut self, value: Vec<SelectItem<'static>>) -> &mut Self {
        self.projection = Some(value);
        self
    }
    pub fn pop_projections(&mut self) -> Vec<SelectItem<'static>> {
        self.projection.take().unwrap_or_default()
    }
    /// Returns true if a projection has been explicitly set via `projection()`.
    ///
    /// This method is used to determine whether the SELECT clause has already been
    /// defined, which helps avoid creating duplicate projection nodes during query
    /// unparsing. It returns `true` for both empty and non-empty projections.
    ///
    /// # Returns
    ///
    /// - `true` if `projection()` has been called (regardless of whether it was empty or not)
    /// - `false` if no projection has been set yet
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut builder = SelectBuilder::default();
    /// assert!(!builder.already_projected());
    ///
    /// builder.projection(vec![]);
    /// assert!(builder.already_projected()); // true even for empty projection
    ///
    /// builder.projection(vec![select_wildcard()]);
    /// assert!(builder.already_projected()); // true for non-empty projection
    /// ```
    pub fn already_projected(&self) -> bool {
        self.projection.is_some()
    }
    pub fn into(&mut self, value: Option<SelectInto<'static>>) -> &mut Self {
        self.into = value;
        self
    }
    pub fn from(&mut self, value: Vec<TableWithJoinsBuilder>) -> &mut Self {
        self.from = value;
        self
    }
    pub fn push_from(&mut self, value: TableWithJoinsBuilder) -> &mut Self {
        self.from.push(value);
        self
    }
    pub fn pop_from(&mut self) -> Option<TableWithJoinsBuilder> {
        self.from.pop()
    }
    pub fn has_selection(&self) -> bool {
        self.selection.is_some()
    }
    pub fn lateral_views(&mut self, value: Vec<LateralView<'static>>) -> &mut Self {
        self.lateral_views = value;
        self
    }

    /// Replaces the selection with a new value.
    ///
    /// This function is used to replace a specific expression within the selection.
    /// Unlike the `selection` method which combines existing and new selections with AND,
    /// this method searches for and replaces occurrences of a specific expression.
    ///
    /// This method is primarily used to modify LEFT MARK JOIN expressions.
    /// When processing a LEFT MARK JOIN, we need to replace the placeholder expression
    /// with the actual join condition in the selection clause.
    ///
    /// # Arguments
    ///
    /// * `existing_expr` - The expression to replace
    /// * `value` - The new expression to set as the selection
    pub fn replace_mark(
        &mut self,
        existing_expr: &Expr<'static>,
        value: &Expr<'static>,
    ) -> &mut Self {
        if let Some(selection) = &mut self.selection {
            let _ = visit_expressions_mut(selection, |expr| {
                if expr == existing_expr {
                    *expr = value.clone();
                }
                ControlFlow::<()>::Continue(())
            });
        }
        self
    }

    pub fn selection(&mut self, value: Option<Expr<'static>>) -> &mut Self {
        // With filter pushdown optimization, the LogicalPlan can have filters defined as part of `TableScan` and `Filter` nodes.
        // To avoid overwriting one of the filters, we combine the existing filter with the additional filter.
        // Example:
        // |  Projection: customer.c_phone AS cntrycode, customer.c_acctbal
        // |   Filter: CAST(customer.c_acctbal AS Decimal128(38, 6)) > (<subquery>)
        // |     Subquery:
        // |     ..
        // |     TableScan: customer, full_filters=[customer.c_mktsegment = Utf8("BUILDING")]
        match (self.selection.take(), value) {
            (Some(existing_selection), Some(new_selection)) => {
                self.selection =
                    Some(binary(existing_selection, BinaryOp::And, new_selection));
            }
            (None, Some(new_selection)) => {
                self.selection = Some(new_selection);
            }
            (existing, None) => self.selection = existing,
        }

        self
    }
    pub fn group_by(&mut self, value: Vec<Expr<'static>>) -> &mut Self {
        self.group_by = Some(value);
        self
    }
    pub fn having(&mut self, value: Option<Expr<'static>>) -> &mut Self {
        self.having = value;
        self
    }
    pub fn named_window(&mut self, value: Vec<NamedWindow<'static>>) -> &mut Self {
        self.named_window = value;
        self
    }
    pub fn qualify(&mut self, value: Option<Expr<'static>>) -> &mut Self {
        self.qualify = value;
        self
    }
    pub fn build(&self) -> Result<Select<'static>, BuilderError> {
        let mut modifiers = self.modifiers.clone();
        let quantifier = match &self.distinct {
            Some(Distinct::Distinct) => SetQuantifier::Distinct,
            Some(Distinct::On(exprs)) => {
                modifiers.distinct_on = Some(exprs.clone());
                SetQuantifier::None
            }
            None => SetQuantifier::None,
        };
        Ok(Select {
            modifiers,
            quantifier,
            projection: self.projection.clone().unwrap_or_default(),
            into: self.into.clone().map(Box::new),
            from: self
                .from
                .iter()
                .filter_map(|b| b.build().transpose())
                .collect::<Result<Vec<_>, BuilderError>>()?,
            lateral_views: self.lateral_views.clone(),
            prewhere: None,
            selection: self.selection.clone(),
            connect_by: Vec::new(),
            group_by: match self.group_by {
                Some(ref value) => value.clone(),
                None => {
                    return Err(Into::into(UninitializedFieldError::from("group_by")));
                }
            },
            group_by_all: false,
            group_by_modifier: None,
            grouping_sets: None,
            having: self.having.clone(),
            qualify: self.qualify.clone(),
            windows: self.named_window.clone(),
            from_first: false,
            span: Default::default(),
        })
    }
    fn create_empty() -> Self {
        Self {
            distinct: Default::default(),
            modifiers: Default::default(),
            projection: None,
            into: Default::default(),
            from: Default::default(),
            lateral_views: Default::default(),
            selection: Default::default(),
            group_by: Some(Vec::new()),
            having: Default::default(),
            named_window: Default::default(),
            qualify: Default::default(),
            flatten_alias_counter: 0,
            flatten_table_aliases: Vec::new(),
            subquery_alias_depth: 0,
        }
    }
}
impl Default for SelectBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
pub struct TableWithJoinsBuilder {
    relation: Option<RelationBuilder>,
    joins: Vec<Join<'static>>,
}

impl TableWithJoinsBuilder {
    pub fn relation(&mut self, value: RelationBuilder) -> &mut Self {
        self.relation = Some(value);
        self
    }

    pub fn joins(&mut self, value: Vec<Join<'static>>) -> &mut Self {
        self.joins = value;
        self
    }
    pub fn push_join(&mut self, value: Join<'static>) -> &mut Self {
        self.joins.push(value);
        self
    }

    pub fn build(&self) -> Result<Option<TableWithJoins<'static>>, BuilderError> {
        match self.relation {
            Some(ref value) => match value.build()? {
                Some(relation) => Ok(Some(TableWithJoins {
                    relation,
                    joins: self.joins.clone(),
                })),
                None => Ok(None),
            },
            None => Err(Into::into(UninitializedFieldError::from("relation"))),
        }
    }
    fn create_empty() -> Self {
        Self {
            relation: Default::default(),
            joins: Default::default(),
        }
    }
}
impl Default for TableWithJoinsBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
pub struct RelationBuilder {
    relation: Option<TableFactorBuilder>,
}

#[derive(Clone)]
enum TableFactorBuilder {
    Table(TableRelationBuilder),
    Derived(DerivedRelationBuilder),
    NestedJoin(TableWithJoins<'static>, Option<TableAlias<'static>>),
    Unnest(UnnestRelationBuilder),
    Flatten(FlattenRelationBuilder),
    Empty,
}

impl RelationBuilder {
    pub fn has_relation(&self) -> bool {
        self.relation.is_some()
    }
    pub fn table(&mut self, value: TableRelationBuilder) -> &mut Self {
        self.relation = Some(TableFactorBuilder::Table(value));
        self
    }
    pub fn derived(&mut self, value: DerivedRelationBuilder) -> &mut Self {
        self.relation = Some(TableFactorBuilder::Derived(value));
        self
    }

    pub fn nested_join(
        &mut self,
        value: TableWithJoins<'static>,
        alias: Option<TableAlias<'static>>,
    ) -> &mut Self {
        self.relation = Some(TableFactorBuilder::NestedJoin(value, alias));
        self
    }

    pub fn unnest(&mut self, value: UnnestRelationBuilder) -> &mut Self {
        self.relation = Some(TableFactorBuilder::Unnest(value));
        self
    }

    pub fn flatten(&mut self, value: FlattenRelationBuilder) -> &mut Self {
        self.relation = Some(TableFactorBuilder::Flatten(value));
        self
    }

    pub fn empty(&mut self) -> &mut Self {
        self.relation = Some(TableFactorBuilder::Empty);
        self
    }
    pub fn alias(&mut self, value: Option<TableAlias<'static>>) -> &mut Self {
        let new = self;
        match new.relation {
            Some(TableFactorBuilder::Table(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            Some(TableFactorBuilder::Derived(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            Some(TableFactorBuilder::NestedJoin(_, ref mut alias)) => {
                *alias = value;
            }
            Some(TableFactorBuilder::Unnest(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            Some(TableFactorBuilder::Flatten(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            Some(TableFactorBuilder::Empty) => (),
            None => (),
        }
        new
    }
    pub fn build(&self) -> Result<Option<TableRef<'static>>, BuilderError> {
        Ok(match self.relation {
            Some(TableFactorBuilder::Table(ref value)) => Some(value.build()?),
            Some(TableFactorBuilder::Derived(ref value)) => Some(value.build()?),
            Some(TableFactorBuilder::NestedJoin(ref table_with_joins, ref alias)) => {
                Some(table_ref(
                    TableFactor::NestedJoin(Box::new(table_with_joins.clone())),
                    alias.clone(),
                ))
            }
            Some(TableFactorBuilder::Unnest(ref value)) => Some(value.build()?),
            Some(TableFactorBuilder::Flatten(ref value)) => Some(value.build()?),
            Some(TableFactorBuilder::Empty) => None,
            None => return Err(Into::into(UninitializedFieldError::from("relation"))),
        })
    }
    fn create_empty() -> Self {
        Self {
            relation: Default::default(),
        }
    }
}
impl Default for RelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

fn table_ref(
    factor: TableFactor<'static>,
    alias: Option<TableAlias<'static>>,
) -> TableRef<'static> {
    TableRef {
        factor,
        alias,
        span: Span::default(),
    }
}

#[derive(Clone)]
pub struct TableRelationBuilder {
    name: Option<ObjectName<'static>>,
    alias: Option<TableAlias<'static>>,
    args: Option<Vec<FunctionArg<'static>>>,
}

impl TableRelationBuilder {
    pub fn name(&mut self, value: ObjectName<'static>) -> &mut Self {
        self.name = Some(value);
        self
    }
    pub fn alias(&mut self, value: Option<TableAlias<'static>>) -> &mut Self {
        self.alias = value;
        self
    }
    /// Makes the table a call of a table function with these arguments.
    pub fn args(&mut self, value: Option<Vec<FunctionArg<'static>>>) -> &mut Self {
        self.args = value;
        self
    }
    pub fn build(&self) -> Result<TableRef<'static>, BuilderError> {
        let name = match self.name {
            Some(ref value) => value.clone(),
            None => return Err(Into::into(UninitializedFieldError::from("name"))),
        };
        let factor = match &self.args {
            Some(args) => TableFactor::Function {
                call: Box::new(
                    ExprKind::Function {
                        name,
                        args: args.clone(),
                        options: None,
                    }
                    .into(),
                ),
                with_ordinality: false,
            },
            None => TableFactor::Named(name),
        };
        Ok(table_ref(factor, self.alias.clone()))
    }
    fn create_empty() -> Self {
        Self {
            name: Default::default(),
            alias: Default::default(),
            args: Default::default(),
        }
    }
}
impl Default for TableRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}
#[derive(Clone)]
pub struct DerivedRelationBuilder {
    lateral: Option<bool>,
    subquery: Option<Box<Query<'static>>>,
    alias: Option<TableAlias<'static>>,
    projection_names: Vec<Ident<'static>>,
}

impl DerivedRelationBuilder {
    pub fn lateral(&mut self, value: bool) -> &mut Self {
        self.lateral = Some(value);
        self
    }
    pub fn subquery(&mut self, value: Box<Query<'static>>) -> &mut Self {
        self.subquery = Some(value);
        self
    }
    pub fn alias(&mut self, value: Option<TableAlias<'static>>) -> &mut Self {
        self.alias = value;
        self
    }
    pub(super) fn projection_names(&mut self, value: Vec<Ident<'static>>) -> &mut Self {
        self.projection_names = value;
        self
    }
    fn build(&self) -> Result<TableRef<'static>, BuilderError> {
        let mut subquery = match self.subquery {
            Some(ref value) => value.clone(),
            None => {
                return Err(Into::into(UninitializedFieldError::from("subquery")));
            }
        };
        if self
            .alias
            .as_ref()
            .is_none_or(|alias| alias.columns.is_empty())
            && let SetExpr::Select(select) = &mut subquery.body
            && select.projection.len() == self.projection_names.len()
        {
            for (item, alias) in select.projection.iter_mut().zip(&self.projection_names)
            {
                if let SelectItemKind::Expr { expr, alias: None } = &mut item.kind {
                    let preserves_name = match &expr.kind {
                        ExprKind::Ident(name) => name.value() == alias.value(),
                        ExprKind::Compound(names) => names
                            .parts
                            .last()
                            .is_some_and(|name| name.value() == alias.value()),
                        _ => false,
                    };
                    if !preserves_name {
                        item.kind = SelectItemKind::Expr {
                            expr: expr.clone(),
                            alias: Some(alias.clone()),
                        };
                    }
                }
            }
        }
        let derived = TableFactor::Derived(subquery);
        let factor = match self.lateral {
            Some(true) => TableFactor::Lateral(Box::new(derived)),
            Some(false) => derived,
            None => return Err(Into::into(UninitializedFieldError::from("lateral"))),
        };
        Ok(table_ref(factor, self.alias.clone()))
    }
    fn create_empty() -> Self {
        Self {
            lateral: Default::default(),
            subquery: Default::default(),
            alias: Default::default(),
            projection_names: Default::default(),
        }
    }
}
impl Default for DerivedRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
pub struct UnnestRelationBuilder {
    pub alias: Option<TableAlias<'static>>,
    pub array_exprs: Vec<Expr<'static>>,
    with_offset: bool,
    with_offset_alias: Option<Ident<'static>>,
    with_ordinality: bool,
}

impl UnnestRelationBuilder {
    pub fn alias(&mut self, value: Option<TableAlias<'static>>) -> &mut Self {
        self.alias = value;
        self
    }
    pub fn array_exprs(&mut self, value: Vec<Expr<'static>>) -> &mut Self {
        self.array_exprs = value;
        self
    }

    /// BigQuery's `WITH OFFSET`.
    pub fn with_offset(&mut self, value: bool) -> &mut Self {
        self.with_offset = value;
        self
    }

    pub fn with_offset_alias(&mut self, value: Option<Ident<'static>>) -> &mut Self {
        self.with_offset_alias = value;
        self
    }

    pub fn with_ordinality(&mut self, value: bool) -> &mut Self {
        self.with_ordinality = value;
        self
    }

    pub fn build(&self) -> Result<TableRef<'static>, BuilderError> {
        let call = ExprKind::Function {
            name: object_name(vec![ident("UNNEST")]),
            args: self
                .array_exprs
                .iter()
                .cloned()
                .map(FunctionArg::Expr)
                .collect(),
            options: None,
        };
        let unnest = table_ref(
            TableFactor::Function {
                call: Box::new(call.into()),
                with_ordinality: self.with_ordinality,
            },
            self.alias.clone(),
        );
        if !self.with_offset {
            return Ok(unnest);
        }
        Ok(table_ref(
            TableFactor::Ext(BuiltinTableFactor::BigQuery(
                BigQueryTableFactor::WithOffset(Box::new(BigQueryWithOffset {
                    table: unnest,
                    alias: self.with_offset_alias.clone(),
                })),
            )),
            None,
        ))
    }

    fn create_empty() -> Self {
        Self {
            alias: Default::default(),
            array_exprs: Default::default(),
            with_offset: Default::default(),
            with_offset_alias: Default::default(),
            with_ordinality: Default::default(),
        }
    }
}

impl Default for UnnestRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

/// Builds a `LATERAL FLATTEN(INPUT => expr, OUTER => bool)` table factor
/// for Snowflake-style unnesting.
#[derive(Clone)]
pub struct FlattenRelationBuilder {
    pub alias: Option<TableAlias<'static>>,
    /// The input expression to flatten (e.g. a column reference).
    pub input_expr: Option<Expr<'static>>,
    /// Whether to preserve rows for NULL/empty inputs (Snowflake `OUTER` param).
    pub outer: bool,
}

impl FlattenRelationBuilder {
    pub fn alias(&mut self, value: Option<TableAlias<'static>>) -> &mut Self {
        self.alias = value;
        self
    }

    pub fn input_expr(&mut self, value: Expr<'static>) -> &mut Self {
        self.input_expr = Some(value);
        self
    }

    pub fn outer(&mut self, value: bool) -> &mut Self {
        self.outer = value;
        self
    }

    pub fn build(&self) -> Result<TableRef<'static>, BuilderError> {
        let input = self.input_expr.clone().ok_or_else(|| {
            BuilderError::from(UninitializedFieldError::from("input_expr"))
        })?;

        let mut args = vec![FunctionArg::Named {
            name: ident("INPUT"),
            value: input,
        }];

        if self.outer {
            args.push(FunctionArg::Named {
                name: ident("OUTER"),
                value: ExprKind::Literal(Literal::Boolean(true)).into(),
            });
        }

        Ok(table_ref(
            TableFactor::Lateral(Box::new(TableFactor::Function {
                call: Box::new(function("FLATTEN", args)),
                with_ordinality: false,
            })),
            self.alias.clone(),
        ))
    }

    fn create_empty() -> Self {
        Self {
            alias: None,
            input_expr: None,
            outer: false,
        }
    }
}

impl Default for FlattenRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

/// Runtime error when a `build()` method is called and one or more required fields
/// do not have a value.
#[derive(Debug, Clone)]
pub struct UninitializedFieldError(&'static str);

impl UninitializedFieldError {
    /// Create a new `UninitializedFieldError` for the specified field name.
    pub fn new(field_name: &'static str) -> Self {
        UninitializedFieldError(field_name)
    }

    /// Get the name of the first-declared field that wasn't initialized
    pub fn field_name(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for UninitializedFieldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Field not initialized: {}", self.0)
    }
}

impl From<&'static str> for UninitializedFieldError {
    fn from(field_name: &'static str) -> Self {
        Self::new(field_name)
    }
}
impl std::error::Error for UninitializedFieldError {}

#[derive(Debug)]
pub enum BuilderError {
    UninitializedField(&'static str),
    ValidationError(String),
}
impl From<UninitializedFieldError> for BuilderError {
    fn from(s: UninitializedFieldError) -> Self {
        Self::UninitializedField(s.field_name())
    }
}
impl From<String> for BuilderError {
    fn from(s: String) -> Self {
        Self::ValidationError(s)
    }
}
impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UninitializedField(field) => {
                write!(f, "`{field}` must be initialized")
            }
            Self::ValidationError(error) => write!(f, "{error}"),
        }
    }
}
impl std::error::Error for BuilderError {}

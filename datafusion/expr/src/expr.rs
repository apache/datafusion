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

//! Logical Expressions: [`Expr`]

use std::collections::HashSet;
use std::fmt::{self, Display, Formatter, Write};
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use crate::expr_fn::binary_expr;
use crate::logical_plan::Subquery;
use crate::utils::expr_to_columns;
use crate::Volatility;
use crate::{udaf, ExprSchemable, Operator, Signature, WindowFrame, WindowUDF};

use crate::logical_plan::tree_node::{LogicalPlanPattern, LogicalPlanStats};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cse::HashNode;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};
use datafusion_common::{
    plan_err, Column, DFSchema, HashMap, Result, ScalarValue, TableReference,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use enumset::enum_set;
use sqlparser::ast::{
    display_comma_separated, ExceptSelectItem, ExcludeSelectItem, IlikeSelectItem,
    NullTreatment, RenameSelectItem, ReplaceSelectElement,
};

/// Represents logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
///
/// For example the expression `A + 1` will be represented as
///
///```text
///  BinaryExpr {
///    left: Expr::Column("A"),
///    op: Operator::Plus,
///    right: Expr::Literal(ScalarValue::Int32(Some(1)))
/// }
/// ```
///
/// # Creating Expressions
///
/// `Expr`s can be created directly, but it is often easier and less verbose to
/// use the fluent APIs in [`crate::expr_fn`] such as [`col`] and [`lit`], or
/// methods such as [`Expr::alias`], [`Expr::cast_to`], and [`Expr::Like`]).
///
/// See also [`ExprFunctionExt`] for creating aggregate and window functions.
///
/// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
///
/// # Schema Access
///
/// See [`ExprSchemable::get_type`] to access the [`DataType`] and nullability
/// of an `Expr`.
///
/// # Visiting and Rewriting `Expr`s
///
/// The `Expr` struct implements the [`TreeNode`] trait for walking and
/// rewriting expressions. For example [`TreeNode::apply`] recursively visits an
/// `Expr` and [`TreeNode::transform`] can be used to rewrite an expression. See
/// the examples below and [`TreeNode`] for more information.
///
/// # Examples
///
/// ## Column references and literals
///
/// [`Expr::Column`] refer to the values of columns and are often created with
/// the [`col`] function. For example to create an expression `c1` referring to
/// column named "c1":
///
/// [`col`]: crate::expr_fn::col
///
/// ```
/// # use datafusion_common::Column;
/// # use datafusion_expr::{lit, col, Expr};
/// let expr = col("c1");
/// assert_eq!(expr, Expr::column(Column::from_name("c1")));
/// ```
///
/// [`Expr::Literal`] refer to literal, or constant, values. These are created
/// with the [`lit`] function. For example to create an expression `42`:
///
/// [`lit`]: crate::lit
///
/// ```
/// # use datafusion_common::{Column, ScalarValue};
/// # use datafusion_expr::{lit, col, Expr};
/// // All literals are strongly typed in DataFusion. To make an `i64` 42:
/// let expr = lit(42i64);
/// assert_eq!(expr, Expr::literal(ScalarValue::Int64(Some(42))));
/// // To make a (typed) NULL:
/// let expr = Expr::literal(ScalarValue::Int64(None));
/// // to make an (untyped) NULL (the optimizer will coerce this to the correct type):
/// let expr = lit(ScalarValue::Null);
/// ```
///
/// ## Binary Expressions
///
/// Exprs implement traits that allow easy to understand construction of more
/// complex expressions. For example, to create `c1 + c2` to add columns "c1" and
/// "c2" together
///
/// ```
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// // Use the `+` operator to add two columns together
/// let expr = col("c1") + col("c2");
/// assert!(matches!(expr, Expr::BinaryExpr { ..} ));
/// if let Expr::BinaryExpr(binary_expr, _) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   assert_eq!(*binary_expr.right, col("c2"));
///   assert_eq!(binary_expr.op, Operator::Plus);
/// }
/// ```
///
/// The expression `c1 = 42` to compares the value in column "c1" to the
/// literal value `42`:
///
/// ```
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// let expr = col("c1").eq(lit(42_i32));
/// assert!(matches!(expr, Expr::BinaryExpr { .. } ));
/// if let Expr::BinaryExpr(binary_expr, _) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   let scalar = ScalarValue::Int32(Some(42));
///   assert_eq!(*binary_expr.right, Expr::literal(scalar));
///   assert_eq!(binary_expr.op, Operator::Eq);
/// }
/// ```
///
/// Here is how to implement the equivalent of `SELECT *` to select all
/// [`Expr::Column`] from a [`DFSchema`]'s columns:
///
/// ```
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::{DFSchema, Column};
/// # use datafusion_expr::Expr;
/// // Create a schema c1(int, c2 float)
/// let arrow_schema = Schema::new(vec![
///    Field::new("c1", DataType::Int32, false),
///    Field::new("c2", DataType::Float64, false),
/// ]);
/// // DFSchema is a an Arrow schema with optional relation name
/// let df_schema = DFSchema::try_from_qualified_schema("t1", &arrow_schema)
///   .unwrap();
///
/// // Form Vec<Expr> with an expression for each column in the schema
/// let exprs: Vec<_> = df_schema.iter()
///   .map(Expr::from)
///   .collect();
///
/// assert_eq!(exprs, vec![
///   Expr::from(Column::from_qualified_name("t1.c1")),
///   Expr::from(Column::from_qualified_name("t1.c2")),
/// ]);
/// ```
///
/// # Visiting and Rewriting `Expr`s
///
/// Here is an example that finds all literals in an `Expr` tree:
/// ```
/// # use std::collections::{HashSet};
/// use datafusion_common::ScalarValue;
/// # use datafusion_expr::{col, Expr, lit};
/// use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
/// // Expression a = 5 AND b = 6
/// let expr = col("a").eq(lit(5)) & col("b").eq(lit(6));
/// // find all literals in a HashMap
/// let mut scalars = HashSet::new();
/// // apply recursively visits all nodes in the expression tree
/// expr.apply(|e| {
///    if let Expr::Literal(scalar, _) = e {
///       scalars.insert(scalar);
///    }
///    // The return value controls whether to continue visiting the tree
///    Ok(TreeNodeRecursion::Continue)
/// }).unwrap();
/// // All subtrees have been visited and literals found
/// assert_eq!(scalars.len(), 2);
/// assert!(scalars.contains(&ScalarValue::Int32(Some(5))));
/// assert!(scalars.contains(&ScalarValue::Int32(Some(6))));
/// ```
///
/// Rewrite an expression, replacing references to column "a" in an
/// to the literal `42`:
///
///  ```
/// # use datafusion_common::tree_node::{Transformed, TreeNode};
/// # use datafusion_expr::{col, Expr, lit};
/// // expression a = 5 AND b = 6
/// let expr = col("a").eq(lit(5)).and(col("b").eq(lit(6)));
/// // rewrite all references to column "a" to the literal 42
/// let rewritten = expr.transform(|e| {
///   if let Expr::Column(c, _) = &e {
///     if &c.name == "a" {
///       // return Transformed::yes to indicate the node was changed
///       return Ok(Transformed::yes(lit(42)))
///     }
///   }
///   // return Transformed::no to indicate the node was not changed
///   Ok(Transformed::no(e))
/// }).unwrap();
/// // The expression has been rewritten
/// assert!(rewritten.transformed);
/// // to 42 = 5 AND b = 6
/// assert_eq!(rewritten.data, lit(42).eq(lit(5)).and(col("b").eq(lit(6))));
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Alias, LogicalPlanStats),
    /// A named reference to a qualified field in a schema.
    Column(Column, LogicalPlanStats),
    /// A named reference to a variable in a registry.
    ScalarVariable(DataType, Vec<String>, LogicalPlanStats),
    /// A constant value.
    Literal(ScalarValue, LogicalPlanStats),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr, LogicalPlanStats),
    /// LIKE expression
    Like(Like, LogicalPlanStats),
    /// LIKE expression that uses regular expressions
    SimilarTo(Like, LogicalPlanStats),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>, LogicalPlanStats),
    /// True if argument is not NULL, false otherwise. This expression itself is never NULL.
    IsNotNull(Box<Expr>, LogicalPlanStats),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>, LogicalPlanStats),
    /// True if argument is true, false otherwise. This expression itself is never NULL.
    IsTrue(Box<Expr>, LogicalPlanStats),
    /// True if argument is  false, false otherwise. This expression itself is never NULL.
    IsFalse(Box<Expr>, LogicalPlanStats),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsUnknown(Box<Expr>, LogicalPlanStats),
    /// True if argument is FALSE or NULL, false otherwise. This expression itself is never NULL.
    IsNotTrue(Box<Expr>, LogicalPlanStats),
    /// True if argument is TRUE OR NULL, false otherwise. This expression itself is never NULL.
    IsNotFalse(Box<Expr>, LogicalPlanStats),
    /// True if argument is TRUE or FALSE, false otherwise. This expression itself is never NULL.
    IsNotUnknown(Box<Expr>, LogicalPlanStats),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>, LogicalPlanStats),
    /// Whether an expression is between a given range.
    Between(Between, LogicalPlanStats),
    /// The CASE expression is similar to a series of nested if/else and there are two forms that
    /// can be used. The first form consists of a series of boolean "when" expressions with
    /// corresponding "then" expressions, and an optional "else" expression.
    ///
    /// ```text
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    /// ```
    ///
    /// The second form uses a base expression and then a series of "when" clauses that match on a
    /// literal value.
    ///
    /// ```text
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    /// ```
    Case(Case, LogicalPlanStats),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast, LogicalPlanStats),
    /// Casts the expression to a given type and will return a null value if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    TryCast(TryCast, LogicalPlanStats),
    /// Represents the call of a scalar function with a set of arguments.
    ScalarFunction(ScalarFunction, LogicalPlanStats),
    /// Calls an aggregate function with arguments, and optional
    /// `ORDER BY`, `FILTER`, `DISTINCT` and `NULL TREATMENT`.
    ///
    /// See also [`ExprFunctionExt`] to set these fields.
    ///
    /// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
    AggregateFunction(AggregateFunction, LogicalPlanStats),
    /// Represents the call of a window function with arguments.
    WindowFunction(WindowFunction, LogicalPlanStats),
    /// Returns whether the list contains the expr value.
    InList(InList, LogicalPlanStats),
    /// EXISTS subquery
    Exists(Exists, LogicalPlanStats),
    /// IN subquery
    InSubquery(InSubquery, LogicalPlanStats),
    /// Scalar subquery
    ScalarSubquery(Subquery, LogicalPlanStats),
    /// Represents a reference to all available fields in a specific schema,
    /// with an optional (schema) qualifier.
    ///
    /// This expr has to be resolved to a list of columns before translating logical
    /// plan into physical plan.
    Wildcard(Wildcard, LogicalPlanStats),
    /// List of grouping set expressions. Only valid in the context of an aggregate
    /// GROUP BY expression list
    GroupingSet(GroupingSet, LogicalPlanStats),
    /// A place holder for parameters in a prepared statement
    /// (e.g. `$foo` or `$1`)
    Placeholder(Placeholder, LogicalPlanStats),
    /// A place holder which hold a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterReferenceColumn(DataType, Column, LogicalPlanStats),
    /// Unnest expression
    Unnest(Unnest, LogicalPlanStats),
}

impl Default for Expr {
    fn default() -> Self {
        Expr::literal(ScalarValue::Null)
    }
}

/// Create an [`Expr`] from a [`Column`]
impl From<Column> for Expr {
    fn from(value: Column) -> Self {
        Expr::column(value)
    }
}

/// Create an [`Expr`] from an optional qualifier and a [`FieldRef`]. This is
/// useful for creating [`Expr`] from a [`DFSchema`].
///
/// See example on [`Expr`]
impl<'a> From<(Option<&'a TableReference>, &'a FieldRef)> for Expr {
    fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self {
        Expr::from(Column::from(value))
    }
}

impl<'a> TreeNodeContainer<'a, Self> for Expr {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

/// Wildcard expression.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Wildcard {
    pub qualifier: Option<TableReference>,
    pub options: WildcardOptions,
}

impl Wildcard {
    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.options.stats()
    }
}

/// UNNEST expression.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Unnest {
    pub expr: Box<Expr>,
}

impl Unnest {
    /// Create a new Unnest expression.
    pub fn new(expr: Expr) -> Self {
        Self {
            expr: Box::new(expr),
        }
    }

    /// Create a new Unnest expression.
    pub fn new_boxed(boxed: Box<Expr>) -> Self {
        Self { expr: boxed }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats()
    }
}

/// Alias expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub relation: Option<TableReference>,
    pub name: String,
}

impl Alias {
    /// Create an alias with an optional schema/field qualifier.
    pub fn new(
        expr: Expr,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            expr: Box::new(expr),
            relation: relation.map(|r| r.into()),
            name: name.into(),
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats()
    }
}

/// Binary expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    /// Create a new binary expression
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.left.stats().merge(self.right.stats())
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Put parentheses around child binary expressions so that we can see the difference
        // between `(a OR b) AND c` and `a OR (b AND c)`. We only insert parentheses when needed,
        // based on operator precedence. For example, `(a AND b) OR c` and `a AND b OR c` are
        // equivalent and the parentheses are not necessary.

        fn write_child(
            f: &mut Formatter<'_>,
            expr: &Expr,
            precedence: u8,
        ) -> fmt::Result {
            match expr {
                Expr::BinaryExpr(child, _) => {
                    let p = child.op.precedence();
                    if p == 0 || p < precedence {
                        write!(f, "({child})")?;
                    } else {
                        write!(f, "{child}")?;
                    }
                }
                _ => write!(f, "{expr}")?,
            }
            Ok(())
        }

        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

/// CASE expression
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Case {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub expr: Option<Box<Expr>>,
    /// One or more when/then expressions
    pub when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
    /// Optional "else" expression
    pub else_expr: Option<Box<Expr>>,
}

impl Case {
    /// Create a new Case expression
    pub fn new(
        expr: Option<Box<Expr>>,
        when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
        else_expr: Option<Box<Expr>>,
    ) -> Self {
        Self {
            expr,
            when_then_expr,
            else_expr,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr
            .iter()
            .chain(
                self.when_then_expr
                    .iter()
                    .flat_map(|(w, t)| vec![w, t])
                    .chain(self.else_expr.iter()),
            )
            .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats()))
    }
}

/// LIKE expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
    /// Whether to ignore case on comparing
    pub case_insensitive: bool,
}

impl Like {
    /// Create a new Like expression
    pub fn new(
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
        case_insensitive: bool,
    ) -> Self {
        Self {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats().merge(self.pattern.stats())
    }
}

/// BETWEEN expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Between {
    /// The value to compare
    pub expr: Box<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
    /// The low end of the range
    pub low: Box<Expr>,
    /// The high end of the range
    pub high: Box<Expr>,
}

impl Between {
    /// Create a new Between expression
    pub fn new(expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr>) -> Self {
        Self {
            expr,
            negated,
            low,
            high,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr
            .stats()
            .merge(self.low.stats())
            .merge(self.high.stats())
    }
}

/// ScalarFunction expression invokes a built-in scalar function
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct ScalarFunction {
    /// The function
    pub func: Arc<crate::ScalarUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
}

impl ScalarFunction {
    // return the Function's name
    pub fn name(&self) -> &str {
        self.func.name()
    }

    /// Create a new ScalarFunction expression with a user-defined function (UDF)
    pub fn new_udf(udf: Arc<crate::ScalarUDF>, args: Vec<Expr>) -> Self {
        Self { func: udf, args }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.args
            .iter()
            .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats()))
    }
}

/// Access a sub field of a nested type, such as `Field` or `List`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum GetFieldAccess {
    /// Named field, for example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// Single list index, for example: `list[i]`
    ListIndex { key: Box<Expr> },
    /// List stride, for example `list[i:j:k]`
    ListRange {
        start: Box<Expr>,
        stop: Box<Expr>,
        stride: Box<Expr>,
    },
}

/// Cast expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl Cast {
    /// Create a new Cast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats()
    }
}

/// TryCast Expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct TryCast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl TryCast {
    /// Create a new TryCast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats()
    }
}

/// SORT expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Sort {
    /// The expression to sort on
    pub expr: Expr,
    /// The direction of the sort
    pub asc: bool,
    /// Whether to put Nulls before all other data values
    pub nulls_first: bool,
}

impl Sort {
    /// Create a new Sort expression
    pub fn new(expr: Expr, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }

    /// Create a new Sort expression with the opposite sort direction
    pub fn reverse(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            asc: !self.asc,
            nulls_first: !self.nulls_first,
        }
    }

    /// Replaces the Sort expressions with `expr`
    pub fn with_expr(&self, expr: Expr) -> Self {
        Self {
            expr,
            asc: self.asc,
            nulls_first: self.nulls_first,
        }
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " ASC")?;
        } else {
            write!(f, " DESC")?;
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST")?;
        } else {
            write!(f, " NULLS LAST")?;
        }
        Ok(())
    }
}

impl<'a> TreeNodeContainer<'a, Expr> for Sort {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.expr.apply_elements(f)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.expr
            .map_elements(f)?
            .map_data(|expr| Ok(Self { expr, ..self }))
    }
}

/// Aggregate function
///
/// See also  [`ExprFunctionExt`] to set these fields on `Expr`
///
/// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct AggregateFunction {
    /// Name of the function
    pub func: Arc<crate::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ordering
    pub order_by: Option<Vec<Sort>>,
    pub null_treatment: Option<NullTreatment>,
}

impl AggregateFunction {
    /// Create a new AggregateFunction expression with a user-defined function (UDF)
    pub fn new_udf(
        func: Arc<crate::AggregateUDF>,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Sort>>,
        null_treatment: Option<NullTreatment>,
    ) -> Self {
        Self {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.args
            .iter()
            .chain(self.filter.iter().map(|e| e.as_ref()))
            .chain(self.order_by.iter().flatten().map(|s| &s.expr))
            .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats()))
    }
}

/// A function used as a SQL window function
///
/// In SQL, you can use:
/// - Actual window functions ([`WindowUDF`])
/// - Normal aggregate functions ([`AggregateUDF`])
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WindowFunctionDefinition {
    /// A user defined aggregate function
    AggregateUDF(Arc<crate::AggregateUDF>),
    /// A user defined aggregate function
    WindowUDF(Arc<WindowUDF>),
}

impl WindowFunctionDefinition {
    /// Returns the datatype of the window function
    pub fn return_type(
        &self,
        input_expr_types: &[DataType],
        _input_expr_nullable: &[bool],
        display_name: &str,
    ) -> Result<DataType> {
        match self {
            WindowFunctionDefinition::AggregateUDF(fun) => {
                fun.return_type(input_expr_types)
            }
            WindowFunctionDefinition::WindowUDF(fun) => fun
                .field(WindowUDFFieldArgs::new(input_expr_types, display_name))
                .map(|field| field.data_type().clone()),
        }
    }

    /// The signatures supported by the function `fun`.
    pub fn signature(&self) -> Signature {
        match self {
            WindowFunctionDefinition::AggregateUDF(fun) => fun.signature().clone(),
            WindowFunctionDefinition::WindowUDF(fun) => fun.signature().clone(),
        }
    }

    /// Function's name for display
    pub fn name(&self) -> &str {
        match self {
            WindowFunctionDefinition::WindowUDF(fun) => fun.name(),
            WindowFunctionDefinition::AggregateUDF(fun) => fun.name(),
        }
    }
}

impl Display for WindowFunctionDefinition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            WindowFunctionDefinition::AggregateUDF(fun) => Display::fmt(fun, f),
            WindowFunctionDefinition::WindowUDF(fun) => Display::fmt(fun, f),
        }
    }
}

impl From<Arc<crate::AggregateUDF>> for WindowFunctionDefinition {
    fn from(value: Arc<crate::AggregateUDF>) -> Self {
        Self::AggregateUDF(value)
    }
}

impl From<Arc<WindowUDF>> for WindowFunctionDefinition {
    fn from(value: Arc<WindowUDF>) -> Self {
        Self::WindowUDF(value)
    }
}

/// Window function
///
/// Holds the actual function to call [`WindowFunction`] as well as its
/// arguments (`args`) and the contents of the `OVER` clause:
///
/// 1. `PARTITION BY`
/// 2. `ORDER BY`
/// 3. Window frame (e.g. `ROWS 1 PRECEDING AND 1 FOLLOWING`)
///
/// See [`ExprFunctionExt`] for examples of how to create a `WindowFunction`.
///
/// [`ExprFunctionExt`]: crate::ExprFunctionExt
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct WindowFunction {
    /// Name of the function
    pub fun: WindowFunctionDefinition,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// List of partition by expressions
    pub partition_by: Vec<Expr>,
    /// List of order by expressions
    pub order_by: Vec<Sort>,
    /// Window frame
    pub window_frame: WindowFrame,
    /// Specifies how NULL value is treated: ignore or respect
    pub null_treatment: Option<NullTreatment>,
}

impl WindowFunction {
    /// Create a new Window expression with the specified argument an
    /// empty `OVER` clause
    pub fn new(fun: impl Into<WindowFunctionDefinition>, args: Vec<Expr>) -> Self {
        Self {
            fun: fun.into(),
            args,
            partition_by: Vec::default(),
            order_by: Vec::default(),
            window_frame: WindowFrame::new(None),
            null_treatment: None,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.args
            .iter()
            .chain(self.partition_by.iter())
            .chain(self.order_by.iter().map(|s| &s.expr))
            .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats()))
    }
}

/// EXISTS expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Exists {
    /// Subquery that will produce a single column of data
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl Exists {
    // Create a new Exists expression.
    pub fn new(subquery: Subquery, negated: bool) -> Self {
        Self { subquery, negated }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.subquery.stats()
    }
}

/// User Defined Aggregate Function
///
/// See [`udaf::AggregateUDF`] for more information.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateUDF {
    /// The function
    pub fun: Arc<udaf::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ORDER BY applied prior to aggregating
    pub order_by: Option<Vec<Expr>>,
}

impl AggregateUDF {
    /// Create a new AggregateUDF expression
    pub fn new(
        fun: Arc<udaf::AggregateUDF>,
        args: Vec<Expr>,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            fun,
            args,
            filter,
            order_by,
        }
    }
}

/// InList expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InList {
    /// Create a new InList expression
    pub fn new(expr: Box<Expr>, list: Vec<Expr>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.list
            .iter()
            .fold(self.expr.stats(), |s, e| s.merge(e.stats()))
    }
}

/// IN subquery
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct InSubquery {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// Subquery that will produce a single column of data to compare against
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InSubquery {
    /// Create a new InSubquery expression
    pub fn new(expr: Box<Expr>, subquery: Subquery, negated: bool) -> Self {
        Self {
            expr,
            subquery,
            negated,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.expr.stats().merge(self.subquery.stats())
    }
}

/// Placeholder, representing bind parameter values such as `$1` or `$name`.
///
/// The type of these parameters is inferred using [`Expr::infer_placeholder_types`]
/// or can be specified directly using `PREPARE` statements.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Placeholder {
    /// The identifier of the parameter, including the leading `$` (e.g, `"$1"` or `"$foo"`)
    pub id: String,
    /// The type the parameter will be filled in with
    pub data_type: Option<DataType>,
}

impl Placeholder {
    /// Create a new Placeholder expression
    pub fn new(id: String, data_type: Option<DataType>) -> Self {
        Self { id, data_type }
    }
}

/// Grouping sets
///
/// See <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>
/// for Postgres definition.
/// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
/// for Apache Spark definition.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum GroupingSet {
    /// Rollup grouping sets
    Rollup(Vec<Expr>),
    /// Cube grouping sets
    Cube(Vec<Expr>),
    /// User-defined grouping sets
    GroupingSets(Vec<Vec<Expr>>),
}

impl GroupingSet {
    /// Return all distinct exprs in the grouping set. For `CUBE` and `ROLLUP` this
    /// is just the underlying list of exprs. For `GROUPING SET` we need to deduplicate
    /// the exprs in the underlying sets.
    pub fn distinct_expr(&self) -> Vec<&Expr> {
        match self {
            GroupingSet::Rollup(exprs) | GroupingSet::Cube(exprs) => {
                exprs.iter().collect()
            }
            GroupingSet::GroupingSets(groups) => {
                let mut exprs: Vec<&Expr> = vec![];
                for exp in groups.iter().flatten() {
                    if !exprs.contains(&exp) {
                        exprs.push(exp);
                    }
                }
                exprs
            }
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        match self {
            GroupingSet::Rollup(exprs) | GroupingSet::Cube(exprs) => exprs
                .iter()
                .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats())),
            GroupingSet::GroupingSets(groups) => groups
                .iter()
                .flatten()
                .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats())),
        }
    }
}

/// Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug, Default)]
pub struct WildcardOptions {
    /// `[ILIKE...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub ilike: Option<IlikeSelectItem>,
    /// `[EXCLUDE...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub exclude: Option<ExcludeSelectItem>,
    /// `[EXCEPT...]`.
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_except>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#except>
    pub except: Option<ExceptSelectItem>,
    /// `[REPLACE]`
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#replace>
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub replace: Option<PlannedReplaceSelectItem>,
    /// `[RENAME ...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub rename: Option<RenameSelectItem>,
}

impl WildcardOptions {
    pub fn with_replace(self, replace: PlannedReplaceSelectItem) -> Self {
        WildcardOptions {
            ilike: self.ilike,
            exclude: self.exclude,
            except: self.except,
            replace: Some(replace),
            rename: self.rename,
        }
    }

    pub(crate) fn stats(&self) -> LogicalPlanStats {
        self.replace
            .iter()
            .flat_map(|prsi| prsi.planned_expressions.iter())
            .fold(LogicalPlanStats::empty(), |s, e| s.merge(e.stats()))
    }
}

impl Display for WildcardOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(ilike) = &self.ilike {
            write!(f, " {ilike}")?;
        }
        if let Some(exclude) = &self.exclude {
            write!(f, " {exclude}")?;
        }
        if let Some(except) = &self.except {
            write!(f, " {except}")?;
        }
        if let Some(replace) = &self.replace {
            write!(f, " {replace}")?;
        }
        if let Some(rename) = &self.rename {
            write!(f, " {rename}")?;
        }
        Ok(())
    }
}

/// The planned expressions for `REPLACE`
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug, Default)]
pub struct PlannedReplaceSelectItem {
    /// The original ast nodes
    pub items: Vec<ReplaceSelectElement>,
    /// The expression planned from the ast nodes. They will be used when expanding the wildcard.
    pub planned_expressions: Vec<Expr>,
}

impl Display for PlannedReplaceSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "REPLACE")?;
        write!(f, " ({})", display_comma_separated(&self.items))?;
        Ok(())
    }
}

impl PlannedReplaceSelectItem {
    pub fn items(&self) -> &[ReplaceSelectElement] {
        &self.items
    }

    pub fn expressions(&self) -> &[Expr] {
        &self.planned_expressions
    }
}

impl Expr {
    #[deprecated(since = "40.0.0", note = "use schema_name instead")]
    pub fn display_name(&self) -> Result<String> {
        Ok(self.schema_name().to_string())
    }

    /// The name of the column (field) that this `Expr` will produce.
    ///
    /// For example, for a projection (e.g. `SELECT <expr>`) the resulting arrow
    /// [`Schema`] will have a field with this name.
    ///
    /// Note that the resulting string is subtlety different from the `Display`
    /// representation for certain `Expr`. Some differences:
    ///
    /// 1. [`Expr::Alias`], which shows only the alias itself
    /// 2. [`Expr::Cast`] / [`Expr::TryCast`], which only displays the expression
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::{col, lit};
    /// let expr = col("foo").eq(lit(42));
    /// assert_eq!("foo = Int32(42)", expr.schema_name().to_string());
    ///
    /// let expr = col("foo").alias("bar").eq(lit(11));
    /// assert_eq!("bar = Int32(11)", expr.schema_name().to_string());
    /// ```
    ///
    /// [`Schema`]: arrow::datatypes::Schema
    pub fn schema_name(&self) -> impl Display + '_ {
        SchemaDisplay(self)
    }

    /// Returns the qualifier and the schema name of this expression.
    ///
    /// Used when the expression forms the output field of a certain plan.
    /// The result is the field's qualifier and field name in the plan's
    /// output schema. We can use this qualified name to reference the field.
    pub fn qualified_name(&self) -> (Option<TableReference>, String) {
        match self {
            Expr::Column(Column { relation, name }, _) => {
                (relation.clone(), name.clone())
            }
            Expr::Alias(Alias { relation, name, .. }, _) => {
                (relation.clone(), name.clone())
            }
            _ => (None, self.schema_name().to_string()),
        }
    }

    /// Returns a full and complete string representation of this expression.
    #[deprecated(since = "42.0.0", note = "use format! instead")]
    pub fn canonical_name(&self) -> String {
        format!("{self}")
    }

    /// Return String representation of the variant represented by `self`
    /// Useful for non-rust based bindings
    pub fn variant_name(&self) -> &str {
        match self {
            Expr::AggregateFunction { .. } => "AggregateFunction",
            Expr::Alias(..) => "Alias",
            Expr::Between { .. } => "Between",
            Expr::BinaryExpr { .. } => "BinaryExpr",
            Expr::Case { .. } => "Case",
            Expr::Cast { .. } => "Cast",
            Expr::Column(..) => "Column",
            Expr::OuterReferenceColumn(_, _, _) => "Outer",
            Expr::Exists { .. } => "Exists",
            Expr::GroupingSet(..) => "GroupingSet",
            Expr::InList { .. } => "InList",
            Expr::InSubquery(..) => "InSubquery",
            Expr::IsNotNull(..) => "IsNotNull",
            Expr::IsNull(..) => "IsNull",
            Expr::Like { .. } => "Like",
            Expr::SimilarTo { .. } => "RLike",
            Expr::IsTrue(..) => "IsTrue",
            Expr::IsFalse(..) => "IsFalse",
            Expr::IsUnknown(..) => "IsUnknown",
            Expr::IsNotTrue(..) => "IsNotTrue",
            Expr::IsNotFalse(..) => "IsNotFalse",
            Expr::IsNotUnknown(..) => "IsNotUnknown",
            Expr::Literal(..) => "Literal",
            Expr::Negative(..) => "Negative",
            Expr::Not(..) => "Not",
            Expr::Placeholder { .. } => "Placeholder",
            Expr::ScalarFunction(..) => "ScalarFunction",
            Expr::ScalarSubquery { .. } => "ScalarSubquery",
            Expr::ScalarVariable(..) => "ScalarVariable",
            Expr::TryCast { .. } => "TryCast",
            Expr::WindowFunction { .. } => "WindowFunction",
            Expr::Wildcard { .. } => "Wildcard",
            Expr::Unnest { .. } => "Unnest",
        }
    }

    /// Return `self == other`
    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Eq, other)
    }

    /// Return `self != other`
    pub fn not_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::NotEq, other)
    }

    /// Return `self > other`
    pub fn gt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Gt, other)
    }

    /// Return `self >= other`
    pub fn gt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GtEq, other)
    }

    /// Return `self < other`
    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Lt, other)
    }

    /// Return `self <= other`
    pub fn lt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LtEq, other)
    }

    /// Return `self && other`
    pub fn and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::And, other)
    }

    /// Return `self || other`
    pub fn or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Or, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        let like = Like::new(false, Box::new(self), Box::new(other), None, false);
        Expr::_like(like)
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        let like = Like::new(true, Box::new(self), Box::new(other), None, false);
        Expr::_like(like)
    }

    /// Return `self ILIKE other`
    pub fn ilike(self, other: Expr) -> Expr {
        let like = Like::new(false, Box::new(self), Box::new(other), None, true);
        Expr::_like(like)
    }

    /// Return `self NOT ILIKE other`
    pub fn not_ilike(self, other: Expr) -> Expr {
        Expr::_like(Like::new(true, Box::new(self), Box::new(other), None, true))
    }

    /// Return the name to use for the specific Expr
    pub fn name_for_alias(&self) -> Result<String> {
        Ok(self.schema_name().to_string())
    }

    /// Ensure `expr` has the name as `original_name` by adding an
    /// alias if necessary.
    pub fn alias_if_changed(self, original_name: String) -> Result<Expr> {
        let new_name = self.name_for_alias()?;
        if new_name == original_name {
            return Ok(self);
        }

        Ok(self.alias(original_name))
    }

    /// Return `self AS name` alias expression
    pub fn alias(self, name: impl Into<String>) -> Expr {
        let alias = Alias::new(self, None::<&str>, name.into());
        let stats = alias.stats();
        Expr::Alias(alias, stats)
    }

    /// Return `self AS name` alias expression with a specific qualifier
    pub fn alias_qualified(
        self,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Expr {
        let alias = Alias::new(self, relation, name.into());
        let stats = alias.stats();
        Expr::Alias(alias, stats)
    }

    /// Remove an alias from an expression if one exists.
    ///
    /// If the expression is not an alias, the expression is returned unchanged.
    /// This method does not remove aliases from nested expressions.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// // `foo as "bar"` is unaliased to `foo`
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.unalias(), col("foo"));
    ///
    /// // `foo as "bar" + baz` is not unaliased
    /// let expr = col("foo").alias("bar") + col("baz");
    /// assert_eq!(expr.clone().unalias(), expr);
    ///
    /// // `foo as "bar" as "baz" is unalaised to foo as "bar"
    /// let expr = col("foo").alias("bar").alias("baz");
    /// assert_eq!(expr.unalias(), col("foo").alias("bar"));
    /// ```
    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(alias, _) => *alias.expr,
            _ => self,
        }
    }

    /// Recursively removed potentially multiple aliases from an expression.
    ///
    /// This method removes nested aliases and returns [`Transformed`]
    /// to signal if the expression was changed.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// // `foo as "bar"` is unaliased to `foo`
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.unalias_nested().data, col("foo"));
    ///
    /// // `foo as "bar" + baz` is  unaliased
    /// let expr = col("foo").alias("bar") + col("baz");
    /// assert_eq!(expr.clone().unalias_nested().data, col("foo") + col("baz"));
    ///
    /// // `foo as "bar" as "baz" is unalaised to foo
    /// let expr = col("foo").alias("bar").alias("baz");
    /// assert_eq!(expr.unalias_nested().data, col("foo"));
    /// ```
    pub fn unalias_nested(self) -> Transformed<Expr> {
        self.transform_down_up(
            |expr| {
                // f_down: skip subqueries.  Check in f_down to avoid recursing into them
                let recursion = if matches!(
                    expr,
                    Expr::Exists { .. }
                        | Expr::ScalarSubquery { .. }
                        | Expr::InSubquery { .. }
                ) {
                    // Subqueries could contain aliases so don't recurse into those
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                };
                Ok(Transformed::new(expr, false, recursion))
            },
            |expr| {
                // f_up: unalias on up so we can remove nested aliases like
                // `(x as foo) as bar`
                if let Expr::Alias(Alias { expr, .. }, _) = expr {
                    Ok(Transformed::yes(*expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            },
        )
        // Unreachable code: internal closure doesn't return err
        .unwrap()
    }

    /// Return `self IN <list>` if `negated` is false, otherwise
    /// return `self NOT IN <list>`.a
    pub fn in_list(self, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::_in_list(InList::new(Box::new(self), list, negated))
    }

    /// Return `IsNull(Box(self))
    pub fn is_null(self) -> Expr {
        Expr::_is_null(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    pub fn is_not_null(self) -> Expr {
        Expr::_is_not_null(Box::new(self))
    }

    /// Create a sort configuration from an existing expression.
    ///
    /// ```
    /// # use datafusion_expr::col;
    /// let sort_expr = col("foo").sort(true, true); // SORT ASC NULLS_FIRST
    /// ```
    pub fn sort(self, asc: bool, nulls_first: bool) -> Sort {
        Sort::new(self, asc, nulls_first)
    }

    /// Return `IsTrue(Box(self))`
    pub fn is_true(self) -> Expr {
        Expr::_is_true(Box::new(self))
    }

    /// Return `IsNotTrue(Box(self))`
    pub fn is_not_true(self) -> Expr {
        Expr::_is_not_true(Box::new(self))
    }

    /// Return `IsFalse(Box(self))`
    pub fn is_false(self) -> Expr {
        Expr::_is_false(Box::new(self))
    }

    /// Return `IsNotFalse(Box(self))`
    pub fn is_not_false(self) -> Expr {
        Expr::_is_not_false(Box::new(self))
    }

    /// Return `IsUnknown(Box(self))`
    pub fn is_unknown(self) -> Expr {
        Expr::_is_unknown(Box::new(self))
    }

    /// Return `IsNotUnknown(Box(self))`
    pub fn is_not_unknown(self) -> Expr {
        Expr::_is_not_unknown(Box::new(self))
    }

    /// return `self BETWEEN low AND high`
    pub fn between(self, low: Expr, high: Expr) -> Expr {
        Expr::_between(Between::new(
            Box::new(self),
            false,
            Box::new(low),
            Box::new(high),
        ))
    }

    /// Return `self NOT BETWEEN low AND high`
    pub fn not_between(self, low: Expr, high: Expr) -> Expr {
        Expr::_between(Between::new(
            Box::new(self),
            true,
            Box::new(low),
            Box::new(high),
        ))
    }

    #[deprecated(since = "39.0.0", note = "use try_as_col instead")]
    pub fn try_into_col(&self) -> Result<Column> {
        match self {
            Expr::Column(it, _) => Ok(it.clone()),
            _ => plan_err!("Could not coerce '{self}' into Column!"),
        }
    }

    /// Return a reference to the inner `Column` if any
    ///
    /// returns `None` if the expression is not a `Column`
    ///
    /// Note: None may be returned for expressions that are not `Column` but
    /// are convertible to `Column` such as `Cast` expressions.
    ///
    /// Example
    /// ```
    /// # use datafusion_common::Column;
    /// use datafusion_expr::{col, Expr};
    /// let expr = col("foo");
    /// assert_eq!(expr.try_as_col(), Some(&Column::from("foo")));
    ///
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.try_as_col(), None);
    /// ```
    pub fn try_as_col(&self) -> Option<&Column> {
        if let Expr::Column(it, _) = self {
            Some(it)
        } else {
            None
        }
    }

    /// Returns the inner `Column` if any. This is a specialized version of
    /// [`Self::try_as_col`] that take Cast expressions into account when the
    /// expression is as on condition for joins.
    ///
    /// Called this method when you are sure that the expression is a `Column`
    /// or a `Cast` expression that wraps a `Column`.
    pub fn get_as_join_column(&self) -> Option<&Column> {
        match self {
            Expr::Column(c, _) => Some(c),
            Expr::Cast(Cast { expr, .. }, _) => match &**expr {
                Expr::Column(c, _) => Some(c),
                _ => None,
            },
            _ => None,
        }
    }

    /// Return all referenced columns of this expression.
    #[deprecated(since = "40.0.0", note = "use Expr::column_refs instead")]
    pub fn to_columns(&self) -> Result<HashSet<Column>> {
        let mut using_columns = HashSet::new();
        expr_to_columns(self, &mut using_columns)?;

        Ok(using_columns)
    }

    /// Return all references to columns in this expression.
    ///
    /// # Example
    /// ```
    /// # use std::collections::HashSet;
    /// # use datafusion_common::Column;
    /// # use datafusion_expr::col;
    /// // For an expression `a + (b * a)`
    /// let expr = col("a") + (col("b") * col("a"));
    /// let refs = expr.column_refs();
    /// // refs contains "a" and "b"
    /// assert_eq!(refs.len(), 2);
    /// assert!(refs.contains(&Column::new_unqualified("a")));
    /// assert!(refs.contains(&Column::new_unqualified("b")));
    /// ```
    pub fn column_refs(&self) -> HashSet<&Column> {
        let mut using_columns = HashSet::new();
        self.add_column_refs(&mut using_columns);
        using_columns
    }

    /// Adds references to all columns in this expression to the set
    ///
    /// See [`Self::column_refs`] for details
    pub fn add_column_refs<'a>(&'a self, set: &mut HashSet<&'a Column>) {
        self.apply(|expr| {
            if let Expr::Column(col, _) = expr {
                set.insert(col);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
    }

    /// Return all references to columns and their occurrence counts in the expression.
    ///
    /// # Example
    /// ```
    /// # use std::collections::HashMap;
    /// # use datafusion_common::Column;
    /// # use datafusion_expr::col;
    /// // For an expression `a + (b * a)`
    /// let expr = col("a") + (col("b") * col("a"));
    /// let mut refs = expr.column_refs_counts();
    /// // refs contains "a" and "b"
    /// assert_eq!(refs.len(), 2);
    /// assert_eq!(*refs.get(&Column::new_unqualified("a")).unwrap(), 2);
    /// assert_eq!(*refs.get(&Column::new_unqualified("b")).unwrap(), 1);
    /// ```
    pub fn column_refs_counts(&self) -> HashMap<&Column, usize> {
        let mut map = HashMap::new();
        self.add_column_ref_counts(&mut map);
        map
    }

    /// Adds references to all columns and their occurrence counts in the expression to
    /// the map.
    ///
    /// See [`Self::column_refs_counts`] for details
    pub fn add_column_ref_counts<'a>(&'a self, map: &mut HashMap<&'a Column, usize>) {
        self.apply(|expr| {
            if let Expr::Column(col, _) = expr {
                *map.entry(col).or_default() += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
    }

    /// Returns true if there are any column references in this Expr
    pub fn any_column_refs(&self) -> bool {
        self.exists(|expr| Ok(matches!(expr, Expr::Column(_, _))))
            .expect("exists closure is infallible")
    }

    /// Return true if the expression contains out reference(correlated) expressions.
    pub fn contains_outer(&self) -> bool {
        self.exists(|expr| Ok(matches!(expr, Expr::OuterReferenceColumn { .. })))
            .expect("exists closure is infallible")
    }

    /// Returns true if the expression node is volatile, i.e. whether it can return
    /// different results when evaluated multiple times with the same input.
    /// Note: unlike [`Self::is_volatile`], this function does not consider inputs:
    /// - `rand()` returns `true`,
    /// - `a + rand()` returns `false`
    pub fn is_volatile_node(&self) -> bool {
        matches!(self, Expr::ScalarFunction(func, _) if func.func.signature().volatility == Volatility::Volatile)
    }

    /// Returns true if the expression is volatile, i.e. whether it can return different
    /// results when evaluated multiple times with the same input.
    ///
    /// For example the function call `RANDOM()` is volatile as each call will
    /// return a different value.
    ///
    /// See [`Volatility`] for more information.
    pub fn is_volatile(&self) -> bool {
        self.exists(|expr| Ok(expr.is_volatile_node()))
            .expect("exists closure is infallible")
    }

    /// Recursively find all [`Expr::Placeholder`] expressions, and
    /// to infer their [`DataType`] from the context of their use.
    ///
    /// For example, gicen an expression like `<int32> = $0` will infer `$0` to
    /// have type `int32`.
    ///
    /// Returns transformed expression and flag that is true if expression contains
    /// at least one placeholder.
    pub fn infer_placeholder_types(self, schema: &DFSchema) -> Result<(Expr, bool)> {
        let mut has_placeholder = false;
        self.transform(|mut expr| {
            // Default to assuming the arguments are the same type
            if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }, _) = &mut expr {
                rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
                rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
            };
            if let Expr::Between(
                Between {
                    expr,
                    negated: _,
                    low,
                    high,
                },
                _,
            ) = &mut expr
            {
                rewrite_placeholder(low.as_mut(), expr.as_ref(), schema)?;
                rewrite_placeholder(high.as_mut(), expr.as_ref(), schema)?;
            }
            if let Expr::Placeholder(_, _) = &expr {
                has_placeholder = true;
            }
            Ok(Transformed::yes(expr))
        })
        .data()
        .map(|data| (data, has_placeholder))
    }

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered
    pub fn short_circuits(&self) -> bool {
        match self {
            Expr::ScalarFunction(ScalarFunction { func, .. }, _) => func.short_circuits(),
            Expr::BinaryExpr(BinaryExpr { op, .. }, _) => {
                matches!(op, Operator::And | Operator::Or)
            }
            Expr::Case { .. } => true,
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            Expr::AggregateFunction(..)
            | Expr::Alias(..)
            | Expr::Between(..)
            | Expr::Cast(..)
            | Expr::Column(..)
            | Expr::Exists(..)
            | Expr::GroupingSet(..)
            | Expr::InList(..)
            | Expr::InSubquery(..)
            | Expr::IsFalse(..)
            | Expr::IsNotFalse(..)
            | Expr::IsNotNull(..)
            | Expr::IsNotTrue(..)
            | Expr::IsNotUnknown(..)
            | Expr::IsNull(..)
            | Expr::IsTrue(..)
            | Expr::IsUnknown(..)
            | Expr::Like(..)
            | Expr::ScalarSubquery(..)
            | Expr::ScalarVariable(_, _, _)
            | Expr::SimilarTo(..)
            | Expr::Not(..)
            | Expr::Negative(..)
            | Expr::OuterReferenceColumn(_, _, _)
            | Expr::TryCast(..)
            | Expr::Unnest(..)
            | Expr::Wildcard { .. }
            | Expr::WindowFunction(..)
            | Expr::Literal(..)
            | Expr::Placeholder(..) => false,
        }
    }

    pub fn wildcard(wildcard: Wildcard) -> Self {
        let stats = wildcard.stats();
        Expr::Wildcard(wildcard, stats)
    }

    pub fn binary_expr(binary_expr: BinaryExpr) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprBinaryExpr))
            .merge(binary_expr.stats());
        Expr::BinaryExpr(binary_expr, stats)
    }

    pub fn similar_to(like: Like) -> Self {
        let stats = like.stats();
        Expr::SimilarTo(like, stats)
    }

    pub fn _like(like: Like) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprLike))
            .merge(like.stats());
        Expr::Like(like, stats)
    }

    pub fn unnest(unnest: Unnest) -> Self {
        let stats = unnest.stats();
        Expr::Unnest(unnest, stats)
    }

    pub fn in_subquery(in_subquery: InSubquery) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprInSubquery))
            .merge(in_subquery.stats());
        Expr::InSubquery(in_subquery, stats)
    }

    pub fn scalar_subquery(subquery: Subquery) -> Self {
        let stats =
            LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprScalarSubquery))
                .merge(subquery.stats());
        Expr::ScalarSubquery(subquery, stats)
    }

    pub fn _not(expr: Box<Expr>) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprNot))
            .merge(expr.stats());
        Expr::Not(expr, stats)
    }

    pub fn _is_not_null(expr: Box<Expr>) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprIsNotNull))
            .merge(expr.stats());
        Expr::IsNotNull(expr, stats)
    }

    pub fn _is_null(expr: Box<Expr>) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprIsNull))
            .merge(expr.stats());
        Expr::IsNull(expr, stats)
    }

    pub fn _is_true(expr: Box<Expr>) -> Self {
        let stats = expr.stats();
        Expr::IsTrue(expr, stats)
    }

    pub fn _is_false(expr: Box<Expr>) -> Self {
        let stats = expr.stats();
        Expr::IsFalse(expr, stats)
    }

    pub fn _is_unknown(expr: Box<Expr>) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprIsUnknown))
            .merge(expr.stats());
        Expr::IsUnknown(expr, stats)
    }

    pub fn _is_not_true(expr: Box<Expr>) -> Self {
        let stats = expr.stats();
        Expr::IsNotTrue(expr, stats)
    }

    pub fn _is_not_false(expr: Box<Expr>) -> Self {
        let stats = expr.stats();
        Expr::IsNotFalse(expr, stats)
    }

    pub fn _is_not_unknown(expr: Box<Expr>) -> Self {
        let stats =
            LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprIsNotUnknown))
                .merge(expr.stats());
        Expr::IsNotUnknown(expr, stats)
    }

    pub fn negative(expr: Box<Expr>) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprNegative))
            .merge(expr.stats());
        Expr::Negative(expr, stats)
    }

    pub fn _between(between: Between) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprBetween))
            .merge(between.stats());
        Expr::Between(between, stats)
    }

    pub fn case(case: Case) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprCase))
            .merge(case.stats());
        Expr::Case(case, stats)
    }

    pub fn cast(cast: Cast) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprCast))
            .merge(cast.stats());
        Expr::Cast(cast, stats)
    }

    pub fn try_cast(try_cast: TryCast) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprTryCast))
            .merge(try_cast.stats());
        Expr::TryCast(try_cast, stats)
    }

    pub fn scalar_function(scalar_function: ScalarFunction) -> Self {
        let stats =
            LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprScalarFunction))
                .merge(scalar_function.stats());
        Expr::ScalarFunction(scalar_function, stats)
    }

    pub fn window_function(window_function: WindowFunction) -> Self {
        let stats =
            LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprWindowFunction))
                .merge(window_function.stats());
        Expr::WindowFunction(window_function, stats)
    }

    pub fn aggregate_function(aggregate_function: AggregateFunction) -> Self {
        let stats =
            LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprAggregateFunction))
                .merge(aggregate_function.stats());
        Expr::AggregateFunction(aggregate_function, stats)
    }

    pub fn grouping_set(grouping_set: GroupingSet) -> Self {
        let stats = grouping_set.stats();
        Expr::GroupingSet(grouping_set, stats)
    }

    pub fn _in_list(in_list: InList) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprInList))
            .merge(in_list.stats());
        Expr::InList(in_list, stats)
    }

    pub fn exists(exists: Exists) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprExists))
            .merge(exists.stats());
        Expr::Exists(exists, stats)
    }

    pub fn literal(scalar_value: ScalarValue) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprLiteral));
        Expr::Literal(scalar_value, stats)
    }

    pub fn column(column: Column) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprColumn));
        Expr::Column(column, stats)
    }

    pub fn outer_reference_column(data_type: DataType, column: Column) -> Self {
        let stats = LogicalPlanStats::empty();
        Expr::OuterReferenceColumn(data_type, column, stats)
    }

    pub fn placeholder(placeholder: Placeholder) -> Self {
        let stats = LogicalPlanStats::new(enum_set!(LogicalPlanPattern::ExprPlaceholder));
        Expr::Placeholder(placeholder, stats)
    }

    pub fn scalar_variable(data_type: DataType, names: Vec<String>) -> Self {
        let stats = LogicalPlanStats::empty();
        Expr::ScalarVariable(data_type, names, stats)
    }
}

impl HashNode for Expr {
    /// As it is pretty easy to forget changing this method when `Expr` changes the
    /// implementation doesn't use wildcard patterns (`..`, `_`) to catch changes
    /// compile time.
    fn hash_node<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            Expr::Alias(
                Alias {
                    expr: _,
                    relation,
                    name,
                },
                _,
            ) => {
                relation.hash(state);
                name.hash(state);
            }
            Expr::Column(column, _) => {
                column.hash(state);
            }
            Expr::ScalarVariable(data_type, name, _) => {
                data_type.hash(state);
                name.hash(state);
            }
            Expr::Literal(scalar_value, _) => {
                scalar_value.hash(state);
            }
            Expr::BinaryExpr(
                BinaryExpr {
                    left: _,
                    op,
                    right: _,
                },
                _,
            ) => {
                op.hash(state);
            }
            Expr::Like(
                Like {
                    negated,
                    expr: _,
                    pattern: _,
                    escape_char,
                    case_insensitive,
                },
                _,
            )
            | Expr::SimilarTo(
                Like {
                    negated,
                    expr: _,
                    pattern: _,
                    escape_char,
                    case_insensitive,
                },
                _,
            ) => {
                negated.hash(state);
                escape_char.hash(state);
                case_insensitive.hash(state);
            }
            Expr::Not(_, _)
            | Expr::IsNotNull(_, _)
            | Expr::IsNull(_, _)
            | Expr::IsTrue(_, _)
            | Expr::IsFalse(_, _)
            | Expr::IsUnknown(_, _)
            | Expr::IsNotTrue(_, _)
            | Expr::IsNotFalse(_, _)
            | Expr::IsNotUnknown(_, _)
            | Expr::Negative(_, _) => {}
            Expr::Between(
                Between {
                    expr: _,
                    negated,
                    low: _,
                    high: _,
                },
                _,
            ) => {
                negated.hash(state);
            }
            Expr::Case(
                Case {
                    expr: _,
                    when_then_expr: _,
                    else_expr: _,
                },
                _,
            ) => {}
            Expr::Cast(Cast { expr: _, data_type }, _)
            | Expr::TryCast(TryCast { expr: _, data_type }, _) => {
                data_type.hash(state);
            }
            Expr::ScalarFunction(ScalarFunction { func, args: _ }, _) => {
                func.hash(state);
            }
            Expr::AggregateFunction(
                AggregateFunction {
                    func,
                    args: _,
                    distinct,
                    filter: _,
                    order_by: _,
                    null_treatment,
                },
                _,
            ) => {
                func.hash(state);
                distinct.hash(state);
                null_treatment.hash(state);
            }
            Expr::WindowFunction(
                WindowFunction {
                    fun,
                    args: _,
                    partition_by: _,
                    order_by: _,
                    window_frame,
                    null_treatment,
                },
                _,
            ) => {
                fun.hash(state);
                window_frame.hash(state);
                null_treatment.hash(state);
            }
            Expr::InList(
                InList {
                    expr: _,
                    list: _,
                    negated,
                },
                _,
            ) => {
                negated.hash(state);
            }
            Expr::Exists(Exists { subquery, negated }, _) => {
                subquery.hash(state);
                negated.hash(state);
            }
            Expr::InSubquery(
                InSubquery {
                    expr: _,
                    subquery,
                    negated,
                },
                _,
            ) => {
                subquery.hash(state);
                negated.hash(state);
            }
            Expr::ScalarSubquery(subquery, _) => {
                subquery.hash(state);
            }
            Expr::Wildcard(wildcard, _) => {
                wildcard.hash(state);
                wildcard.hash(state);
            }
            Expr::GroupingSet(grouping_set, _) => {
                mem::discriminant(grouping_set).hash(state);
                match grouping_set {
                    GroupingSet::Rollup(_) | GroupingSet::Cube(_) => {}
                    GroupingSet::GroupingSets(_) => {}
                }
            }
            Expr::Placeholder(place_holder, _) => {
                place_holder.hash(state);
            }
            Expr::OuterReferenceColumn(data_type, column, _) => {
                data_type.hash(state);
                column.hash(state);
            }
            Expr::Unnest(Unnest { expr: _ }, _) => {}
        };
    }
}

// Modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }, _) = expr {
        if data_type.is_none() {
            let other_dt = other.get_type(schema);
            match other_dt {
                Err(e) => {
                    Err(e.context(format!(
                        "Can not find type of {other} needed to infer type of {expr}"
                    )))?;
                }
                Ok(dt) => {
                    *data_type = Some(dt);
                }
            }
        };
    }
    Ok(())
}

#[macro_export]
macro_rules! expr_vec_fmt {
    ( $ARRAY:expr ) => {{
        $ARRAY
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ")
    }};
}

struct SchemaDisplay<'a>(&'a Expr);
impl Display for SchemaDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            // The same as Display
            Expr::Column(_, _)
            | Expr::Literal(_, _)
            | Expr::ScalarVariable(..)
            | Expr::OuterReferenceColumn(..)
            | Expr::Placeholder(_, _)
            | Expr::Wildcard { .. } => write!(f, "{}", self.0),

            Expr::AggregateFunction(
                AggregateFunction {
                    func,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
                _,
            ) => {
                write!(
                    f,
                    "{}({}{})",
                    func.name(),
                    if *distinct { "DISTINCT " } else { "" },
                    schema_name_from_exprs_comma_seperated_without_space(args)?
                )?;

                if let Some(null_treatment) = null_treatment {
                    write!(f, " {}", null_treatment)?;
                }

                if let Some(filter) = filter {
                    write!(f, " FILTER (WHERE {filter})")?;
                };

                if let Some(order_by) = order_by {
                    write!(f, " ORDER BY [{}]", schema_name_from_sorts(order_by)?)?;
                };

                Ok(())
            }
            // Expr is not shown since it is aliased
            Expr::Alias(Alias { name, .. }, _) => write!(f, "{name}"),
            Expr::Between(
                Between {
                    expr,
                    negated,
                    low,
                    high,
                },
                _,
            ) => {
                if *negated {
                    write!(
                        f,
                        "{} NOT BETWEEN {} AND {}",
                        SchemaDisplay(expr),
                        SchemaDisplay(low),
                        SchemaDisplay(high),
                    )
                } else {
                    write!(
                        f,
                        "{} BETWEEN {} AND {}",
                        SchemaDisplay(expr),
                        SchemaDisplay(low),
                        SchemaDisplay(high),
                    )
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }, _) => {
                write!(f, "{} {op} {}", SchemaDisplay(left), SchemaDisplay(right),)
            }
            Expr::Case(
                Case {
                    expr,
                    when_then_expr,
                    else_expr,
                },
                _,
            ) => {
                write!(f, "CASE ")?;

                if let Some(e) = expr {
                    write!(f, "{} ", SchemaDisplay(e))?;
                }

                for (when, then) in when_then_expr {
                    write!(
                        f,
                        "WHEN {} THEN {} ",
                        SchemaDisplay(when),
                        SchemaDisplay(then),
                    )?;
                }

                if let Some(e) = else_expr {
                    write!(f, "ELSE {} ", SchemaDisplay(e))?;
                }

                write!(f, "END")
            }
            // Cast expr is not shown to be consistant with Postgres and Spark <https://github.com/apache/datafusion/pull/3222>
            Expr::Cast(Cast { expr, .. }, _) | Expr::TryCast(TryCast { expr, .. }, _) => {
                write!(f, "{}", SchemaDisplay(expr))
            }
            Expr::InList(
                InList {
                    expr,
                    list,
                    negated,
                },
                _,
            ) => {
                let inlist_name = schema_name_from_exprs(list)?;

                if *negated {
                    write!(f, "{} NOT IN {}", SchemaDisplay(expr), inlist_name)
                } else {
                    write!(f, "{} IN {}", SchemaDisplay(expr), inlist_name)
                }
            }
            Expr::Exists(Exists { negated: true, .. }, _) => write!(f, "NOT EXISTS"),
            Expr::Exists(Exists { negated: false, .. }, _) => write!(f, "EXISTS"),
            Expr::GroupingSet(GroupingSet::Cube(exprs), _) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs), _) => {
                write!(f, "GROUPING SETS (")?;
                for exprs in lists_of_exprs.iter() {
                    write!(f, "({})", schema_name_from_exprs(exprs)?)?;
                }
                write!(f, ")")
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs), _) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::IsNull(expr, _) => write!(f, "{} IS NULL", SchemaDisplay(expr)),
            Expr::IsNotNull(expr, _) => {
                write!(f, "{} IS NOT NULL", SchemaDisplay(expr))
            }
            Expr::IsUnknown(expr, _) => {
                write!(f, "{} IS UNKNOWN", SchemaDisplay(expr))
            }
            Expr::IsNotUnknown(expr, _) => {
                write!(f, "{} IS NOT UNKNOWN", SchemaDisplay(expr))
            }
            Expr::InSubquery(InSubquery { negated: true, .. }, _) => {
                write!(f, "NOT IN")
            }
            Expr::InSubquery(InSubquery { negated: false, .. }, _) => write!(f, "IN"),
            Expr::IsTrue(expr, _) => write!(f, "{} IS TRUE", SchemaDisplay(expr)),
            Expr::IsFalse(expr, _) => write!(f, "{} IS FALSE", SchemaDisplay(expr)),
            Expr::IsNotTrue(expr, _) => {
                write!(f, "{} IS NOT TRUE", SchemaDisplay(expr))
            }
            Expr::IsNotFalse(expr, _) => {
                write!(f, "{} IS NOT FALSE", SchemaDisplay(expr))
            }
            Expr::Like(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                },
                _,
            ) => {
                write!(
                    f,
                    "{} {}{} {}",
                    SchemaDisplay(expr),
                    if *negated { "NOT " } else { "" },
                    if *case_insensitive { "ILIKE" } else { "LIKE" },
                    SchemaDisplay(pattern),
                )?;

                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::Negative(expr, _) => write!(f, "(- {})", SchemaDisplay(expr)),
            Expr::Not(expr, _) => write!(f, "NOT {}", SchemaDisplay(expr)),
            Expr::Unnest(Unnest { expr }, _) => {
                write!(f, "UNNEST({})", SchemaDisplay(expr))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }, _) => {
                match func.schema_name(args) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from schema_name {}", e)
                    }
                }
            }
            Expr::ScalarSubquery(Subquery { subquery, .. }, _) => {
                write!(f, "{}", subquery.schema().field(0).name())
            }
            Expr::SimilarTo(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    ..
                },
                _,
            ) => {
                write!(
                    f,
                    "{} {} {}",
                    SchemaDisplay(expr),
                    if *negated {
                        "NOT SIMILAR TO"
                    } else {
                        "SIMILAR TO"
                    },
                    SchemaDisplay(pattern),
                )?;
                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::WindowFunction(
                WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
                _,
            ) => {
                write!(
                    f,
                    "{}({})",
                    fun,
                    schema_name_from_exprs_comma_seperated_without_space(args)?
                )?;

                if let Some(null_treatment) = null_treatment {
                    write!(f, " {}", null_treatment)?;
                }

                if !partition_by.is_empty() {
                    write!(
                        f,
                        " PARTITION BY [{}]",
                        schema_name_from_exprs(partition_by)?
                    )?;
                }

                if !order_by.is_empty() {
                    write!(f, " ORDER BY [{}]", schema_name_from_sorts(order_by)?)?;
                };

                write!(f, " {window_frame}")
            }
        }
    }
}

/// Get schema_name for Vector of expressions
///
/// Internal usage. Please call `schema_name_from_exprs` instead
// TODO: Use ", " to standardize the formatting of Vec<Expr>,
// <https://github.com/apache/datafusion/issues/10364>
pub(crate) fn schema_name_from_exprs_comma_seperated_without_space(
    exprs: &[Expr],
) -> Result<String, fmt::Error> {
    schema_name_from_exprs_inner(exprs, ",")
}

/// Get schema_name for Vector of expressions
pub fn schema_name_from_exprs(exprs: &[Expr]) -> Result<String, fmt::Error> {
    schema_name_from_exprs_inner(exprs, ", ")
}

fn schema_name_from_exprs_inner(exprs: &[Expr], sep: &str) -> Result<String, fmt::Error> {
    let mut s = String::new();
    for (i, e) in exprs.iter().enumerate() {
        if i > 0 {
            write!(&mut s, "{sep}")?;
        }
        write!(&mut s, "{}", SchemaDisplay(e))?;
    }

    Ok(s)
}

pub fn schema_name_from_sorts(sorts: &[Sort]) -> Result<String, fmt::Error> {
    let mut s = String::new();
    for (i, e) in sorts.iter().enumerate() {
        if i > 0 {
            write!(&mut s, ", ")?;
        }
        let ordering = if e.asc { "ASC" } else { "DESC" };
        let nulls_ordering = if e.nulls_first {
            "NULLS FIRST"
        } else {
            "NULLS LAST"
        };
        write!(&mut s, "{} {} {}", e.expr, ordering, nulls_ordering)?;
    }

    Ok(s)
}

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl Display for Expr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Expr::Alias(Alias { expr, name, .. }, _) => write!(f, "{expr} AS {name}"),
            Expr::Column(c, _) => write!(f, "{c}"),
            Expr::OuterReferenceColumn(_, c, _) => write!(f, "outer_ref({c})"),
            Expr::ScalarVariable(_, var_names, _) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v, _) => write!(f, "{v:?}"),
            Expr::Case(case, _) => {
                write!(f, "CASE ")?;
                if let Some(e) = &case.expr {
                    write!(f, "{e} ")?;
                }
                for (w, t) in &case.when_then_expr {
                    write!(f, "WHEN {w} THEN {t} ")?;
                }
                if let Some(e) = &case.else_expr {
                    write!(f, "ELSE {e} ")?;
                }
                write!(f, "END")
            }
            Expr::Cast(Cast { expr, data_type }, _) => {
                write!(f, "CAST({expr} AS {data_type:?})")
            }
            Expr::TryCast(TryCast { expr, data_type }, _) => {
                write!(f, "TRY_CAST({expr} AS {data_type:?})")
            }
            Expr::Not(expr, _) => write!(f, "NOT {expr}"),
            Expr::Negative(expr, _) => write!(f, "(- {expr})"),
            Expr::IsNull(expr, _) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr, _) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr, _) => write!(f, "{expr} IS TRUE"),
            Expr::IsFalse(expr, _) => write!(f, "{expr} IS FALSE"),
            Expr::IsUnknown(expr, _) => write!(f, "{expr} IS UNKNOWN"),
            Expr::IsNotTrue(expr, _) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsNotFalse(expr, _) => write!(f, "{expr} IS NOT FALSE"),
            Expr::IsNotUnknown(expr, _) => write!(f, "{expr} IS NOT UNKNOWN"),
            Expr::Exists(
                Exists {
                    subquery,
                    negated: true,
                },
                _,
            ) => write!(f, "NOT EXISTS ({subquery:?})"),
            Expr::Exists(
                Exists {
                    subquery,
                    negated: false,
                },
                _,
            ) => write!(f, "EXISTS ({subquery:?})"),
            Expr::InSubquery(
                InSubquery {
                    expr,
                    subquery,
                    negated: true,
                },
                _,
            ) => write!(f, "{expr} NOT IN ({subquery:?})"),
            Expr::InSubquery(
                InSubquery {
                    expr,
                    subquery,
                    negated: false,
                },
                _,
            ) => write!(f, "{expr} IN ({subquery:?})"),
            Expr::ScalarSubquery(subquery, _) => write!(f, "({subquery:?})"),
            Expr::BinaryExpr(expr, _) => write!(f, "{expr}"),
            Expr::ScalarFunction(fun, _) => {
                fmt_function(f, fun.name(), false, &fun.args, true)
            }
            // TODO: use udf's display_name, need to fix the seperator issue, <https://github.com/apache/datafusion/issues/10364>
            // Expr::ScalarFunction(ScalarFunction { func, args }) => {
            //     write!(f, "{}", func.display_name(args).unwrap())
            // }
            Expr::WindowFunction(
                WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
                _,
            ) => {
                fmt_function(f, &fun.to_string(), false, args, true)?;

                if let Some(nt) = null_treatment {
                    write!(f, "{}", nt)?;
                }

                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY [{}]", expr_vec_fmt!(partition_by))?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(order_by))?;
                }
                write!(
                    f,
                    " {} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, window_frame.end_bound
                )?;
                Ok(())
            }
            Expr::AggregateFunction(
                AggregateFunction {
                    func,
                    distinct,
                    ref args,
                    filter,
                    order_by,
                    null_treatment,
                    ..
                },
                _,
            ) => {
                fmt_function(f, func.name(), *distinct, args, true)?;
                if let Some(nt) = null_treatment {
                    write!(f, " {}", nt)?;
                }
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
                }
                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
            Expr::Between(
                Between {
                    expr,
                    negated,
                    low,
                    high,
                },
                _,
            ) => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                },
                _,
            ) => {
                write!(f, "{expr}")?;
                let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " {op_name} {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " {op_name} {pattern}")
                }
            }
            Expr::SimilarTo(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive: _,
                },
                _,
            ) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " SIMILAR TO {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " SIMILAR TO {pattern}")
                }
            }
            Expr::InList(
                InList {
                    expr,
                    list,
                    negated,
                },
                _,
            ) => {
                if *negated {
                    write!(f, "{expr} NOT IN ([{}])", expr_vec_fmt!(list))
                } else {
                    write!(f, "{expr} IN ([{}])", expr_vec_fmt!(list))
                }
            }
            Expr::Wildcard(Wildcard { qualifier, options }, _) => match qualifier {
                Some(qualifier) => write!(f, "{qualifier}.*{options}"),
                None => write!(f, "*{options}"),
            },
            Expr::GroupingSet(grouping_sets, _) => match grouping_sets {
                GroupingSet::Rollup(exprs) => {
                    // ROLLUP (c0, c1, c2)
                    write!(f, "ROLLUP ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::Cube(exprs) => {
                    // CUBE (c0, c1, c2)
                    write!(f, "CUBE ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    // GROUPING SETS ((c0), (c1, c2), (c3, c4))
                    write!(
                        f,
                        "GROUPING SETS ({})",
                        lists_of_exprs
                            .iter()
                            .map(|exprs| format!("({})", expr_vec_fmt!(exprs)))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
            },
            Expr::Placeholder(Placeholder { id, .. }, _) => write!(f, "{id}"),
            Expr::Unnest(Unnest { expr }, _) => {
                write!(f, "UNNEST({expr})")
            }
        }
    }
}

fn fmt_function(
    f: &mut Formatter,
    fun: &str,
    distinct: bool,
    args: &[Expr],
    display: bool,
) -> fmt::Result {
    let args: Vec<String> = match display {
        true => args.iter().map(|arg| format!("{arg}")).collect(),
        false => args.iter().map(|arg| format!("{arg:?}")).collect(),
    };

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

/// The name of the column (field) that this `Expr` will produce in the physical plan.
/// The difference from [Expr::schema_name] is that top-level columns are unqualified.
pub fn physical_name(expr: &Expr) -> Result<String> {
    if let Expr::Column(col, _) = expr {
        Ok(col.name.clone())
    } else {
        Ok(expr.schema_name().to_string())
    }
}

#[cfg(test)]
mod test {
    use crate::expr_fn::col;
    use crate::{
        case, lit, qualified_wildcard, wildcard, wildcard_with_options, ColumnarValue,
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Volatility,
    };
    use sqlparser::ast;
    use sqlparser::ast::{Ident, IdentWithAlias};
    use std::any::Any;

    #[test]
    #[allow(deprecated)]
    fn format_case_when() -> Result<()> {
        let expr = case(col("a"))
            .when(lit(1), lit(true))
            .when(lit(0), lit(false))
            .otherwise(lit(ScalarValue::Null))?;
        let expected = "CASE a WHEN Int32(1) THEN Boolean(true) WHEN Int32(0) THEN Boolean(false) ELSE NULL END";
        assert_eq!(expected, expr.canonical_name());
        assert_eq!(expected, format!("{expr}"));
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn format_cast() -> Result<()> {
        let expr = Expr::cast(Cast {
            expr: Box::new(Expr::literal(ScalarValue::Float32(Some(1.23)))),
            data_type: DataType::Utf8,
        });
        let expected_canonical = "CAST(Float32(1.23) AS Utf8)";
        assert_eq!(expected_canonical, expr.canonical_name());
        assert_eq!(expected_canonical, format!("{expr}"));
        // Note that CAST intentionally has a name that is different from its `Display`
        // representation. CAST does not change the name of expressions.
        assert_eq!("Float32(1.23)", expr.schema_name().to_string());
        Ok(())
    }

    #[test]
    fn test_partial_ord() {
        // Test validates that partial ord is defined for Expr, not
        // intended to exhaustively test all possibilities
        let exp1 = col("a") + lit(1);
        let exp2 = col("a") + lit(2);
        let exp3 = !(col("a") + lit(2));

        assert!(exp1 < exp2);
        assert!(exp3 > exp2);
        assert!(exp1 < exp3)
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        // single column
        {
            let expr = &Expr::cast(Cast::new(Box::new(col("a")), DataType::Float64));
            let columns = expr.column_refs();
            assert_eq!(1, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
        }

        // multiple columns
        {
            let expr = col("a") + col("b") + lit(1);
            let columns = expr.column_refs();
            assert_eq!(2, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
            assert!(columns.contains(&Column::from_name("b")));
        }

        Ok(())
    }

    #[test]
    fn test_logical_ops() {
        assert_eq!(
            format!("{}", lit(1u32).eq(lit(2u32))),
            "UInt32(1) = UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).not_eq(lit(2u32))),
            "UInt32(1) != UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt(lit(2u32))),
            "UInt32(1) > UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt_eq(lit(2u32))),
            "UInt32(1) >= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt(lit(2u32))),
            "UInt32(1) < UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt_eq(lit(2u32))),
            "UInt32(1) <= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).and(lit(2u32))),
            "UInt32(1) AND UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).or(lit(2u32))),
            "UInt32(1) OR UInt32(2)"
        );
    }

    #[test]
    fn test_is_volatile_scalar_func() {
        // UDF
        #[derive(Debug)]
        struct TestScalarUDF {
            signature: Signature,
        }
        impl ScalarUDFImpl for TestScalarUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "TestScalarUDF"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Utf8)
            }

            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::from("a")))
            }
        }
        let udf = Arc::new(ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        }));
        assert_ne!(udf.signature().volatility, Volatility::Volatile);

        let udf = Arc::new(ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));
        assert_eq!(udf.signature().volatility, Volatility::Volatile);
    }

    use super::*;

    #[test]
    fn test_display_wildcard() {
        assert_eq!(format!("{}", wildcard()), "*");
        assert_eq!(format!("{}", qualified_wildcard("t1")), "t1.*");
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    Some(IlikeSelectItem {
                        pattern: "c1".to_string()
                    }),
                    None,
                    None,
                    None,
                    None
                ))
            ),
            "* ILIKE 'c1'"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    Some(ExcludeSelectItem::Multiple(vec![
                        Ident::from("c1"),
                        Ident::from("c2")
                    ])),
                    None,
                    None,
                    None
                ))
            ),
            "* EXCLUDE (c1, c2)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    Some(ExceptSelectItem {
                        first_element: Ident::from("c1"),
                        additional_elements: vec![Ident::from("c2")]
                    }),
                    None,
                    None
                ))
            ),
            "* EXCEPT (c1, c2)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    None,
                    Some(PlannedReplaceSelectItem {
                        items: vec![ReplaceSelectElement {
                            expr: ast::Expr::Identifier(Ident::from("c1")),
                            column_name: Ident::from("a1"),
                            as_keyword: false
                        }],
                        planned_expressions: vec![]
                    }),
                    None
                ))
            ),
            "* REPLACE (c1 a1)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    None,
                    None,
                    Some(RenameSelectItem::Multiple(vec![IdentWithAlias {
                        ident: Ident::from("c1"),
                        alias: Ident::from("a1")
                    }]))
                ))
            ),
            "* RENAME (c1 AS a1)"
        )
    }

    fn wildcard_options(
        opt_ilike: Option<IlikeSelectItem>,
        opt_exclude: Option<ExcludeSelectItem>,
        opt_except: Option<ExceptSelectItem>,
        opt_replace: Option<PlannedReplaceSelectItem>,
        opt_rename: Option<RenameSelectItem>,
    ) -> WildcardOptions {
        WildcardOptions {
            ilike: opt_ilike,
            exclude: opt_exclude,
            except: opt_except,
            replace: opt_replace,
            rename: opt_rename,
        }
    }
}

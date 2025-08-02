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

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Display, Formatter, Write};
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use crate::expr_fn::binary_expr;
use crate::function::WindowFunctionSimplification;
use crate::logical_plan::Subquery;
use crate::{AggregateUDF, Volatility};
use crate::{ExprSchemable, Operator, Signature, WindowFrame, WindowUDF};

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cse::{HashNode, NormalizeEq, Normalizeable};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};
use datafusion_common::{
    Column, DFSchema, HashMap, Result, ScalarValue, Spans, TableReference,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
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
///    right: Expr::Literal(ScalarValue::Int32(Some(1)), None)
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
/// # Printing Expressions
///
/// You can print `Expr`s using the the `Debug` trait, `Display` trait, or
/// [`Self::human_display`]. See the [examples](#examples-displaying-exprs) below.
///
/// If you need  SQL to pass to other systems, consider using [`Unparser`].
///
/// [`Unparser`]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/struct.Unparser.html
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
/// # Examples: Creating and Using `Expr`s
///
/// ## Column References and Literals
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
/// assert_eq!(expr, Expr::Column(Column::from_name("c1")));
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
/// assert_eq!(expr, Expr::Literal(ScalarValue::Int64(Some(42)), None));
/// assert_eq!(expr, Expr::Literal(ScalarValue::Int64(Some(42)), None));
/// // To make a (typed) NULL:
/// let expr = Expr::Literal(ScalarValue::Int64(None), None);
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
/// if let Expr::BinaryExpr(binary_expr) = expr {
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
/// if let Expr::BinaryExpr(binary_expr) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   let scalar = ScalarValue::Int32(Some(42));
///   assert_eq!(*binary_expr.right, Expr::Literal(scalar, None));
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
/// # Examples: Displaying `Exprs`
///
/// There are three ways to print an `Expr` depending on the usecase.
///
/// ## Use `Debug` trait
///
/// Following Rust conventions, the `Debug` implementation prints out the
/// internal structure of the expression, which is useful for debugging.
///
/// ```
/// # use datafusion_expr::{lit, col};
/// let expr = col("c1") + lit(42);
/// assert_eq!(format!("{expr:?}"), "BinaryExpr(BinaryExpr { left: Column(Column { relation: None, name: \"c1\" }), op: Plus, right: Literal(Int32(42), None) })");
/// ```
///
/// ## Use the `Display` trait  (detailed expression)
///
/// The `Display` implementation prints out the expression in a SQL-like form,
/// but has additional details such as the data type of literals. This is useful
/// for understanding the expression in more detail and is used for the low level
/// [`ExplainFormat::Indent`] explain plan format.
///
/// [`ExplainFormat::Indent`]: crate::logical_plan::ExplainFormat::Indent
///
/// ```
/// # use datafusion_expr::{lit, col};
/// let expr = col("c1") + lit(42);
/// assert_eq!(format!("{expr}"), "c1 + Int32(42)");
/// ```
///
/// ## Use [`Self::human_display`] (human readable)
///
/// [`Self::human_display`]  prints out the expression in a SQL-like form, optimized
/// for human consumption by end users. It is used for the
/// [`ExplainFormat::Tree`] explain plan format.
///
/// [`ExplainFormat::Tree`]: crate::logical_plan::ExplainFormat::Tree
///
///```
/// # use datafusion_expr::{lit, col};
/// let expr = col("c1") + lit(42);
/// assert_eq!(format!("{}", expr.human_display()), "c1 + 42");
/// ```
///
/// # Examples: Visiting and Rewriting `Expr`s
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
///   if let Expr::Column(c) = &e {
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
#[derive(Clone, PartialEq, PartialOrd, Eq, Debug, Hash)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Box<Alias>),
    /// A named reference to a qualified field in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(DataType, Vec<String>),
    /// A constant value along with associated [`FieldMetadata`].
    Literal(ScalarValue, Option<FieldMetadata>),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// LIKE expression
    Like(Like),
    /// LIKE expression that uses regular expressions
    SimilarTo(Like),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// True if argument is not NULL, false otherwise. This expression itself is never NULL.
    IsNotNull(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>),
    /// True if argument is true, false otherwise. This expression itself is never NULL.
    IsTrue(Box<Expr>),
    /// True if argument is  false, false otherwise. This expression itself is never NULL.
    IsFalse(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsUnknown(Box<Expr>),
    /// True if argument is FALSE or NULL, false otherwise. This expression itself is never NULL.
    IsNotTrue(Box<Expr>),
    /// True if argument is TRUE OR NULL, false otherwise. This expression itself is never NULL.
    IsNotFalse(Box<Expr>),
    /// True if argument is TRUE or FALSE, false otherwise. This expression itself is never NULL.
    IsNotUnknown(Box<Expr>),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>),
    /// Whether an expression is between a given range.
    Between(Between),
    /// A CASE expression (see docs on [`Case`])
    Case(Case),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast),
    /// Casts the expression to a given type and will return a null value if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    TryCast(TryCast),
    /// Call a scalar function with a set of arguments.
    ScalarFunction(ScalarFunction),
    /// Calls an aggregate function with arguments, and optional
    /// `ORDER BY`, `FILTER`, `DISTINCT` and `NULL TREATMENT`.
    ///
    /// See also [`ExprFunctionExt`] to set these fields.
    ///
    /// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
    AggregateFunction(AggregateFunction),
    /// Call a window function with a set of arguments.
    WindowFunction(Box<WindowFunction>),
    /// Returns whether the list contains the expr value.
    InList(InList),
    /// EXISTS subquery
    Exists(Exists),
    /// IN subquery
    InSubquery(InSubquery),
    /// Scalar subquery
    ScalarSubquery(Subquery),
    /// Represents a reference to all available fields in a specific schema,
    /// with an optional (schema) qualifier.
    ///
    /// This expr has to be resolved to a list of columns before translating logical
    /// plan into physical plan.
    #[deprecated(
        since = "46.0.0",
        note = "A wildcard needs to be resolved to concrete expressions when constructing the logical plan. See https://github.com/apache/datafusion/issues/7765"
    )]
    Wildcard {
        qualifier: Option<TableReference>,
        options: Box<WildcardOptions>,
    },
    /// List of grouping set expressions. Only valid in the context of an aggregate
    /// GROUP BY expression list
    GroupingSet(GroupingSet),
    /// A place holder for parameters in a prepared statement
    /// (e.g. `$foo` or `$1`)
    Placeholder(Placeholder),
    /// A placeholder which holds a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterReferenceColumn(Box<(DataType, Column)>),
    /// Unnest expression
    Unnest(Unnest),
}

impl Default for Expr {
    fn default() -> Self {
        Expr::Literal(ScalarValue::Null, None)
    }
}

/// Create an [`Expr`] from a [`Column`]
impl From<Column> for Expr {
    fn from(value: Column) -> Self {
        Expr::Column(value)
    }
}

/// Create an [`Expr`] from a [`WindowFunction`]
impl From<WindowFunction> for Expr {
    fn from(value: WindowFunction) -> Self {
        Expr::WindowFunction(Box::new(value))
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

/// Literal metadata
///
/// Stores metadata associated with a literal expressions
/// and is designed to be fast to `clone`.
///
/// This structure is used to store metadata associated with a literal expression, and it
/// corresponds to the `metadata` field on [`Field`].
///
/// # Example: Create [`FieldMetadata`] from a [`Field`]
/// ```
/// # use std::collections::HashMap;
/// # use datafusion_expr::expr::FieldMetadata;
/// # use arrow::datatypes::{Field, DataType};
/// # let field = Field::new("c1", DataType::Int32, true)
/// #  .with_metadata(HashMap::from([("foo".to_string(), "bar".to_string())]));
/// // Create a new `FieldMetadata` instance from a `Field`
/// let metadata = FieldMetadata::new_from_field(&field);
/// // There is also a `From` impl:
/// let metadata = FieldMetadata::from(&field);
/// ```
///
/// # Example: Update a [`Field`] with [`FieldMetadata`]
/// ```
/// # use datafusion_expr::expr::FieldMetadata;
/// # use arrow::datatypes::{Field, DataType};
/// # let field = Field::new("c1", DataType::Int32, true);
/// # let metadata = FieldMetadata::new_from_field(&field);
/// // Add any metadata from `FieldMetadata` to `Field`
/// let updated_field = metadata.add_to_field(field);
/// ```
///
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct FieldMetadata {
    /// The inner metadata of a literal expression, which is a map of string
    /// keys to string values.
    ///
    /// Note this is not a `HashMap because `HashMap` does not provide
    /// implementations for traits like `Debug` and `Hash`.
    inner: Arc<BTreeMap<String, String>>,
}

impl Default for FieldMetadata {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl FieldMetadata {
    /// Create a new empty metadata instance.
    pub fn new_empty() -> Self {
        Self {
            inner: Arc::new(BTreeMap::new()),
        }
    }

    /// Merges two optional `FieldMetadata` instances, overwriting any existing
    /// keys in `m` with keys from `n` if present
    pub fn merge_options(
        m: Option<&FieldMetadata>,
        n: Option<&FieldMetadata>,
    ) -> Option<FieldMetadata> {
        match (m, n) {
            (Some(m), Some(n)) => {
                let mut merged = m.clone();
                merged.extend(n.clone());
                Some(merged)
            }
            (Some(m), None) => Some(m.clone()),
            (None, Some(n)) => Some(n.clone()),
            (None, None) => None,
        }
    }

    /// Create a new metadata instance from a `Field`'s metadata.
    pub fn new_from_field(field: &Field) -> Self {
        let inner = field
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new metadata instance from a map of string keys to string values.
    pub fn new(inner: BTreeMap<String, String>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Get the inner metadata as a reference to a `BTreeMap`.
    pub fn inner(&self) -> &BTreeMap<String, String> {
        &self.inner
    }

    /// Return the inner metadata
    pub fn into_inner(self) -> Arc<BTreeMap<String, String>> {
        self.inner
    }

    /// Adds metadata from `other` into `self`, overwriting any existing keys.
    pub fn extend(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        let other = Arc::unwrap_or_clone(other.into_inner());
        Arc::make_mut(&mut self.inner).extend(other);
    }

    /// Returns true if the metadata is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of key-value pairs in the metadata.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Convert this `FieldMetadata` into a `HashMap<String, String>`
    pub fn to_hashmap(&self) -> std::collections::HashMap<String, String> {
        self.inner
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Updates the metadata on the Field with this metadata, if it is not empty.
    pub fn add_to_field(&self, field: Field) -> Field {
        if self.inner.is_empty() {
            return field;
        }

        field.with_metadata(self.to_hashmap())
    }
}

impl From<&Field> for FieldMetadata {
    fn from(field: &Field) -> Self {
        Self::new_from_field(field)
    }
}

impl From<BTreeMap<String, String>> for FieldMetadata {
    fn from(inner: BTreeMap<String, String>) -> Self {
        Self::new(inner)
    }
}

impl From<std::collections::HashMap<String, String>> for FieldMetadata {
    fn from(map: std::collections::HashMap<String, String>) -> Self {
        Self::new(map.into_iter().collect())
    }
}

/// From reference
impl From<&std::collections::HashMap<String, String>> for FieldMetadata {
    fn from(map: &std::collections::HashMap<String, String>) -> Self {
        let inner = map
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self::new(inner)
    }
}

/// From hashbrown map
impl From<HashMap<String, String>> for FieldMetadata {
    fn from(map: HashMap<String, String>) -> Self {
        let inner = map.into_iter().collect();
        Self::new(inner)
    }
}

impl From<&HashMap<String, String>> for FieldMetadata {
    fn from(map: &HashMap<String, String>) -> Self {
        let inner = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self::new(inner)
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
}

/// Alias expression
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub relation: Option<TableReference>,
    pub name: String,
    pub metadata: Option<FieldMetadata>,
}

impl Hash for Alias {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.relation.hash(state);
        self.name.hash(state);
    }
}

impl PartialOrd for Alias {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self.expr.partial_cmp(&other.expr);
        let Some(Ordering::Equal) = cmp else {
            return cmp;
        };
        let cmp = self.relation.partial_cmp(&other.relation);
        let Some(Ordering::Equal) = cmp else {
            return cmp;
        };
        self.name.partial_cmp(&other.name)
    }
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
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: Option<FieldMetadata>) -> Self {
        self.metadata = metadata;
        self
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
                Expr::BinaryExpr(child) => {
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
///
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
}

/// Invoke a [`ScalarUDF`] with a set of arguments
///
/// [`ScalarUDF`]: crate::ScalarUDF
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
}

impl ScalarFunction {
    /// Create a new `ScalarFunction` from a [`ScalarUDF`]
    ///
    /// [`ScalarUDF`]: crate::ScalarUDF
    pub fn new_udf(udf: Arc<crate::ScalarUDF>, args: Vec<Expr>) -> Self {
        Self { func: udf, args }
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
    pub func: Arc<AggregateUDF>,
    pub params: AggregateFunctionParams,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct AggregateFunctionParams {
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ordering
    pub order_by: Vec<Sort>,
    pub null_treatment: Option<NullTreatment>,
}

impl AggregateFunction {
    /// Create a new AggregateFunction expression with a user-defined function (UDF)
    pub fn new_udf(
        func: Arc<AggregateUDF>,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Vec<Sort>,
        null_treatment: Option<NullTreatment>,
    ) -> Self {
        Self {
            func,
            params: AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment,
            },
        }
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
    AggregateUDF(Arc<AggregateUDF>),
    /// A user defined aggregate function
    WindowUDF(Arc<WindowUDF>),
}

impl WindowFunctionDefinition {
    /// Returns the datatype of the window function
    pub fn return_field(
        &self,
        input_expr_fields: &[FieldRef],
        _input_expr_nullable: &[bool],
        display_name: &str,
    ) -> Result<FieldRef> {
        match self {
            WindowFunctionDefinition::AggregateUDF(fun) => {
                fun.return_field(input_expr_fields)
            }
            WindowFunctionDefinition::WindowUDF(fun) => {
                fun.field(WindowUDFFieldArgs::new(input_expr_fields, display_name))
            }
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

    /// Return the the inner window simplification function, if any
    ///
    /// See [`WindowFunctionSimplification`] for more information
    pub fn simplify(&self) -> Option<WindowFunctionSimplification> {
        match self {
            WindowFunctionDefinition::AggregateUDF(_) => None,
            WindowFunctionDefinition::WindowUDF(udwf) => udwf.simplify(),
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

impl From<Arc<AggregateUDF>> for WindowFunctionDefinition {
    fn from(value: Arc<AggregateUDF>) -> Self {
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
    pub params: WindowFunctionParams,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct WindowFunctionParams {
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
    /// Distinct flag
    pub distinct: bool,
}

impl WindowFunction {
    /// Create a new Window expression with the specified argument an
    /// empty `OVER` clause
    pub fn new(fun: impl Into<WindowFunctionDefinition>, args: Vec<Expr>) -> Self {
        Self {
            fun: fun.into(),
            params: WindowFunctionParams {
                args,
                partition_by: Vec::default(),
                order_by: Vec::default(),
                window_frame: WindowFrame::new(None),
                null_treatment: None,
                distinct: false,
            },
        }
    }

    /// Return the the inner window simplification function, if any
    ///
    /// See [`WindowFunctionSimplification`] for more information
    pub fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.fun.simplify()
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

    /// Human readable display formatting for this expression.
    ///
    /// This function is primarily used in printing the explain tree output,
    /// (e.g. `EXPLAIN FORMAT TREE <query>`), providing a readable format to
    /// show how expressions are used in physical and logical plans. See the
    /// [`Expr`] for other ways to format expressions
    ///
    /// Note this format is intended for human consumption rather than SQL for
    /// other systems. If you need  SQL to pass to other systems, consider using
    /// [`Unparser`].
    ///
    /// [`Unparser`]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/struct.Unparser.html
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::{col, lit};
    /// let expr = col("foo") + lit(42);
    /// // For EXPLAIN output:
    /// // "foo + 42"
    /// println!("{}", expr.human_display());
    /// ```
    pub fn human_display(&self) -> impl Display + '_ {
        SqlDisplay(self)
    }

    /// Returns the qualifier and the schema name of this expression.
    ///
    /// Used when the expression forms the output field of a certain plan.
    /// The result is the field's qualifier and field name in the plan's
    /// output schema. We can use this qualified name to reference the field.
    pub fn qualified_name(&self) -> (Option<TableReference>, String) {
        match self {
            Expr::Column(Column {
                relation,
                name,
                spans: _,
            }) => (relation.clone(), name.clone()),
            Expr::Alias(boxed_alias) => {
                (boxed_alias.relation.clone(), boxed_alias.name.clone())
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
            Expr::OuterReferenceColumn(_) => "Outer",
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
            Expr::Placeholder(_) => "Placeholder",
            Expr::ScalarFunction(..) => "ScalarFunction",
            Expr::ScalarSubquery { .. } => "ScalarSubquery",
            Expr::ScalarVariable(..) => "ScalarVariable",
            Expr::TryCast { .. } => "TryCast",
            Expr::WindowFunction { .. } => "WindowFunction",
            #[expect(deprecated)]
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
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            true,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self ILIKE other`
    pub fn ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            true,
        ))
    }

    /// Return `self NOT ILIKE other`
    pub fn not_ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(true, Box::new(self), Box::new(other), None, true))
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
        Expr::Alias(Box::new(Alias::new(self, None::<&str>, name.into())))
    }

    /// Return `self AS name` alias expression with metadata
    ///
    /// The metadata will be attached to the Arrow Schema field when the expression
    /// is converted to a field via `Expr.to_field()`.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// # use std::collections::HashMap;
    /// # use datafusion_expr::expr::FieldMetadata;
    /// let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
    /// let metadata = FieldMetadata::from(metadata);
    /// let expr = col("foo").alias_with_metadata("bar", Some(metadata));
    /// ```
    ///
    pub fn alias_with_metadata(
        self,
        name: impl Into<String>,
        metadata: Option<FieldMetadata>,
    ) -> Expr {
        Expr::Alias(Box::new(
            Alias::new(self, None::<&str>, name.into()).with_metadata(metadata),
        ))
    }

    /// Return `self AS name` alias expression with a specific qualifier
    pub fn alias_qualified(
        self,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Expr {
        Expr::Alias(Box::new(Alias::new(self, relation, name.into())))
    }

    /// Return `self AS name` alias expression with a specific qualifier and metadata
    ///
    /// The metadata will be attached to the Arrow Schema field when the expression
    /// is converted to a field via `Expr.to_field()`.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// # use std::collections::HashMap;
    /// # use datafusion_expr::expr::FieldMetadata;
    /// let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
    /// let metadata = FieldMetadata::from(metadata);
    /// let expr = col("foo").alias_qualified_with_metadata(Some("tbl"), "bar", Some(metadata));
    /// ```
    ///
    pub fn alias_qualified_with_metadata(
        self,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
        metadata: Option<FieldMetadata>,
    ) -> Expr {
        Expr::Alias(Box::new(
            Alias::new(self, relation, name.into()).with_metadata(metadata),
        ))
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
    /// // `foo as "bar" as "baz" is unaliased to foo as "bar"
    /// let expr = col("foo").alias("bar").alias("baz");
    /// assert_eq!(expr.unalias(), col("foo").alias("bar"));
    /// ```
    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(alias) => *alias.expr,
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
                    Expr::Exists { .. } | Expr::ScalarSubquery(_) | Expr::InSubquery(_)
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
                if let Expr::Alias(alias) = expr {
                    match alias
                        .metadata
                        .as_ref()
                        .map(|h| h.is_empty())
                        .unwrap_or(true)
                    {
                        true => Ok(Transformed::yes(*alias.expr)),
                        false => Ok(Transformed::no(Expr::Alias(alias))),
                    }
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
        Expr::InList(InList::new(Box::new(self), list, negated))
    }

    /// Return `IsNull(Box(self))
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
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
        Expr::IsTrue(Box::new(self))
    }

    /// Return `IsNotTrue(Box(self))`
    pub fn is_not_true(self) -> Expr {
        Expr::IsNotTrue(Box::new(self))
    }

    /// Return `IsFalse(Box(self))`
    pub fn is_false(self) -> Expr {
        Expr::IsFalse(Box::new(self))
    }

    /// Return `IsNotFalse(Box(self))`
    pub fn is_not_false(self) -> Expr {
        Expr::IsNotFalse(Box::new(self))
    }

    /// Return `IsUnknown(Box(self))`
    pub fn is_unknown(self) -> Expr {
        Expr::IsUnknown(Box::new(self))
    }

    /// Return `IsNotUnknown(Box(self))`
    pub fn is_not_unknown(self) -> Expr {
        Expr::IsNotUnknown(Box::new(self))
    }

    /// return `self BETWEEN low AND high`
    pub fn between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            false,
            Box::new(low),
            Box::new(high),
        ))
    }

    /// Return `self NOT BETWEEN low AND high`
    pub fn not_between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            true,
            Box::new(low),
            Box::new(high),
        ))
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
        if let Expr::Column(it) = self {
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
            Expr::Column(c) => Some(c),
            Expr::Cast(Cast { expr, .. }) => match &**expr {
                Expr::Column(c) => Some(c),
                _ => None,
            },
            _ => None,
        }
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
            if let Expr::Column(col) = expr {
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
            if let Expr::Column(col) = expr {
                *map.entry(col).or_default() += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
    }

    /// Returns true if there are any column references in this Expr
    pub fn any_column_refs(&self) -> bool {
        self.exists(|expr| Ok(matches!(expr, Expr::Column(_))))
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
        matches!(self, Expr::ScalarFunction(func) if func.func.signature().volatility == Volatility::Volatile)
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
    /// For example, given an expression like `<int32> = $0` will infer `$0` to
    /// have type `int32`.
    ///
    /// Returns transformed expression and flag that is true if expression contains
    /// at least one placeholder.
    pub fn infer_placeholder_types(self, schema: &DFSchema) -> Result<(Expr, bool)> {
        let mut has_placeholder = false;
        self.transform(|mut expr| {
            match &mut expr {
                // Default to assuming the arguments are the same type
                Expr::BinaryExpr(BinaryExpr { left, op: _, right }) => {
                    rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
                    rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
                }
                Expr::Between(Between {
                    expr,
                    negated: _,
                    low,
                    high,
                }) => {
                    rewrite_placeholder(low.as_mut(), expr.as_ref(), schema)?;
                    rewrite_placeholder(high.as_mut(), expr.as_ref(), schema)?;
                }
                Expr::InList(InList {
                    expr,
                    list,
                    negated: _,
                }) => {
                    for item in list.iter_mut() {
                        rewrite_placeholder(item, expr.as_ref(), schema)?;
                    }
                }
                Expr::Like(Like { expr, pattern, .. })
                | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                    rewrite_placeholder(pattern.as_mut(), expr.as_ref(), schema)?;
                }
                Expr::Placeholder(_) => {
                    has_placeholder = true;
                }
                _ => {}
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
            Expr::ScalarFunction(ScalarFunction { func, .. }) => func.short_circuits(),
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                matches!(op, Operator::And | Operator::Or)
            }
            Expr::Case { .. } => true,
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
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
            | Expr::ScalarVariable(_, _)
            | Expr::SimilarTo(..)
            | Expr::Not(..)
            | Expr::Negative(..)
            | Expr::OuterReferenceColumn(_)
            | Expr::TryCast(..)
            | Expr::Unnest(..)
            | Expr::Wildcard { .. }
            | Expr::WindowFunction(..)
            | Expr::Literal(..)
            | Expr::Placeholder(..) => false,
        }
    }

    /// Returns a reference to the set of locations in the SQL query where this
    /// expression appears, if known. [`None`] is returned if the expression
    /// type doesn't support tracking locations yet.
    pub fn spans(&self) -> Option<&Spans> {
        match self {
            Expr::Column(col) => Some(&col.spans),
            _ => None,
        }
    }

    /// Check if the Expr is literal and get the literal value if it is.
    pub fn as_literal(&self) -> Option<&ScalarValue> {
        if let Expr::Literal(lit, _) = self {
            Some(lit)
        } else {
            None
        }
    }
}

impl Normalizeable for Expr {
    fn can_normalize(&self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match self {
            Expr::BinaryExpr(BinaryExpr {
                op:
                    _op @ (Operator::Plus
                    | Operator::Multiply
                    | Operator::BitwiseAnd
                    | Operator::BitwiseOr
                    | Operator::BitwiseXor
                    | Operator::Eq
                    | Operator::NotEq),
                ..
            }) => true,
            _ => false,
        }
    }
}

impl NormalizeEq for Expr {
    fn normalize_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: self_left,
                    op: self_op,
                    right: self_right,
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: other_left,
                    op: other_op,
                    right: other_right,
                }),
            ) => {
                if self_op != other_op {
                    return false;
                }

                if matches!(
                    self_op,
                    Operator::Plus
                        | Operator::Multiply
                        | Operator::BitwiseAnd
                        | Operator::BitwiseOr
                        | Operator::BitwiseXor
                        | Operator::Eq
                        | Operator::NotEq
                ) {
                    (self_left.normalize_eq(other_left)
                        && self_right.normalize_eq(other_right))
                        || (self_left.normalize_eq(other_right)
                            && self_right.normalize_eq(other_left))
                } else {
                    self_left.normalize_eq(other_left)
                        && self_right.normalize_eq(other_right)
                }
            }
            (Expr::Alias(boxed_alias), Expr::Alias(boxed_other_alias)) => {
                boxed_alias.name == boxed_other_alias.name
                    && boxed_alias.relation == boxed_other_alias.relation
                    && boxed_alias.expr.normalize_eq(&*boxed_other_alias.expr)
            }
            (
                Expr::Like(Like {
                    negated: self_negated,
                    expr: self_expr,
                    pattern: self_pattern,
                    escape_char: self_escape_char,
                    case_insensitive: self_case_insensitive,
                }),
                Expr::Like(Like {
                    negated: other_negated,
                    expr: other_expr,
                    pattern: other_pattern,
                    escape_char: other_escape_char,
                    case_insensitive: other_case_insensitive,
                }),
            )
            | (
                Expr::SimilarTo(Like {
                    negated: self_negated,
                    expr: self_expr,
                    pattern: self_pattern,
                    escape_char: self_escape_char,
                    case_insensitive: self_case_insensitive,
                }),
                Expr::SimilarTo(Like {
                    negated: other_negated,
                    expr: other_expr,
                    pattern: other_pattern,
                    escape_char: other_escape_char,
                    case_insensitive: other_case_insensitive,
                }),
            ) => {
                self_negated == other_negated
                    && self_escape_char == other_escape_char
                    && self_case_insensitive == other_case_insensitive
                    && self_expr.normalize_eq(other_expr)
                    && self_pattern.normalize_eq(other_pattern)
            }
            (Expr::Not(self_expr), Expr::Not(other_expr))
            | (Expr::IsNull(self_expr), Expr::IsNull(other_expr))
            | (Expr::IsTrue(self_expr), Expr::IsTrue(other_expr))
            | (Expr::IsFalse(self_expr), Expr::IsFalse(other_expr))
            | (Expr::IsUnknown(self_expr), Expr::IsUnknown(other_expr))
            | (Expr::IsNotNull(self_expr), Expr::IsNotNull(other_expr))
            | (Expr::IsNotTrue(self_expr), Expr::IsNotTrue(other_expr))
            | (Expr::IsNotFalse(self_expr), Expr::IsNotFalse(other_expr))
            | (Expr::IsNotUnknown(self_expr), Expr::IsNotUnknown(other_expr))
            | (Expr::Negative(self_expr), Expr::Negative(other_expr))
            | (
                Expr::Unnest(Unnest { expr: self_expr }),
                Expr::Unnest(Unnest { expr: other_expr }),
            ) => self_expr.normalize_eq(other_expr),
            (
                Expr::Between(Between {
                    expr: self_expr,
                    negated: self_negated,
                    low: self_low,
                    high: self_high,
                }),
                Expr::Between(Between {
                    expr: other_expr,
                    negated: other_negated,
                    low: other_low,
                    high: other_high,
                }),
            ) => {
                self_negated == other_negated
                    && self_expr.normalize_eq(other_expr)
                    && self_low.normalize_eq(other_low)
                    && self_high.normalize_eq(other_high)
            }
            (
                Expr::Cast(Cast {
                    expr: self_expr,
                    data_type: self_data_type,
                }),
                Expr::Cast(Cast {
                    expr: other_expr,
                    data_type: other_data_type,
                }),
            )
            | (
                Expr::TryCast(TryCast {
                    expr: self_expr,
                    data_type: self_data_type,
                }),
                Expr::TryCast(TryCast {
                    expr: other_expr,
                    data_type: other_data_type,
                }),
            ) => self_data_type == other_data_type && self_expr.normalize_eq(other_expr),
            (
                Expr::ScalarFunction(ScalarFunction {
                    func: self_func,
                    args: self_args,
                }),
                Expr::ScalarFunction(ScalarFunction {
                    func: other_func,
                    args: other_args,
                }),
            ) => {
                self_func.name() == other_func.name()
                    && self_args.len() == other_args.len()
                    && self_args
                        .iter()
                        .zip(other_args.iter())
                        .all(|(a, b)| a.normalize_eq(b))
            }
            (
                Expr::AggregateFunction(AggregateFunction {
                    func: self_func,
                    params:
                        AggregateFunctionParams {
                            args: self_args,
                            distinct: self_distinct,
                            filter: self_filter,
                            order_by: self_order_by,
                            null_treatment: self_null_treatment,
                        },
                }),
                Expr::AggregateFunction(AggregateFunction {
                    func: other_func,
                    params:
                        AggregateFunctionParams {
                            args: other_args,
                            distinct: other_distinct,
                            filter: other_filter,
                            order_by: other_order_by,
                            null_treatment: other_null_treatment,
                        },
                }),
            ) => {
                self_func.name() == other_func.name()
                    && self_distinct == other_distinct
                    && self_null_treatment == other_null_treatment
                    && self_args.len() == other_args.len()
                    && self_args
                        .iter()
                        .zip(other_args.iter())
                        .all(|(a, b)| a.normalize_eq(b))
                    && match (self_filter, other_filter) {
                        (Some(self_filter), Some(other_filter)) => {
                            self_filter.normalize_eq(other_filter)
                        }
                        (None, None) => true,
                        _ => false,
                    }
                    && self_order_by
                        .iter()
                        .zip(other_order_by.iter())
                        .all(|(a, b)| {
                            a.asc == b.asc
                                && a.nulls_first == b.nulls_first
                                && a.expr.normalize_eq(&b.expr)
                        })
                    && self_order_by.len() == other_order_by.len()
            }
            (Expr::WindowFunction(left), Expr::WindowFunction(other)) => {
                let WindowFunction {
                    fun: self_fun,
                    params:
                        WindowFunctionParams {
                            args: self_args,
                            window_frame: self_window_frame,
                            partition_by: self_partition_by,
                            order_by: self_order_by,
                            null_treatment: self_null_treatment,
                            distinct: self_distinct,
                        },
                } = left.as_ref();
                let WindowFunction {
                    fun: other_fun,
                    params:
                        WindowFunctionParams {
                            args: other_args,
                            window_frame: other_window_frame,
                            partition_by: other_partition_by,
                            order_by: other_order_by,
                            null_treatment: other_null_treatment,
                            distinct: other_distinct,
                        },
                } = other.as_ref();

                self_fun.name() == other_fun.name()
                    && self_window_frame == other_window_frame
                    && self_null_treatment == other_null_treatment
                    && self_args.len() == other_args.len()
                    && self_args
                        .iter()
                        .zip(other_args.iter())
                        .all(|(a, b)| a.normalize_eq(b))
                    && self_partition_by
                        .iter()
                        .zip(other_partition_by.iter())
                        .all(|(a, b)| a.normalize_eq(b))
                    && self_order_by
                        .iter()
                        .zip(other_order_by.iter())
                        .all(|(a, b)| {
                            a.asc == b.asc
                                && a.nulls_first == b.nulls_first
                                && a.expr.normalize_eq(&b.expr)
                        })
                    && self_distinct == other_distinct
            }
            (
                Expr::Exists(Exists {
                    subquery: self_subquery,
                    negated: self_negated,
                }),
                Expr::Exists(Exists {
                    subquery: other_subquery,
                    negated: other_negated,
                }),
            ) => {
                self_negated == other_negated
                    && self_subquery.normalize_eq(other_subquery)
            }
            (
                Expr::InSubquery(InSubquery {
                    expr: self_expr,
                    subquery: self_subquery,
                    negated: self_negated,
                }),
                Expr::InSubquery(InSubquery {
                    expr: other_expr,
                    subquery: other_subquery,
                    negated: other_negated,
                }),
            ) => {
                self_negated == other_negated
                    && self_expr.normalize_eq(other_expr)
                    && self_subquery.normalize_eq(other_subquery)
            }
            (
                Expr::ScalarSubquery(self_subquery),
                Expr::ScalarSubquery(other_subquery),
            ) => self_subquery.normalize_eq(other_subquery),
            (
                Expr::GroupingSet(GroupingSet::Rollup(self_exprs)),
                Expr::GroupingSet(GroupingSet::Rollup(other_exprs)),
            )
            | (
                Expr::GroupingSet(GroupingSet::Cube(self_exprs)),
                Expr::GroupingSet(GroupingSet::Cube(other_exprs)),
            ) => {
                self_exprs.len() == other_exprs.len()
                    && self_exprs
                        .iter()
                        .zip(other_exprs.iter())
                        .all(|(a, b)| a.normalize_eq(b))
            }
            (
                Expr::GroupingSet(GroupingSet::GroupingSets(self_exprs)),
                Expr::GroupingSet(GroupingSet::GroupingSets(other_exprs)),
            ) => {
                self_exprs.len() == other_exprs.len()
                    && self_exprs.iter().zip(other_exprs.iter()).all(|(a, b)| {
                        a.len() == b.len()
                            && a.iter().zip(b.iter()).all(|(x, y)| x.normalize_eq(y))
                    })
            }
            (
                Expr::InList(InList {
                    expr: self_expr,
                    list: self_list,
                    negated: self_negated,
                }),
                Expr::InList(InList {
                    expr: other_expr,
                    list: other_list,
                    negated: other_negated,
                }),
            ) => {
                // TODO: normalize_eq for lists, for example `a IN (c1 + c3, c3)` is equal to `a IN (c3, c1 + c3)`
                self_negated == other_negated
                    && self_expr.normalize_eq(other_expr)
                    && self_list.len() == other_list.len()
                    && self_list
                        .iter()
                        .zip(other_list.iter())
                        .all(|(a, b)| a.normalize_eq(b))
            }
            (
                Expr::Case(Case {
                    expr: self_expr,
                    when_then_expr: self_when_then_expr,
                    else_expr: self_else_expr,
                }),
                Expr::Case(Case {
                    expr: other_expr,
                    when_then_expr: other_when_then_expr,
                    else_expr: other_else_expr,
                }),
            ) => {
                // TODO: normalize_eq for when_then_expr
                // for example `CASE a WHEN 1 THEN 2 WHEN 3 THEN 4 ELSE 5 END` is equal to `CASE a WHEN 3 THEN 4 WHEN 1 THEN 2 ELSE 5 END`
                self_when_then_expr.len() == other_when_then_expr.len()
                    && self_when_then_expr
                        .iter()
                        .zip(other_when_then_expr.iter())
                        .all(|((self_when, self_then), (other_when, other_then))| {
                            self_when.normalize_eq(other_when)
                                && self_then.normalize_eq(other_then)
                        })
                    && match (self_expr, other_expr) {
                        (Some(self_expr), Some(other_expr)) => {
                            self_expr.normalize_eq(other_expr)
                        }
                        (None, None) => true,
                        (_, _) => false,
                    }
                    && match (self_else_expr, other_else_expr) {
                        (Some(self_else_expr), Some(other_else_expr)) => {
                            self_else_expr.normalize_eq(other_else_expr)
                        }
                        (None, None) => true,
                        (_, _) => false,
                    }
            }
            (_, _) => self == other,
        }
    }
}

impl HashNode for Expr {
    /// As it is pretty easy to forget changing this method when `Expr` changes the
    /// implementation doesn't use wildcard patterns (`..`, `_`) to catch changes
    /// compile time.
    fn hash_node<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            Expr::Alias(boxed_alias) => {
                boxed_alias.relation.hash(state);
                boxed_alias.name.hash(state);
            }
            Expr::Column(column) => {
                column.hash(state);
            }
            Expr::ScalarVariable(data_type, name) => {
                data_type.hash(state);
                name.hash(state);
            }
            Expr::Literal(scalar_value, _) => {
                scalar_value.hash(state);
            }
            Expr::BinaryExpr(BinaryExpr {
                left: _left,
                op,
                right: _right,
            }) => {
                op.hash(state);
            }
            Expr::Like(Like {
                negated,
                expr: _expr,
                pattern: _pattern,
                escape_char,
                case_insensitive,
            })
            | Expr::SimilarTo(Like {
                negated,
                expr: _expr,
                pattern: _pattern,
                escape_char,
                case_insensitive,
            }) => {
                negated.hash(state);
                escape_char.hash(state);
                case_insensitive.hash(state);
            }
            Expr::Not(_expr)
            | Expr::IsNotNull(_expr)
            | Expr::IsNull(_expr)
            | Expr::IsTrue(_expr)
            | Expr::IsFalse(_expr)
            | Expr::IsUnknown(_expr)
            | Expr::IsNotTrue(_expr)
            | Expr::IsNotFalse(_expr)
            | Expr::IsNotUnknown(_expr)
            | Expr::Negative(_expr) => {}
            Expr::Between(Between {
                expr: _expr,
                negated,
                low: _low,
                high: _high,
            }) => {
                negated.hash(state);
            }
            Expr::Case(Case {
                expr: _expr,
                when_then_expr: _when_then_expr,
                else_expr: _else_expr,
            }) => {}
            Expr::Cast(Cast {
                expr: _expr,
                data_type,
            })
            | Expr::TryCast(TryCast {
                expr: _expr,
                data_type,
            }) => {
                data_type.hash(state);
            }
            Expr::ScalarFunction(ScalarFunction { func, args: _args }) => {
                func.hash(state);
            }
            Expr::AggregateFunction(AggregateFunction {
                func,
                params:
                    AggregateFunctionParams {
                        args: _args,
                        distinct,
                        filter: _,
                        order_by: _,
                        null_treatment,
                    },
            }) => {
                func.hash(state);
                distinct.hash(state);
                null_treatment.hash(state);
            }
            Expr::WindowFunction(window_fun) => {
                let WindowFunction {
                    fun,
                    params:
                        WindowFunctionParams {
                            args: _args,
                            partition_by: _,
                            order_by: _,
                            window_frame,
                            null_treatment,
                            distinct,
                        },
                } = window_fun.as_ref();
                fun.hash(state);
                window_frame.hash(state);
                null_treatment.hash(state);
                distinct.hash(state);
            }
            Expr::InList(InList {
                expr: _expr,
                list: _list,
                negated,
            }) => {
                negated.hash(state);
            }
            Expr::Exists(Exists { subquery, negated }) => {
                subquery.hash(state);
                negated.hash(state);
            }
            Expr::InSubquery(InSubquery {
                expr: _expr,
                subquery,
                negated,
            }) => {
                subquery.hash(state);
                negated.hash(state);
            }
            Expr::ScalarSubquery(subquery) => {
                subquery.hash(state);
            }
            #[expect(deprecated)]
            Expr::Wildcard { qualifier, options } => {
                qualifier.hash(state);
                options.hash(state);
            }
            Expr::GroupingSet(grouping_set) => {
                mem::discriminant(grouping_set).hash(state);
                match grouping_set {
                    GroupingSet::Rollup(_exprs) | GroupingSet::Cube(_exprs) => {}
                    GroupingSet::GroupingSets(_exprs) => {}
                }
            }
            Expr::Placeholder(place_holder) => {
                place_holder.hash(state);
            }
            Expr::OuterReferenceColumn(boxed_orc) => {
                let (data_type, column) = boxed_orc.as_ref();
                data_type.hash(state);
                column.hash(state);
            }
            Expr::Unnest(Unnest { expr: _expr }) => {}
        };
    }
}

// Modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }) = expr {
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
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::ScalarVariable(..)
            | Expr::OuterReferenceColumn(..)
            | Expr::Placeholder(_)
            | Expr::Wildcard { .. } => write!(f, "{}", self.0),
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                match func.schema_name(params) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from schema_name {e}")
                    }
                }
            }
            // Expr is not shown since it is aliased
            Expr::Alias(boxed_alias) => {
                let alias = boxed_alias.as_ref();
                if let Some(relation) = &alias.relation {
                    write!(f, "{relation}.{}", alias.name)
                } else {
                    write!(f, "{}", alias.name)
                }
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
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
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                write!(f, "{} {op} {}", SchemaDisplay(left), SchemaDisplay(right),)
            }
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
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
            // Cast expr is not shown to be consistent with Postgres and Spark <https://github.com/apache/datafusion/pull/3222>
            Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
                write!(f, "{}", SchemaDisplay(expr))
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let inlist_name = schema_name_from_exprs(list)?;

                if *negated {
                    write!(f, "{} NOT IN {}", SchemaDisplay(expr), inlist_name)
                } else {
                    write!(f, "{} IN {}", SchemaDisplay(expr), inlist_name)
                }
            }
            Expr::Exists(Exists { negated: true, .. }) => write!(f, "NOT EXISTS"),
            Expr::Exists(Exists { negated: false, .. }) => write!(f, "EXISTS"),
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                write!(f, "GROUPING SETS (")?;
                for exprs in lists_of_exprs.iter() {
                    write!(f, "({})", schema_name_from_exprs(exprs)?)?;
                }
                write!(f, ")")
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::IsNull(expr) => write!(f, "{} IS NULL", SchemaDisplay(expr)),
            Expr::IsNotNull(expr) => {
                write!(f, "{} IS NOT NULL", SchemaDisplay(expr))
            }
            Expr::IsUnknown(expr) => {
                write!(f, "{} IS UNKNOWN", SchemaDisplay(expr))
            }
            Expr::IsNotUnknown(expr) => {
                write!(f, "{} IS NOT UNKNOWN", SchemaDisplay(expr))
            }
            Expr::InSubquery(InSubquery { negated: true, .. }) => {
                write!(f, "NOT IN")
            }
            Expr::InSubquery(InSubquery { negated: false, .. }) => write!(f, "IN"),
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", SchemaDisplay(expr)),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", SchemaDisplay(expr)),
            Expr::IsNotTrue(expr) => {
                write!(f, "{} IS NOT TRUE", SchemaDisplay(expr))
            }
            Expr::IsNotFalse(expr) => {
                write!(f, "{} IS NOT FALSE", SchemaDisplay(expr))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
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
            Expr::Negative(expr) => write!(f, "(- {})", SchemaDisplay(expr)),
            Expr::Not(expr) => write!(f, "NOT {}", SchemaDisplay(expr)),
            Expr::Unnest(Unnest { expr }) => {
                write!(f, "UNNEST({})", SchemaDisplay(expr))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                match func.schema_name(args) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from schema_name {e}")
                    }
                }
            }
            Expr::ScalarSubquery(Subquery { subquery, .. }) => {
                write!(f, "{}", subquery.schema().field(0).name())
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            }) => {
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
            Expr::WindowFunction(window_fun) => {
                let WindowFunction { fun, params } = window_fun.as_ref();
                match fun {
                    WindowFunctionDefinition::AggregateUDF(fun) => {
                        match fun.window_function_schema_name(params) {
                            Ok(name) => {
                                write!(f, "{name}")
                            }
                            Err(e) => {
                                write!(
                                    f,
                                    "got error from window_function_schema_name {e}"
                                )
                            }
                        }
                    }
                    _ => {
                        let WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            null_treatment,
                            distinct,
                        } = params;

                        // Write function name and open parenthesis
                        write!(f, "{fun}(")?;

                        // If DISTINCT, emit the keyword
                        if *distinct {
                            write!(f, "DISTINCT ")?;
                        }

                        // Write the comma‑separated argument list
                        write!(
                            f,
                            "{}",
                            schema_name_from_exprs_comma_separated_without_space(args)?
                        )?;

                        // **Close the argument parenthesis**
                        write!(f, ")")?;

                        if let Some(null_treatment) = null_treatment {
                            write!(f, " {null_treatment}")?;
                        }

                        if !partition_by.is_empty() {
                            write!(
                                f,
                                " PARTITION BY [{}]",
                                schema_name_from_exprs(partition_by)?
                            )?;
                        }

                        if !order_by.is_empty() {
                            write!(
                                f,
                                " ORDER BY [{}]",
                                schema_name_from_sorts(order_by)?
                            )?;
                        };

                        write!(f, " {window_frame}")
                    }
                }
            }
        }
    }
}

/// A helper struct for displaying an `Expr` as an SQL-like string.
struct SqlDisplay<'a>(&'a Expr);

impl Display for SqlDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            Expr::Literal(scalar, _) => scalar.fmt(f),
            Expr::Alias(boxed_alias) => {
                let name = &boxed_alias.as_ref().name;
                write!(f, "{name}")
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(
                        f,
                        "{} NOT BETWEEN {} AND {}",
                        SqlDisplay(expr),
                        SqlDisplay(low),
                        SqlDisplay(high),
                    )
                } else {
                    write!(
                        f,
                        "{} BETWEEN {} AND {}",
                        SqlDisplay(expr),
                        SqlDisplay(low),
                        SqlDisplay(high),
                    )
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                write!(f, "{} {op} {}", SqlDisplay(left), SqlDisplay(right),)
            }
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                write!(f, "CASE ")?;

                if let Some(e) = expr {
                    write!(f, "{} ", SqlDisplay(e))?;
                }

                for (when, then) in when_then_expr {
                    write!(f, "WHEN {} THEN {} ", SqlDisplay(when), SqlDisplay(then),)?;
                }

                if let Some(e) = else_expr {
                    write!(f, "ELSE {} ", SqlDisplay(e))?;
                }

                write!(f, "END")
            }
            Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
                write!(f, "{}", SqlDisplay(expr))
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                write!(
                    f,
                    "{}{} IN {}",
                    SqlDisplay(expr),
                    if *negated { " NOT" } else { "" },
                    ExprListDisplay::comma_separated(list.as_slice())
                )
            }
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                write!(
                    f,
                    "ROLLUP ({})",
                    ExprListDisplay::comma_separated(exprs.as_slice())
                )
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                write!(f, "GROUPING SETS (")?;
                for exprs in lists_of_exprs.iter() {
                    write!(
                        f,
                        "({})",
                        ExprListDisplay::comma_separated(exprs.as_slice())
                    )?;
                }
                write!(f, ")")
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                write!(
                    f,
                    "ROLLUP ({})",
                    ExprListDisplay::comma_separated(exprs.as_slice())
                )
            }
            Expr::IsNull(expr) => write!(f, "{} IS NULL", SqlDisplay(expr)),
            Expr::IsNotNull(expr) => {
                write!(f, "{} IS NOT NULL", SqlDisplay(expr))
            }
            Expr::IsUnknown(expr) => {
                write!(f, "{} IS UNKNOWN", SqlDisplay(expr))
            }
            Expr::IsNotUnknown(expr) => {
                write!(f, "{} IS NOT UNKNOWN", SqlDisplay(expr))
            }
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", SqlDisplay(expr)),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", SqlDisplay(expr)),
            Expr::IsNotTrue(expr) => {
                write!(f, "{} IS NOT TRUE", SqlDisplay(expr))
            }
            Expr::IsNotFalse(expr) => {
                write!(f, "{} IS NOT FALSE", SqlDisplay(expr))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                write!(
                    f,
                    "{} {}{} {}",
                    SqlDisplay(expr),
                    if *negated { "NOT " } else { "" },
                    if *case_insensitive { "ILIKE" } else { "LIKE" },
                    SqlDisplay(pattern),
                )?;

                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::Negative(expr) => write!(f, "(- {})", SqlDisplay(expr)),
            Expr::Not(expr) => write!(f, "NOT {}", SqlDisplay(expr)),
            Expr::Unnest(Unnest { expr }) => {
                write!(f, "UNNEST({})", SqlDisplay(expr))
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            }) => {
                write!(
                    f,
                    "{} {} {}",
                    SqlDisplay(expr),
                    if *negated {
                        "NOT SIMILAR TO"
                    } else {
                        "SIMILAR TO"
                    },
                    SqlDisplay(pattern),
                )?;
                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                match func.human_display(params) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from schema_name {e}")
                    }
                }
            }
            _ => write!(f, "{}", self.0),
        }
    }
}

/// Get schema_name for Vector of expressions
///
/// Internal usage. Please call `schema_name_from_exprs` instead
// TODO: Use ", " to standardize the formatting of Vec<Expr>,
// <https://github.com/apache/datafusion/issues/10364>
pub(crate) fn schema_name_from_exprs_comma_separated_without_space(
    exprs: &[Expr],
) -> Result<String, fmt::Error> {
    schema_name_from_exprs_inner(exprs, ",")
}

/// Formats a list of `&Expr` with a custom separator using SQL display format
pub struct ExprListDisplay<'a> {
    exprs: &'a [Expr],
    sep: &'a str,
}

impl<'a> ExprListDisplay<'a> {
    /// Create a new display struct with the given expressions and separator
    pub fn new(exprs: &'a [Expr], sep: &'a str) -> Self {
        Self { exprs, sep }
    }

    /// Create a new display struct with comma-space separator
    pub fn comma_separated(exprs: &'a [Expr]) -> Self {
        Self::new(exprs, ", ")
    }
}

impl Display for ExprListDisplay<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut first = true;
        for expr in self.exprs {
            if !first {
                write!(f, "{}", self.sep)?;
            }
            write!(f, "{}", SqlDisplay(expr))?;
            first = false;
        }
        Ok(())
    }
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

pub const OUTER_REFERENCE_COLUMN_PREFIX: &str = "outer_ref";
pub const UNNEST_COLUMN_PREFIX: &str = "UNNEST";

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl Display for Expr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Expr::Alias(boxed_alias) => {
                let Alias { expr, name, .. } = boxed_alias.as_ref();
                write!(f, "{expr} AS {name}")
            }
            Expr::Column(c) => write!(f, "{c}"),
            Expr::OuterReferenceColumn(boxed_orc) => {
                let (_, c) = boxed_orc.as_ref();
                write!(f, "{OUTER_REFERENCE_COLUMN_PREFIX}({c})")
            }
            Expr::ScalarVariable(_, var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v, metadata) => {
                match metadata.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                    false => write!(f, "{v:?} {:?}", metadata.as_ref().unwrap()),
                    true => write!(f, "{v:?}"),
                }
            }
            Expr::Case(case) => {
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
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "CAST({expr} AS {data_type:?})")
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                write!(f, "TRY_CAST({expr} AS {data_type:?})")
            }
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::Negative(expr) => write!(f, "(- {expr})"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsUnknown(expr) => write!(f, "{expr} IS UNKNOWN"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::IsNotUnknown(expr) => write!(f, "{expr} IS NOT UNKNOWN"),
            Expr::Exists(Exists {
                subquery,
                negated: true,
            }) => write!(f, "NOT EXISTS ({subquery:?})"),
            Expr::Exists(Exists {
                subquery,
                negated: false,
            }) => write!(f, "EXISTS ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: true,
            }) => write!(f, "{expr} NOT IN ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: false,
            }) => write!(f, "{expr} IN ({subquery:?})"),
            Expr::ScalarSubquery(subquery) => write!(f, "({subquery:?})"),
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::ScalarFunction(fun) => {
                fmt_function(f, fun.name(), false, &fun.args, true)
            }
            // TODO: use udf's display_name, need to fix the separator issue, <https://github.com/apache/datafusion/issues/10364>
            // Expr::ScalarFunction(ScalarFunction { func, args }) => {
            //     write!(f, "{}", func.display_name(args).unwrap())
            // }
            Expr::WindowFunction(window_fun) => {
                let WindowFunction { fun, params } = window_fun.as_ref();
                match fun {
                    WindowFunctionDefinition::AggregateUDF(fun) => {
                        match fun.window_function_display_name(params) {
                            Ok(name) => {
                                write!(f, "{name}")
                            }
                            Err(e) => {
                                write!(
                                    f,
                                    "got error from window_function_display_name {e}"
                                )
                            }
                        }
                    }
                    WindowFunctionDefinition::WindowUDF(fun) => {
                        let WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            null_treatment,
                            distinct,
                        } = params;

                        fmt_function(f, &fun.to_string(), *distinct, args, true)?;

                        if let Some(nt) = null_treatment {
                            write!(f, "{nt}")?;
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
                            window_frame.units,
                            window_frame.start_bound,
                            window_frame.end_bound
                        )
                    }
                }
            }
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                match func.display_name(params) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from display_name {e}")
                    }
                }
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
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
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => {
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
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT IN ([{}])", expr_vec_fmt!(list))
                } else {
                    write!(f, "{expr} IN ([{}])", expr_vec_fmt!(list))
                }
            }
            #[expect(deprecated)]
            Expr::Wildcard { qualifier, options } => match qualifier {
                Some(qualifier) => write!(f, "{qualifier}.*{options}"),
                None => write!(f, "*{options}"),
            },
            Expr::GroupingSet(grouping_sets) => match grouping_sets {
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
            Expr::Placeholder(Placeholder { id, .. }) => write!(f, "{id}"),
            Expr::Unnest(Unnest { expr }) => {
                write!(f, "{UNNEST_COLUMN_PREFIX}({expr})")
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
    match expr {
        Expr::Column(col) => Ok(col.name.clone()),
        Expr::Alias(alias) => Ok(alias.name.clone()),
        _ => Ok(expr.schema_name().to_string()),
    }
}

#[cfg(test)]
mod test {
    use crate::expr_fn::col;
    use crate::{
        case, lit, qualified_wildcard, wildcard, wildcard_with_options, ColumnarValue,
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Volatility,
    };
    use arrow::datatypes::{Field, Schema};
    use sqlparser::ast;
    use sqlparser::ast::{Ident, IdentWithAlias};
    use std::any::Any;

    #[test]
    fn infer_placeholder_in_clause() {
        // SELECT * FROM employees WHERE department_id IN ($1, $2, $3);
        let column = col("department_id");
        let param_placeholders = vec![
            Expr::Placeholder(Placeholder {
                id: "$1".to_string(),
                data_type: None,
            }),
            Expr::Placeholder(Placeholder {
                id: "$2".to_string(),
                data_type: None,
            }),
            Expr::Placeholder(Placeholder {
                id: "$3".to_string(),
                data_type: None,
            }),
        ];
        let in_list = Expr::InList(InList {
            expr: Box::new(column),
            list: param_placeholders,
            negated: false,
        });

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("department_id", DataType::Int32, true),
        ]));
        let df_schema = DFSchema::try_from(schema).unwrap();

        let (inferred_expr, contains_placeholder) =
            in_list.infer_placeholder_types(&df_schema).unwrap();

        assert!(contains_placeholder);

        match inferred_expr {
            Expr::InList(in_list) => {
                for expr in in_list.list {
                    match expr {
                        Expr::Placeholder(placeholder) => {
                            assert_eq!(
                                placeholder.data_type,
                                Some(DataType::Int32),
                                "Placeholder {} should infer Int32",
                                placeholder.id
                            );
                        }
                        _ => panic!("Expected Placeholder expression"),
                    }
                }
            }
            _ => panic!("Expected InList expression"),
        }
    }

    #[test]
    fn infer_placeholder_like_and_similar_to() {
        // name LIKE $1
        let schema =
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let df_schema = DFSchema::try_from(schema).unwrap();

        let like = Like {
            expr: Box::new(col("name")),
            pattern: Box::new(Expr::Placeholder(Placeholder {
                id: "$1".to_string(),
                data_type: None,
            })),
            negated: false,
            case_insensitive: false,
            escape_char: None,
        };

        let expr = Expr::Like(like.clone());

        let (inferred_expr, _) = expr.infer_placeholder_types(&df_schema).unwrap();
        match inferred_expr {
            Expr::Like(like) => match *like.pattern {
                Expr::Placeholder(placeholder) => {
                    assert_eq!(placeholder.data_type, Some(DataType::Utf8));
                }
                _ => panic!("Expected Placeholder"),
            },
            _ => panic!("Expected Like"),
        }

        // name SIMILAR TO $1
        let expr = Expr::SimilarTo(like);

        let (inferred_expr, _) = expr.infer_placeholder_types(&df_schema).unwrap();
        match inferred_expr {
            Expr::SimilarTo(like) => match *like.pattern {
                Expr::Placeholder(placeholder) => {
                    assert_eq!(
                        placeholder.data_type,
                        Some(DataType::Utf8),
                        "Placeholder {} should infer Utf8",
                        placeholder.id
                    );
                }
                _ => panic!("Expected Placeholder expression"),
            },
            _ => panic!("Expected SimilarTo expression"),
        }
    }

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
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Float32(Some(1.23)), None)),
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
            let expr = &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64));
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

    #[test]
    fn test_schema_display_alias_with_relation() {
        assert_eq!(
            format!(
                "{}",
                SchemaDisplay(
                    &lit(1).alias_qualified("table_name".into(), "column_name")
                )
            ),
            "table_name.column_name"
        );
    }

    #[test]
    fn test_schema_display_alias_without_relation() {
        assert_eq!(
            format!(
                "{}",
                SchemaDisplay(&lit(1).alias_qualified(None::<&str>, "column_name"))
            ),
            "column_name"
        );
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

    #[test]
    fn test_size_of_expr() {
        // because Expr is such a widely used struct in DataFusion
        // it is important to keep its size as small as possible
        //
        // If this test fails when you change `Expr`, please try
        // `Box`ing the fields to make `Expr` smaller
        // See https://github.com/apache/datafusion/issues/16199 for details
        assert_eq!(size_of::<Expr>(), 112);
        assert_eq!(size_of::<ScalarValue>(), 64);
        assert_eq!(size_of::<DataType>(), 24); // 3 ptrs
        assert_eq!(size_of::<Vec<Expr>>(), 24);
        assert_eq!(size_of::<Arc<Expr>>(), 8);
    }
}

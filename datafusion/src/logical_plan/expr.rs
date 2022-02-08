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

//! This module provides an `Expr` enum for representing expressions
//! such as `col = 5` or `SUM(col)`. See examples on the [`Expr`] struct.

pub use super::Operator;
use crate::error::{DataFusionError, Result};
use crate::field_util::get_indexed_field;
use crate::logical_plan::{window_frames, DFField, DFSchema};
use crate::physical_plan::functions::Volatility;
use crate::physical_plan::{
    aggregates, expressions::binary_operator_data_type, functions, udf::ScalarUDF,
    window_functions,
};
use crate::{physical_plan::udaf::AggregateUDF, scalar::ScalarValue};
use aggregates::{AccumulatorFunctionImplementation, StateTypeFunction};
use arrow::{compute::can_cast_types, datatypes::DataType};
pub use datafusion_common::{Column, ExprSchema};
use functions::{ReturnTypeFunction, ScalarFunctionImplementation, Signature};
use std::collections::HashSet;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;

/// `Expr` is a central struct of DataFusion's query API, and
/// represent logical expressions such as `A + 1`, or `CAST(c1 AS
/// int)`.
///
/// An `Expr` can compute its [DataType](arrow::datatypes::DataType)
/// and nullability, and has functions for building up complex
/// expressions.
///
/// # Examples
///
/// ## Create an expression `c1` referring to column named "c1"
/// ```
/// # use datafusion::logical_plan::*;
/// let expr = col("c1");
/// assert_eq!(expr, Expr::Column(Column::from_name("c1")));
/// ```
///
/// ## Create the expression `c1 + c2` to add columns "c1" and "c2" together
/// ```
/// # use datafusion::logical_plan::*;
/// let expr = col("c1") + col("c2");
///
/// assert!(matches!(expr, Expr::BinaryExpr { ..} ));
/// if let Expr::BinaryExpr { left, right, op } = expr {
///   assert_eq!(*left, col("c1"));
///   assert_eq!(*right, col("c2"));
///   assert_eq!(op, Operator::Plus);
/// }
/// ```
///
/// ## Create expression `c1 = 42` to compare the value in coumn "c1" to the literal value `42`
/// ```
/// # use datafusion::logical_plan::*;
/// # use datafusion::scalar::*;
/// let expr = col("c1").eq(lit(42));
///
/// assert!(matches!(expr, Expr::BinaryExpr { ..} ));
/// if let Expr::BinaryExpr { left, right, op } = expr {
///   assert_eq!(*left, col("c1"));
///   let scalar = ScalarValue::Int32(Some(42));
///   assert_eq!(*right, Expr::Literal(scalar));
///   assert_eq!(op, Operator::Eq);
/// }
/// ```
#[derive(Clone, PartialEq, Hash)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Box<Expr>, String),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// Whether an expression is not Null. This expression is never null.
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>),
    /// Returns the field of a [`ListArray`] or [`StructArray`] by key
    GetIndexedField {
        /// the expression to take the field from
        expr: Box<Expr>,
        /// The name of the field to take
        key: ScalarValue,
    },
    /// Whether an expression is between a given range.
    Between {
        /// The value to compare
        expr: Box<Expr>,
        /// Whether the expression is negated
        negated: bool,
        /// The low end of the range
        low: Box<Expr>,
        /// The high end of the range
        high: Box<Expr>,
    },
    /// The CASE expression is similar to a series of nested if/else and there are two forms that
    /// can be used. The first form consists of a series of boolean "when" expressions with
    /// corresponding "then" expressions, and an optional "else" expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    ///
    /// The second form uses a base expression and then a series of "when" clauses that match on a
    /// literal value.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    Case {
        /// Optional base expression that can be compared to literal values in the "when" expressions
        expr: Option<Box<Expr>>,
        /// One or more when/then expressions
        when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
        /// Optional "else" expression
        else_expr: Option<Box<Expr>>,
    },
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// Casts the expression to a given type and will return a null value if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    TryCast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expr>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// Represents the call of a built-in scalar function with a set of arguments.
    ScalarFunction {
        /// The function
        fun: functions::BuiltinScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of a user-defined scalar function with arguments.
    ScalarUDF {
        /// The function
        fun: Arc<ScalarUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction {
        /// Name of the function
        fun: aggregates::AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Whether this is a DISTINCT aggregation or not
        distinct: bool,
    },
    /// Represents the call of a window function with arguments.
    WindowFunction {
        /// Name of the function
        fun: window_functions::WindowFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// List of partition by expressions
        partition_by: Vec<Expr>,
        /// List of order by expressions
        order_by: Vec<Expr>,
        /// Window frame
        window_frame: Option<window_frames::WindowFrame>,
    },
    /// aggregate function
    AggregateUDF {
        /// The function
        fun: Arc<AggregateUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Returns whether the list contains the expr value.
    InList {
        /// The expression to compare
        expr: Box<Expr>,
        /// A list of values to compare against
        list: Vec<Expr>,
        /// Whether the expression is negated
        negated: bool,
    },
    /// Represents a reference to all fields in a schema.
    Wildcard,
}

/// Fixed seed for the hashing so that Ords are consistent across runs
const SEED: ahash::RandomState = ahash::RandomState::with_seeds(0, 0, 0, 0);

impl PartialOrd for Expr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let mut hasher = SEED.build_hasher();
        self.hash(&mut hasher);
        let s = hasher.finish();

        let mut hasher = SEED.build_hasher();
        other.hash(&mut hasher);
        let o = hasher.finish();

        Some(s.cmp(&o))
    }
}

impl Expr {
    /// Returns the [arrow::datatypes::DataType] of the expression
    /// based on [ExprSchema]
    ///
    /// Note: [DFSchema] implements [ExprSchema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// [arrow::datatypes::DataType].  This happens when e.g. the
    /// expression refers to a column that does not exist in the
    /// schema, or when the expression is incorrectly typed
    /// (e.g. `[utf8] + [bool]`).
    pub fn get_type<S: ExprSchema>(&self, schema: &S) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) | Expr::Sort { expr, .. } | Expr::Negative(expr) => {
                expr.get_type(schema)
            }
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::ScalarVariable(_) => Ok(DataType::Utf8),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Case { when_then_expr, .. } => when_then_expr[0].1.get_type(schema),
            Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
                Ok(data_type.clone())
            }
            Expr::ScalarUDF { fun, args } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
            }
            Expr::ScalarFunction { fun, args } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                functions::return_type(fun, &data_types)
            }
            Expr::WindowFunction { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                window_functions::return_type(fun, &data_types)
            }
            Expr::AggregateFunction { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                aggregates::return_type(fun, &data_types)
            }
            Expr::AggregateUDF { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
            }
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => binary_operator_data_type(
                &left.get_type(schema)?,
                op,
                &right.get_type(schema)?,
            ),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
            Expr::GetIndexedField { ref expr, key } => {
                let data_type = expr.get_type(schema)?;

                get_indexed_field(&data_type, key).map(|x| x.data_type().clone())
            }
        }
    }

    /// Returns the nullability of the expression based on [ExprSchema].
    ///
    /// Note: [DFSchema] implements [ExprSchema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// nullability.  This happens when the expression refers to a
    /// column that does not exist in the schema.
    pub fn nullable<S: ExprSchema>(&self, input_schema: &S) -> Result<bool> {
        match self {
            Expr::Alias(expr, _)
            | Expr::Not(expr)
            | Expr::Negative(expr)
            | Expr::Sort { expr, .. }
            | Expr::Between { expr, .. }
            | Expr::InList { expr, .. } => expr.nullable(input_schema),
            Expr::Column(c) => input_schema.nullable(c),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::Case {
                when_then_expr,
                else_expr,
                ..
            } => {
                // this expression is nullable if any of the input expressions are nullable
                let then_nullable = when_then_expr
                    .iter()
                    .map(|(_, t)| t.nullable(input_schema))
                    .collect::<Result<Vec<_>>>()?;
                if then_nullable.contains(&true) {
                    Ok(true)
                } else if let Some(e) = else_expr {
                    e.nullable(input_schema)
                } else {
                    Ok(false)
                }
            }
            Expr::Cast { expr, .. } => expr.nullable(input_schema),
            Expr::ScalarVariable(_)
            | Expr::TryCast { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarUDF { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. } => Ok(true),
            Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(false),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ..
            } => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
            Expr::GetIndexedField { ref expr, key } => {
                let data_type = expr.get_type(input_schema)?;
                get_indexed_field(&data_type, key).map(|x| x.is_nullable())
            }
        }
    }

    /// Returns the name of this expression based on [crate::logical_plan::DFSchema].
    ///
    /// This represents how a column with this expression is named when no alias is chosen
    pub fn name(&self, input_schema: &DFSchema) -> Result<String> {
        create_name(self, input_schema)
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    pub fn to_field(&self, input_schema: &DFSchema) -> Result<DFField> {
        match self {
            Expr::Column(c) => Ok(DFField::new(
                c.relation.as_deref(),
                &c.name,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
            _ => Ok(DFField::new(
                None,
                &self.name(input_schema)?,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
        }
    }

    /// Wraps this expression in a cast to a target [arrow::datatypes::DataType].
    ///
    /// # Errors
    ///
    /// This function errors when it is impossible to cast the
    /// expression to the target [arrow::datatypes::DataType].
    pub fn cast_to<S: ExprSchema>(
        self,
        cast_to_type: &DataType,
        schema: &S,
    ) -> Result<Expr> {
        // TODO(kszucs): most of the operations do not validate the type correctness
        // like all of the binary expressions below. Perhaps Expr should track the
        // type of the expression?
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            Ok(self)
        } else if can_cast_types(&this_type, cast_to_type) {
            Ok(Expr::Cast {
                expr: Box::new(self),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(DataFusionError::Plan(format!(
                "Cannot automatically convert {:?} to {:?}",
                this_type, cast_to_type
            )))
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

    /// Return `!self`
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        !self
    }

    /// Calculate the modulus of two expressions.
    /// Return `self % other`
    pub fn modulus(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Modulo, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Like, other)
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        binary_expr(self, Operator::NotLike, other)
    }

    /// Return `self AS name` alias expression
    pub fn alias(self, name: &str) -> Expr {
        Expr::Alias(Box::new(self), name.to_owned())
    }

    /// Return `self IN <list>` if `negated` is false, otherwise
    /// return `self NOT IN <list>`.a
    pub fn in_list(self, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated,
        }
    }

    /// Return `IsNull(Box(self))
    #[allow(clippy::wrong_self_convention)]
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    #[allow(clippy::wrong_self_convention)]
    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    /// Create a sort expression from an existing expression.
    ///
    /// ```
    /// # use datafusion::logical_plan::col;
    /// let sort_expr = col("foo").sort(true, true); // SORT ASC NULLS_FIRST
    /// ```
    pub fn sort(self, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort {
            expr: Box::new(self),
            asc,
            nulls_first,
        }
    }
}

impl Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Expr::Not(Box::new(self))
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => write!(f, "{} {} {}", left, op, right),
            Expr::AggregateFunction {
                /// Name of the function
                ref fun,
                /// List of expressions to feed to the functions as arguments
                ref args,
                /// Whether this is a DISTINCT aggregation or not
                ref distinct,
            } => fmt_function(f, &fun.to_string(), *distinct, args, true),
            Expr::ScalarFunction {
                /// Name of the function
                ref fun,
                /// List of expressions to feed to the functions as arguments
                ref args,
            } => fmt_function(f, &fun.to_string(), false, args, true),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// Helper struct for building [Expr::Case]
pub struct CaseBuilder {
    expr: Option<Box<Expr>>,
    when_expr: Vec<Expr>,
    then_expr: Vec<Expr>,
    else_expr: Option<Box<Expr>>,
}

impl CaseBuilder {
    pub fn when(&mut self, when: Expr, then: Expr) -> CaseBuilder {
        self.when_expr.push(when);
        self.then_expr.push(then);
        CaseBuilder {
            expr: self.expr.clone(),
            when_expr: self.when_expr.clone(),
            then_expr: self.then_expr.clone(),
            else_expr: self.else_expr.clone(),
        }
    }
    pub fn otherwise(&mut self, else_expr: Expr) -> Result<Expr> {
        self.else_expr = Some(Box::new(else_expr));
        self.build()
    }

    pub fn end(&self) -> Result<Expr> {
        self.build()
    }
}

impl CaseBuilder {
    fn build(&self) -> Result<Expr> {
        // collect all "then" expressions
        let mut then_expr = self.then_expr.clone();
        if let Some(e) = &self.else_expr {
            then_expr.push(e.as_ref().to_owned());
        }

        let then_types: Vec<DataType> = then_expr
            .iter()
            .map(|e| match e {
                Expr::Literal(_) => e.get_type(&DFSchema::empty()),
                _ => Ok(DataType::Null),
            })
            .collect::<Result<Vec<_>>>()?;

        if then_types.contains(&DataType::Null) {
            // cannot verify types until execution type
        } else {
            let unique_types: HashSet<&DataType> = then_types.iter().collect();
            if unique_types.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "CASE expression 'then' values had multiple data types: {:?}",
                    unique_types
                )));
            }
        }

        Ok(Expr::Case {
            expr: self.expr.clone(),
            when_then_expr: self
                .when_expr
                .iter()
                .zip(self.then_expr.iter())
                .map(|(w, t)| (Box::new(w.clone()), Box::new(t.clone())))
                .collect(),
            else_expr: self.else_expr.clone(),
        })
    }
}

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
pub fn case(expr: Expr) -> CaseBuilder {
    CaseBuilder {
        expr: Some(Box::new(expr)),
        when_expr: vec![],
        then_expr: vec![],
        else_expr: None,
    }
}

/// Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
pub fn when(when: Expr, then: Expr) -> CaseBuilder {
    CaseBuilder {
        expr: None,
        when_expr: vec![when],
        then_expr: vec![then],
        else_expr: None,
    }
}

/// return a new expression l <op> r
pub fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    }
}

/// Combines an array of filter expressions into a single filter expression
/// consisting of the input filter expressions joined with logical AND.
/// Returns None if the filters array is empty.
pub fn combine_filters(filters: &[Expr]) -> Option<Expr> {
    if filters.is_empty() {
        return None;
    }
    let combined_filter = filters
        .iter()
        .skip(1)
        .fold(filters[0].clone(), |acc, filter| and(acc, filter.clone()));
    Some(combined_filter)
}

/// return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::Or,
        right: Box::new(right),
    }
}

/// Create a column expression based on a qualified or unqualified column name
pub fn col(ident: &str) -> Expr {
    Expr::Column(ident.into())
}

/// Convert an expression into Column expression if it's already provided as input plan.
///
/// For example, it rewrites:
///
/// ```ignore
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), sum(col("c2"))?
/// ```
///
/// Into:
///
/// ```ignore
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), col("SUM(#c2)")?
/// ```
pub fn columnize_expr(e: Expr, input_schema: &DFSchema) -> Expr {
    match e {
        Expr::Column(_) => e,
        Expr::Alias(inner_expr, name) => {
            Expr::Alias(Box::new(columnize_expr(*inner_expr, input_schema)), name)
        }
        _ => match e.name(input_schema) {
            Ok(name) => match input_schema.field_with_unqualified_name(&name) {
                Ok(field) => Expr::Column(field.qualified_column()),
                // expression not provided as input, do not convert to a column reference
                Err(_) => e,
            },
            Err(_) => e,
        },
    }
}

/// Recursively un-alias an expressions
#[inline]
pub fn unalias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(sub_expr, _) => unalias(*sub_expr),
        _ => expr,
    }
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Min,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Max,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Sum,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Avg,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Count,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the count(distinct) aggregate function
pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Count,
        distinct: true,
        args: vec![expr],
    }
}

/// Create an in_list expression
pub fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList {
        expr: Box::new(expr),
        list,
        negated,
    }
}

/// Trait for converting a type to a [`Literal`] literal expression.
pub trait Literal {
    /// convert the value to a Literal expression
    fn lit(&self) -> Expr;
}

/// Trait for converting a type to a literal timestamp
pub trait TimestampLiteral {
    fn lit_timestamp_nano(&self) -> Expr;
}

impl Literal for &str {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for Vec<u8> {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some((*self).to_owned())))
    }
}

impl Literal for &[u8] {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some((*self).to_owned())))
    }
}

impl Literal for ScalarValue {
    fn lit(&self) -> Expr {
        Expr::Literal(self.clone())
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident, $DOC: expr) => {
        #[doc = $DOC]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(ScalarValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

macro_rules! make_timestamp_literal {
    ($TYPE:ty, $SCALAR:ident, $DOC: expr) => {
        #[doc = $DOC]
        impl TimestampLiteral for $TYPE {
            fn lit_timestamp_nano(&self) -> Expr {
                Expr::Literal(ScalarValue::TimestampNanosecond(
                    Some((self.clone()).into()),
                    None,
                ))
            }
        }
    };
}

make_literal!(bool, Boolean, "literal expression containing a bool");
make_literal!(f32, Float32, "literal expression containing an f32");
make_literal!(f64, Float64, "literal expression containing an f64");
make_literal!(i8, Int8, "literal expression containing an i8");
make_literal!(i16, Int16, "literal expression containing an i16");
make_literal!(i32, Int32, "literal expression containing an i32");
make_literal!(i64, Int64, "literal expression containing an i64");
make_literal!(u8, UInt8, "literal expression containing a u8");
make_literal!(u16, UInt16, "literal expression containing a u16");
make_literal!(u32, UInt32, "literal expression containing a u32");
make_literal!(u64, UInt64, "literal expression containing a u64");

make_timestamp_literal!(i8, Int8, "literal expression containing an i8");
make_timestamp_literal!(i16, Int16, "literal expression containing an i16");
make_timestamp_literal!(i32, Int32, "literal expression containing an i32");
make_timestamp_literal!(i64, Int64, "literal expression containing an i64");
make_timestamp_literal!(u8, UInt8, "literal expression containing a u8");
make_timestamp_literal!(u16, UInt16, "literal expression containing a u16");
make_timestamp_literal!(u32, UInt32, "literal expression containing a u32");

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> Expr {
    n.lit()
}

/// Create a literal timestamp expression
pub fn lit_timestamp_nano<T: TimestampLiteral>(n: T) -> Expr {
    n.lit_timestamp_nano()
}

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
pub fn concat(args: &[Expr]) -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Concat,
        args: args.to_vec(),
    }
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator string, and should not be NULL.
/// Other NULL arguments are ignored.
pub fn concat_ws(sep: impl Into<String>, values: &[Expr]) -> Expr {
    let mut args = vec![lit(sep.into())];
    args.extend_from_slice(values);
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::ConcatWithSeparator,
        args,
    }
}

/// Returns a random value in the range 0.0 <= x < 1.0
pub fn random() -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Random,
        args: vec![],
    }
}

/// Returns the approximate number of distinct input values.
/// This function provides an approximation of count(DISTINCT x).
/// Zero is returned if all input values are null.
/// This function should produce a standard error of 0.81%,
/// which is the standard deviation of the (approximately normal)
/// error distribution over all possible sets.
/// It does not guarantee an upper bound on the error for any specific input set.
pub fn approx_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::ApproxDistinct,
        distinct: false,
        args: vec![expr],
    }
}

/// Calculate an approximation of the specified `percentile` for `expr`.
pub fn approx_percentile_cont(expr: Expr, percentile: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::ApproxPercentileCont,
        distinct: false,
        args: vec![expr, percentile],
    }
}

// TODO(kszucs): this seems buggy, unary_scalar_expr! is used for many
// varying arity functions
/// Create an convenience function representing a unary scalar function
macro_rules! unary_scalar_expr {
    ($ENUM:ident, $FUNC:ident) => {
        #[doc = concat!("Unary scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC(e: Expr) -> Expr {
            Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::$ENUM,
                args: vec![e],
            }
        }
    };
}

macro_rules! scalar_expr {
    ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
        #[doc = concat!("Scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC($($arg: Expr),*) -> Expr {
            Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::$ENUM,
                args: vec![$($arg),*],
            }
        }
    };
}

macro_rules! nary_scalar_expr {
    ($ENUM:ident, $FUNC:ident) => {
        #[doc = concat!("Scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC(args: Vec<Expr>) -> Expr {
            Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::$ENUM,
                args,
            }
        }
    };
}

// generate methods for creating the supported unary/binary expressions

// math functions
unary_scalar_expr!(Sqrt, sqrt);
unary_scalar_expr!(Sin, sin);
unary_scalar_expr!(Cos, cos);
unary_scalar_expr!(Tan, tan);
unary_scalar_expr!(Asin, asin);
unary_scalar_expr!(Acos, acos);
unary_scalar_expr!(Atan, atan);
unary_scalar_expr!(Floor, floor);
unary_scalar_expr!(Ceil, ceil);
unary_scalar_expr!(Now, now);
unary_scalar_expr!(Round, round);
unary_scalar_expr!(Trunc, trunc);
unary_scalar_expr!(Abs, abs);
unary_scalar_expr!(Signum, signum);
unary_scalar_expr!(Exp, exp);
unary_scalar_expr!(Log2, log2);
unary_scalar_expr!(Log10, log10);
unary_scalar_expr!(Ln, ln);

// string functions
scalar_expr!(Ascii, ascii, string);
scalar_expr!(BitLength, bit_length, string);
nary_scalar_expr!(Btrim, btrim);
scalar_expr!(CharacterLength, character_length, string);
scalar_expr!(CharacterLength, length, string);
scalar_expr!(Chr, chr, string);
scalar_expr!(Digest, digest, string, algorithm);
scalar_expr!(InitCap, initcap, string);
scalar_expr!(Left, left, string, count);
scalar_expr!(Lower, lower, string);
nary_scalar_expr!(Lpad, lpad);
scalar_expr!(Ltrim, ltrim, string);
scalar_expr!(MD5, md5, string);
scalar_expr!(OctetLength, octet_length, string);
nary_scalar_expr!(RegexpMatch, regexp_match);
nary_scalar_expr!(RegexpReplace, regexp_replace);
scalar_expr!(Replace, replace, string, from, to);
scalar_expr!(Repeat, repeat, string, count);
scalar_expr!(Reverse, reverse, string);
scalar_expr!(Right, right, string, count);
nary_scalar_expr!(Rpad, rpad);
scalar_expr!(Rtrim, rtrim, string);
scalar_expr!(SHA224, sha224, string);
scalar_expr!(SHA256, sha256, string);
scalar_expr!(SHA384, sha384, string);
scalar_expr!(SHA512, sha512, string);
scalar_expr!(SplitPart, split_part, expr, delimiter, index);
scalar_expr!(StartsWith, starts_with, string, characters);
scalar_expr!(Strpos, strpos, string, substring);
scalar_expr!(Substr, substr, string, position);
scalar_expr!(ToHex, to_hex, string);
scalar_expr!(Translate, translate, string, from, to);
scalar_expr!(Trim, trim, string);
scalar_expr!(Upper, upper, string);

// date functions
scalar_expr!(DatePart, date_part, part, date);
scalar_expr!(DateTrunc, date_trunc, part, date);

/// returns an array of fixed size with each argument on it.
pub fn array(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Array,
        args,
    }
}

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
#[allow(clippy::rc_buffer)]
pub fn create_udaf(
    name: &str,
    input_type: DataType,
    return_type: Arc<DataType>,
    volatility: Volatility,
    accumulator: AccumulatorFunctionImplementation,
    state_type: Arc<Vec<DataType>>,
) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));
    AggregateUDF::new(
        name,
        &Signature::exact(vec![input_type], volatility),
        &return_type,
        &accumulator,
        &state_type,
    )
}

fn fmt_function(
    f: &mut fmt::Formatter,
    fun: &str,
    distinct: bool,
    args: &[Expr],
    display: bool,
) -> fmt::Result {
    let args: Vec<String> = match display {
        true => args.iter().map(|arg| format!("{}", arg)).collect(),
        false => args.iter().map(|arg| format!("{:?}", arg)).collect(),
    };

    // let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{:?} AS {}", expr, alias),
            Expr::Column(c) => write!(f, "{}", c),
            Expr::ScalarVariable(var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
                ..
            } => {
                write!(f, "CASE ")?;
                if let Some(e) = expr {
                    write!(f, "{:?} ", e)?;
                }
                for (w, t) in when_then_expr {
                    write!(f, "WHEN {:?} THEN {:?} ", w, t)?;
                }
                if let Some(e) = else_expr {
                    write!(f, "ELSE {:?} ", e)?;
                }
                write!(f, "END")
            }
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST({:?} AS {:?})", expr, data_type)
            }
            Expr::TryCast { expr, data_type } => {
                write!(f, "TRY_CAST({:?} AS {:?})", expr, data_type)
            }
            Expr::Not(expr) => write!(f, "NOT {:?}", expr),
            Expr::Negative(expr) => write!(f, "(- {:?})", expr),
            Expr::IsNull(expr) => write!(f, "{:?} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{:?} IS NOT NULL", expr),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "{:?} {} {:?}", left, op, right)
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                if *asc {
                    write!(f, "{:?} ASC", expr)?;
                } else {
                    write!(f, "{:?} DESC", expr)?;
                }
                if *nulls_first {
                    write!(f, " NULLS FIRST")
                } else {
                    write!(f, " NULLS LAST")
                }
            }
            Expr::ScalarFunction { fun, args, .. } => {
                fmt_function(f, &fun.to_string(), false, args, false)
            }
            Expr::ScalarUDF { fun, ref args, .. } => {
                fmt_function(f, &fun.name, false, args, false)
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                fmt_function(f, &fun.to_string(), false, args, false)?;
                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY {:?}", partition_by)?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {:?}", order_by)?;
                }
                if let Some(window_frame) = window_frame {
                    write!(
                        f,
                        " {} BETWEEN {} AND {}",
                        window_frame.units,
                        window_frame.start_bound,
                        window_frame.end_bound
                    )?;
                }
                Ok(())
            }
            Expr::AggregateFunction {
                fun,
                distinct,
                ref args,
                ..
            } => fmt_function(f, &fun.to_string(), *distinct, args, true),
            Expr::AggregateUDF { fun, ref args, .. } => {
                fmt_function(f, &fun.name, false, args, false)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    write!(f, "{:?} NOT BETWEEN {:?} AND {:?}", expr, low, high)
                } else {
                    write!(f, "{:?} BETWEEN {:?} AND {:?}", expr, low, high)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if *negated {
                    write!(f, "{:?} NOT IN ({:?})", expr, list)
                } else {
                    write!(f, "{:?} IN ({:?})", expr, list)
                }
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::GetIndexedField { ref expr, key } => {
                write!(f, "({:?})[{}]", expr, key)
            }
        }
    }
}

fn create_function_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
    input_schema: &DFSchema,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_name(e, input_schema))
        .collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

/// Returns a readable name of an expression based on the input schema.
/// This function recursively transverses the expression for names such as "CAST(a > 2)".
fn create_name(e: &Expr, input_schema: &DFSchema) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(c) => Ok(c.flat_name()),
        Expr::ScalarVariable(variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_name(left, input_schema)?;
            let right = create_name(right, input_schema)?;
            Ok(format!("{} {} {}", left, op, right))
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                let e = create_name(e, input_schema)?;
                name += &format!("{} ", e);
            }
            for (w, t) in when_then_expr {
                let when = create_name(w, input_schema)?;
                let then = create_name(t, input_schema)?;
                name += &format!("WHEN {} THEN {} ", when, then);
            }
            if let Some(e) = else_expr {
                let e = create_name(e, input_schema)?;
                name += &format!("ELSE {} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("CAST({} AS {:?})", expr, data_type))
        }
        Expr::TryCast { expr, data_type } => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("TRY_CAST({} AS {:?})", expr, data_type))
        }
        Expr::Not(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("NOT {}", expr))
        }
        Expr::Negative(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("(- {})", expr))
        }
        Expr::IsNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NOT NULL", expr))
        }
        Expr::GetIndexedField { expr, key } => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{}[{}]", expr, key))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_name(&fun.to_string(), false, args, input_schema)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_name(&fun.name, false, args, input_schema)
        }
        Expr::WindowFunction {
            fun,
            args,
            window_frame,
            partition_by,
            order_by,
        } => {
            let mut parts: Vec<String> = vec![create_function_name(
                &fun.to_string(),
                false,
                args,
                input_schema,
            )?];
            if !partition_by.is_empty() {
                parts.push(format!("PARTITION BY {:?}", partition_by));
            }
            if !order_by.is_empty() {
                parts.push(format!("ORDER BY {:?}", order_by));
            }
            if let Some(window_frame) = window_frame {
                parts.push(format!("{}", window_frame));
            }
            Ok(parts.join(" "))
        }
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => create_function_name(&fun.to_string(), *distinct, args, input_schema),
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e, input_schema)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = create_name(expr, input_schema)?;
            let list = list.iter().map(|expr| create_name(expr, input_schema));
            if *negated {
                Ok(format!("{} NOT IN ({:?})", expr, list))
            } else {
                Ok(format!("{} IN ({:?})", expr, list))
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = create_name(expr, input_schema)?;
            let low = create_name(low, input_schema)?;
            let high = create_name(high, input_schema)?;
            if *negated {
                Ok(format!("{} NOT BETWEEN {} AND {}", expr, low, high))
            } else {
                Ok(format!("{} BETWEEN {} AND {}", expr, low, high))
            }
        }
        Expr::Sort { .. } => Err(DataFusionError::Internal(
            "Create name does not support sort expression".to_string(),
        )),
        Expr::Wildcard => Err(DataFusionError::Internal(
            "Create name does not support wildcard".to_string(),
        )),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields<'a>(
    expr: impl IntoIterator<Item = &'a Expr>,
    input_schema: &DFSchema,
) -> Result<Vec<DFField>> {
    expr.into_iter().map(|e| e.to_field(input_schema)).collect()
}

/// Calls a named built in function
/// ```
/// use datafusion::logical_plan::*;
///
/// // create the expression sin(x) < 0.2
/// let expr = call_fn("sin", vec![col("x")]).unwrap().lt(lit(0.2));
/// ```
pub fn call_fn(name: impl AsRef<str>, args: Vec<Expr>) -> Result<Expr> {
    match name.as_ref().parse::<functions::BuiltinScalarFunction>() {
        Ok(fun) => Ok(Expr::ScalarFunction { fun, args }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{col, lit, when};
    use super::*;

    #[test]
    fn case_when_same_literal_then_types() -> Result<()> {
        let _ = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit(212))
            .end()?;
        Ok(())
    }

    #[test]
    fn case_when_different_literal_then_types() {
        let maybe_expr = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit("212"))
            .end();
        assert!(maybe_expr.is_err());
    }

    #[test]
    fn test_lit_timestamp_nano() {
        let expr = col("time").eq(lit_timestamp_nano(10)); // 10 is an implicit i32
        let expected =
            col("time").eq(lit(ScalarValue::TimestampNanosecond(Some(10), None)));
        assert_eq!(expr, expected);

        let i: i64 = 10;
        let expr = col("time").eq(lit_timestamp_nano(i));
        assert_eq!(expr, expected);

        let i: u32 = 10;
        let expr = col("time").eq(lit_timestamp_nano(i));
        assert_eq!(expr, expected);
    }

    #[test]
    fn filter_is_null_and_is_not_null() {
        let col_null = col("col1");
        let col_not_null = col("col2");
        assert_eq!(format!("{:?}", col_null.is_null()), "#col1 IS NULL");
        assert_eq!(
            format!("{:?}", col_not_null.is_not_null()),
            "#col2 IS NOT NULL"
        );
    }

    #[test]
    fn test_not() {
        assert_eq!(lit(1).not(), !lit(1));
    }

    macro_rules! test_unary_scalar_expr {
        ($ENUM:ident, $FUNC:ident) => {{
            if let Expr::ScalarFunction { fun, args } = $FUNC(col("tableA.a")) {
                let name = functions::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(1, args.len());
            } else {
                assert!(false, "unexpected");
            }
        }};
    }

    macro_rules! test_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                $(
                    col(stringify!($arg.to_string()))
                ),*
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = functions::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    macro_rules! test_nary_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                vec![
                    $(
                        col(stringify!($arg.to_string()))
                    ),*
                ]
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = functions::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    #[test]
    fn digest_function_definitions() {
        if let Expr::ScalarFunction { fun, args } = digest(col("tableA.a"), lit("md5")) {
            let name = functions::BuiltinScalarFunction::Digest;
            assert_eq!(name, fun);
            assert_eq!(2, args.len());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn scalar_function_definitions() {
        test_unary_scalar_expr!(Sqrt, sqrt);
        test_unary_scalar_expr!(Sin, sin);
        test_unary_scalar_expr!(Cos, cos);
        test_unary_scalar_expr!(Tan, tan);
        test_unary_scalar_expr!(Asin, asin);
        test_unary_scalar_expr!(Acos, acos);
        test_unary_scalar_expr!(Atan, atan);
        test_unary_scalar_expr!(Floor, floor);
        test_unary_scalar_expr!(Ceil, ceil);
        test_unary_scalar_expr!(Now, now);
        test_unary_scalar_expr!(Round, round);
        test_unary_scalar_expr!(Trunc, trunc);
        test_unary_scalar_expr!(Abs, abs);
        test_unary_scalar_expr!(Signum, signum);
        test_unary_scalar_expr!(Exp, exp);
        test_unary_scalar_expr!(Log2, log2);
        test_unary_scalar_expr!(Log10, log10);
        test_unary_scalar_expr!(Ln, ln);

        test_scalar_expr!(Ascii, ascii, input);
        test_scalar_expr!(BitLength, bit_length, string);
        test_nary_scalar_expr!(Btrim, btrim, string);
        test_nary_scalar_expr!(Btrim, btrim, string, characters);
        test_scalar_expr!(CharacterLength, character_length, string);
        test_scalar_expr!(CharacterLength, length, string);
        test_scalar_expr!(Chr, chr, string);
        test_scalar_expr!(Digest, digest, string, algorithm);
        test_scalar_expr!(InitCap, initcap, string);
        test_scalar_expr!(Left, left, string, count);
        test_scalar_expr!(Lower, lower, string);
        test_nary_scalar_expr!(Lpad, lpad, string, count);
        test_nary_scalar_expr!(Lpad, lpad, string, count, characters);
        test_scalar_expr!(Ltrim, ltrim, string);
        test_scalar_expr!(MD5, md5, string);
        test_scalar_expr!(OctetLength, octet_length, string);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern, flags);
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement
        );
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement,
            flags
        );
        test_scalar_expr!(Replace, replace, string, from, to);
        test_scalar_expr!(Repeat, repeat, string, count);
        test_scalar_expr!(Reverse, reverse, string);
        test_scalar_expr!(Right, right, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count, characters);
        test_scalar_expr!(Rtrim, rtrim, string);
        test_scalar_expr!(SHA224, sha224, string);
        test_scalar_expr!(SHA256, sha256, string);
        test_scalar_expr!(SHA384, sha384, string);
        test_scalar_expr!(SHA512, sha512, string);
        test_scalar_expr!(SplitPart, split_part, expr, delimiter, index);
        test_scalar_expr!(StartsWith, starts_with, string, characters);
        test_scalar_expr!(Strpos, strpos, string, substring);
        test_scalar_expr!(Substr, substr, string, position);
        test_scalar_expr!(ToHex, to_hex, string);
        test_scalar_expr!(Translate, translate, string, from, to);
        test_scalar_expr!(Trim, trim, string);
        test_scalar_expr!(Upper, upper, string);

        test_scalar_expr!(DatePart, date_part, part, date);
        test_scalar_expr!(DateTrunc, date_trunc, part, date);
    }

    #[test]
    fn test_partial_ord() {
        // Test validates that partial ord is defined for Expr using hashes, not
        // intended to exhaustively test all possibilities
        let exp1 = col("a") + lit(1);
        let exp2 = col("a") + lit(2);
        let exp3 = !(col("a") + lit(2));

        assert!(exp1 < exp2);
        assert!(exp2 > exp1);
        assert!(exp2 > exp3);
        assert!(exp3 < exp2);
    }

    #[test]
    fn combine_zero_filters() {
        let result = combine_filters(&[]);
        assert_eq!(result, None);
    }

    #[test]
    fn combine_one_filter() {
        let filter = binary_expr(col("c1"), Operator::Lt, lit(1));
        let result = combine_filters(&[filter.clone()]);
        assert_eq!(result, Some(filter));
    }

    #[test]
    fn combine_multiple_filters() {
        let filter1 = binary_expr(col("c1"), Operator::Lt, lit(1));
        let filter2 = binary_expr(col("c2"), Operator::Lt, lit(2));
        let filter3 = binary_expr(col("c3"), Operator::Lt, lit(3));
        let result =
            combine_filters(&[filter1.clone(), filter2.clone(), filter3.clone()]);
        assert_eq!(result, Some(and(and(filter1, filter2), filter3)));
    }

    #[test]
    fn expr_schema_nullability() {
        let expr = col("foo").eq(lit(1));
        assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        assert!(expr
            .nullable(&MockExprSchema::new().with_nullable(true))
            .unwrap());
    }

    #[test]
    fn expr_schema_data_type() {
        let expr = col("foo");
        assert_eq!(
            DataType::Utf8,
            expr.get_type(&MockExprSchema::new().with_data_type(DataType::Utf8))
                .unwrap()
        );
    }

    struct MockExprSchema {
        nullable: bool,
        data_type: DataType,
    }

    impl MockExprSchema {
        fn new() -> Self {
            Self {
                nullable: false,
                data_type: DataType::Null,
            }
        }

        fn with_nullable(mut self, nullable: bool) -> Self {
            self.nullable = nullable;
            self
        }

        fn with_data_type(mut self, data_type: DataType) -> Self {
            self.data_type = data_type;
            self
        }
    }

    impl ExprSchema for MockExprSchema {
        fn nullable(&self, _col: &Column) -> Result<bool> {
            Ok(self.nullable)
        }

        fn data_type(&self, _col: &Column) -> Result<&DataType> {
            Ok(&self.data_type)
        }
    }
}

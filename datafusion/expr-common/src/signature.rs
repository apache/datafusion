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

//! Signature module contains foundational types that are used to represent signatures, types,
//! and return types of functions in DataFusion.

use std::fmt::Display;
use std::hash::Hash;

use crate::type_coercion::aggregates::NUMERICS;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::internal_err;
use datafusion_common::types::{LogicalType, LogicalTypeRef, NativeType};
use datafusion_common::utils::ListCoercion;
use indexmap::IndexSet;
use itertools::Itertools;

/// Constant that is used as a placeholder for any valid timezone.
/// This is used where a function can accept a timestamp type with any
/// valid timezone, it exists to avoid the need to enumerate all possible
/// timezones. See [`TypeSignature`] for more details.
///
/// Type coercion always ensures that functions will be executed using
/// timestamp arrays that have a valid time zone. Functions must never
/// return results with this timezone.
pub const TIMEZONE_WILDCARD: &str = "+TZ";

/// Constant that is used as a placeholder for any valid fixed size list.
/// This is used where a function can accept a fixed size list type with any
/// valid length. It exists to avoid the need to enumerate all possible fixed size list lengths.
pub const FIXED_SIZE_LIST_WILDCARD: i32 = i32::MIN;

/// A function's volatility, which defines the functions eligibility for certain optimizations
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Volatility {
    /// An immutable function will always return the same output when given the same
    /// input. DataFusion will attempt to inline immutable functions during planning.
    Immutable,
    /// A stable function may return different values given the same input across different
    /// queries but must return the same value for a given input within a query. An example of
    /// this is the `Now` function. DataFusion will attempt to inline `Stable` functions
    /// during planning, when possible.
    /// For query `select col1, now() from t1`, it might take a while to execute but
    /// `now()` column will be the same for each output row, which is evaluated
    /// during planning.
    Stable,
    /// A volatile function may change the return value from evaluation to evaluation.
    /// Multiple invocations of a volatile function may return different results when used in the
    /// same query. An example of this is the random() function. DataFusion
    /// can not evaluate such functions during planning.
    /// In the query `select col1, random() from t1`, `random()` function will be evaluated
    /// for each output row, resulting in a unique random value for each row.
    Volatile,
}

/// A function's type signature defines the types of arguments the function supports.
///
/// Functions typically support only a few different types of arguments compared to the
/// different datatypes in Arrow. To make functions easy to use, when possible DataFusion
/// automatically coerces (add casts to) function arguments so they match the type signature.
///
/// For example, a function like `cos` may only be implemented for `Float64` arguments. To support a query
/// that calls `cos` with a different argument type, such as `cos(int_column)`, type coercion automatically
/// adds a cast such as `cos(CAST int_column AS DOUBLE)` during planning.
///
/// # Data Types
///
/// ## Timestamps
///
/// Types to match are represented using Arrow's [`DataType`].  [`DataType::Timestamp`] has an optional variable
/// timezone specification. To specify a function can handle a timestamp with *ANY* timezone, use
/// the [`TIMEZONE_WILDCARD`]. For example:
///
/// ```
/// # use arrow::datatypes::{DataType, TimeUnit};
/// # use datafusion_expr_common::signature::{TIMEZONE_WILDCARD, TypeSignature};
/// let type_signature = TypeSignature::Exact(vec![
///   // A nanosecond precision timestamp with ANY timezone
///   // matches  Timestamp(Nanosecond, Some("+0:00"))
///   // matches  Timestamp(Nanosecond, Some("+5:00"))
///   // does not match  Timestamp(Nanosecond, None)
///   DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum TypeSignature {
    /// One or more arguments of a common type out of a list of valid types.
    ///
    /// For functions that take no arguments (e.g. `random()` see [`TypeSignature::Nullary`]).
    ///
    /// # Examples
    ///
    /// A function such as `concat` is `Variadic(vec![DataType::Utf8,
    /// DataType::LargeUtf8])`
    Variadic(Vec<DataType>),
    /// The acceptable signature and coercions rules are special for this
    /// function.
    ///
    /// If this signature is specified,
    /// DataFusion will call [`ScalarUDFImpl::coerce_types`] to prepare argument types.
    ///
    /// [`ScalarUDFImpl::coerce_types`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.coerce_types
    UserDefined,
    /// One or more arguments with arbitrary types
    VariadicAny,
    /// One or more arguments of an arbitrary but equal type out of a list of valid types.
    ///
    /// # Examples
    ///
    /// 1. A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`
    /// 2. A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    Uniform(usize, Vec<DataType>),
    /// One or more arguments with exactly the specified types in order.
    ///
    /// For functions that take no arguments (e.g. `random()`) use [`TypeSignature::Nullary`].
    Exact(Vec<DataType>),
    /// One or more arguments belonging to the [`TypeSignatureClass`], in order.
    ///
    /// [`Coercion`] contains not only the desired type but also the allowed casts.
    /// For example, if you expect a function has string type, but you also allow it to be casted from binary type.
    ///
    /// For functions that take no arguments (e.g. `random()`) see [`TypeSignature::Nullary`].
    Coercible(Vec<Coercion>),
    /// One or more arguments coercible to a single, comparable type.
    ///
    /// Each argument will be coerced to a single type using the
    /// coercion rules described in [`comparison_coercion_numeric`].
    ///
    /// # Examples
    ///
    /// If the `nullif(1, 2)` function is called with `i32` and `i64` arguments
    /// the types will both be coerced to `i64` before the function is invoked.
    ///
    /// If the `nullif('1', 2)` function is called with `Utf8` and `i64` arguments
    /// the types will both be coerced to `Utf8` before the function is invoked.
    ///
    /// Note:
    /// - For functions that take no arguments (e.g. `random()` see [`TypeSignature::Nullary`]).
    /// - If all arguments have type [`DataType::Null`], they are coerced to `Utf8`
    ///
    /// [`comparison_coercion_numeric`]: crate::type_coercion::binary::comparison_coercion_numeric
    Comparable(usize),
    /// One or more arguments of arbitrary types.
    ///
    /// For functions that take no arguments (e.g. `random()`) use [`TypeSignature::Nullary`].
    Any(usize),
    /// Matches exactly one of a list of [`TypeSignature`]s.
    ///
    /// Coercion is attempted to match the signatures in order, and stops after
    /// the first success, if any.
    ///
    /// # Examples
    ///
    /// Since `make_array` takes 0 or more arguments with arbitrary types, its `TypeSignature`
    /// is `OneOf(vec![Any(0), VariadicAny])`.
    OneOf(Vec<TypeSignature>),
    /// A function that has an [`ArrayFunctionSignature`]
    ArraySignature(ArrayFunctionSignature),
    /// One or more arguments of numeric types.
    ///
    /// See [`NativeType::is_numeric`] to know which type is considered numeric
    ///
    /// For functions that take no arguments (e.g. `random()`) use [`TypeSignature::Nullary`].
    ///
    /// [`NativeType::is_numeric`]: datafusion_common::types::NativeType::is_numeric
    Numeric(usize),
    /// One or arguments of all the same string types.
    ///
    /// The precedence of type from high to low is Utf8View, LargeUtf8 and Utf8.
    /// Null is considered as `Utf8` by default
    /// Dictionary with string value type is also handled.
    ///
    /// For example, if a function is called with (utf8, large_utf8), all
    /// arguments will be coerced to  `LargeUtf8`
    ///
    /// For functions that take no arguments (e.g. `random()` use [`TypeSignature::Nullary`]).
    String(usize),
    /// No arguments
    Nullary,
}

impl TypeSignature {
    #[inline]
    pub fn is_one_of(&self) -> bool {
        matches!(self, TypeSignature::OneOf(_))
    }
}

/// Represents the class of types that can be used in a function signature.
///
/// This is used to specify what types are valid for function arguments in a more flexible way than
/// just listing specific DataTypes. For example, TypeSignatureClass::Timestamp matches any timestamp
/// type regardless of timezone or precision.
///
/// Used primarily with TypeSignature::Coercible to define function signatures that can accept
/// arguments that can be coerced to a particular class of types.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum TypeSignatureClass {
    Timestamp,
    Time,
    Interval,
    Duration,
    Native(LogicalTypeRef),
    // TODO:
    // Numeric
    Integer,
}

impl Display for TypeSignatureClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypeSignatureClass::{self:?}")
    }
}

impl TypeSignatureClass {
    /// Get example acceptable types for this `TypeSignatureClass`
    ///
    /// This is used for `information_schema` and can be used to generate
    /// documentation or error messages.
    fn get_example_types(&self) -> Vec<DataType> {
        match self {
            TypeSignatureClass::Native(l) => get_data_types(l.native()),
            TypeSignatureClass::Timestamp => {
                vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(
                        TimeUnit::Nanosecond,
                        Some(TIMEZONE_WILDCARD.into()),
                    ),
                ]
            }
            TypeSignatureClass::Time => {
                vec![DataType::Time64(TimeUnit::Nanosecond)]
            }
            TypeSignatureClass::Interval => {
                vec![DataType::Interval(IntervalUnit::DayTime)]
            }
            TypeSignatureClass::Duration => {
                vec![DataType::Duration(TimeUnit::Nanosecond)]
            }
            TypeSignatureClass::Integer => {
                vec![DataType::Int64]
            }
        }
    }

    /// Does the specified `NativeType` match this type signature class?
    pub fn matches_native_type(
        self: &TypeSignatureClass,
        logical_type: &NativeType,
    ) -> bool {
        if logical_type == &NativeType::Null {
            return true;
        }

        match self {
            TypeSignatureClass::Native(t) if t.native() == logical_type => true,
            TypeSignatureClass::Timestamp if logical_type.is_timestamp() => true,
            TypeSignatureClass::Time if logical_type.is_time() => true,
            TypeSignatureClass::Interval if logical_type.is_interval() => true,
            TypeSignatureClass::Duration if logical_type.is_duration() => true,
            TypeSignatureClass::Integer if logical_type.is_integer() => true,
            _ => false,
        }
    }

    /// What type would `origin_type` be casted to when casting to the specified native type?
    pub fn default_casted_type(
        &self,
        native_type: &NativeType,
        origin_type: &DataType,
    ) -> datafusion_common::Result<DataType> {
        match self {
            TypeSignatureClass::Native(logical_type) => {
                logical_type.native().default_cast_for(origin_type)
            }
            // If the given type is already a timestamp, we don't change the unit and timezone
            TypeSignatureClass::Timestamp if native_type.is_timestamp() => {
                Ok(origin_type.to_owned())
            }
            TypeSignatureClass::Time if native_type.is_time() => {
                Ok(origin_type.to_owned())
            }
            TypeSignatureClass::Interval if native_type.is_interval() => {
                Ok(origin_type.to_owned())
            }
            TypeSignatureClass::Duration if native_type.is_duration() => {
                Ok(origin_type.to_owned())
            }
            TypeSignatureClass::Integer if native_type.is_integer() => {
                Ok(origin_type.to_owned())
            }
            _ => internal_err!("May miss the matching logic in `matches_native_type`"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ArrayFunctionSignature {
    /// A function takes at least one List/LargeList/FixedSizeList argument.
    Array {
        /// A full list of the arguments accepted by this function.
        arguments: Vec<ArrayFunctionArgument>,
        /// Additional information about how array arguments should be coerced.
        array_coercion: Option<ListCoercion>,
    },
    /// A function takes a single argument that must be a List/LargeList/FixedSizeList
    /// which gets coerced to List, with element type recursively coerced to List too if it is list-like.
    RecursiveArray,
    /// Specialized Signature for MapArray
    /// The function takes a single argument that must be a MapArray
    MapArray,
}

impl Display for ArrayFunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrayFunctionSignature::Array { arguments, .. } => {
                for (idx, argument) in arguments.iter().enumerate() {
                    write!(f, "{argument}")?;
                    if idx != arguments.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                Ok(())
            }
            ArrayFunctionSignature::RecursiveArray => {
                write!(f, "recursive_array")
            }
            ArrayFunctionSignature::MapArray => {
                write!(f, "map_array")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ArrayFunctionArgument {
    /// A non-list or list argument. The list dimensions should be one less than the Array's list
    /// dimensions.
    Element,
    /// An Int64 index argument.
    Index,
    /// An argument of type List/LargeList/FixedSizeList. All Array arguments must be coercible
    /// to the same type.
    Array,
    // A Utf8 argument.
    String,
}

impl Display for ArrayFunctionArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrayFunctionArgument::Element => {
                write!(f, "element")
            }
            ArrayFunctionArgument::Index => {
                write!(f, "index")
            }
            ArrayFunctionArgument::Array => {
                write!(f, "array")
            }
            ArrayFunctionArgument::String => {
                write!(f, "string")
            }
        }
    }
}

impl TypeSignature {
    pub fn to_string_repr(&self) -> Vec<String> {
        match self {
            TypeSignature::Nullary => {
                vec!["NullAry()".to_string()]
            }
            TypeSignature::Variadic(types) => {
                vec![format!("{}, ..", Self::join_types(types, "/"))]
            }
            TypeSignature::Uniform(arg_count, valid_types) => {
                vec![
                    std::iter::repeat_n(Self::join_types(valid_types, "/"), *arg_count)
                        .collect::<Vec<String>>()
                        .join(", "),
                ]
            }
            TypeSignature::String(num) => {
                vec![format!("String({num})")]
            }
            TypeSignature::Numeric(num) => {
                vec![format!("Numeric({num})")]
            }
            TypeSignature::Comparable(num) => {
                vec![format!("Comparable({num})")]
            }
            TypeSignature::Coercible(coercions) => {
                vec![Self::join_types(coercions, ", ")]
            }
            TypeSignature::Exact(types) => {
                vec![Self::join_types(types, ", ")]
            }
            TypeSignature::Any(arg_count) => {
                vec![std::iter::repeat_n("Any", *arg_count)
                    .collect::<Vec<&str>>()
                    .join(", ")]
            }
            TypeSignature::UserDefined => {
                vec!["UserDefined".to_string()]
            }
            TypeSignature::VariadicAny => vec!["Any, .., Any".to_string()],
            TypeSignature::OneOf(sigs) => {
                sigs.iter().flat_map(|s| s.to_string_repr()).collect()
            }
            TypeSignature::ArraySignature(array_signature) => {
                vec![array_signature.to_string()]
            }
        }
    }

    /// Helper function to join types with specified delimiter.
    pub fn join_types<T: Display>(types: &[T], delimiter: &str) -> String {
        types
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<String>>()
            .join(delimiter)
    }

    /// Check whether 0 input argument is valid for given `TypeSignature`
    pub fn supports_zero_argument(&self) -> bool {
        match &self {
            TypeSignature::Exact(vec) => vec.is_empty(),
            TypeSignature::Nullary => true,
            TypeSignature::OneOf(types) => types
                .iter()
                .any(|type_sig| type_sig.supports_zero_argument()),
            _ => false,
        }
    }

    /// Returns true if the signature currently supports or used to supported 0
    /// input arguments in a previous version of DataFusion.
    pub fn used_to_support_zero_arguments(&self) -> bool {
        match &self {
            TypeSignature::Any(num) => *num == 0,
            _ => self.supports_zero_argument(),
        }
    }

    #[deprecated(since = "46.0.0", note = "See get_example_types instead")]
    pub fn get_possible_types(&self) -> Vec<Vec<DataType>> {
        self.get_example_types()
    }

    /// Return example acceptable types for this `TypeSignature`'
    ///
    /// Returns a `Vec<DataType>` for each argument to the function
    ///
    /// This is used for `information_schema` and can be used to generate
    /// documentation or error messages.
    pub fn get_example_types(&self) -> Vec<Vec<DataType>> {
        match self {
            TypeSignature::Exact(types) => vec![types.clone()],
            TypeSignature::OneOf(types) => types
                .iter()
                .flat_map(|type_sig| type_sig.get_example_types())
                .collect(),
            TypeSignature::Uniform(arg_count, types) => types
                .iter()
                .cloned()
                .map(|data_type| vec![data_type; *arg_count])
                .collect(),
            TypeSignature::Coercible(coercions) => coercions
                .iter()
                .map(|c| {
                    let mut all_types: IndexSet<DataType> =
                        c.desired_type().get_example_types().into_iter().collect();

                    if let Some(implicit_coercion) = c.implicit_coercion() {
                        let allowed_casts: Vec<DataType> = implicit_coercion
                            .allowed_source_types
                            .iter()
                            .flat_map(|t| t.get_example_types())
                            .collect();
                        all_types.extend(allowed_casts);
                    }

                    all_types.into_iter().collect::<Vec<_>>()
                })
                .multi_cartesian_product()
                .collect(),
            TypeSignature::Variadic(types) => types
                .iter()
                .cloned()
                .map(|data_type| vec![data_type])
                .collect(),
            TypeSignature::Numeric(arg_count) => NUMERICS
                .iter()
                .cloned()
                .map(|numeric_type| vec![numeric_type; *arg_count])
                .collect(),
            TypeSignature::String(arg_count) => get_data_types(&NativeType::String)
                .into_iter()
                .map(|dt| vec![dt; *arg_count])
                .collect::<Vec<_>>(),
            // TODO: Implement for other types
            TypeSignature::Any(_)
            | TypeSignature::Comparable(_)
            | TypeSignature::Nullary
            | TypeSignature::VariadicAny
            | TypeSignature::ArraySignature(_)
            | TypeSignature::UserDefined => vec![],
        }
    }
}

fn get_data_types(native_type: &NativeType) -> Vec<DataType> {
    match native_type {
        NativeType::Null => vec![DataType::Null],
        NativeType::Boolean => vec![DataType::Boolean],
        NativeType::Int8 => vec![DataType::Int8],
        NativeType::Int16 => vec![DataType::Int16],
        NativeType::Int32 => vec![DataType::Int32],
        NativeType::Int64 => vec![DataType::Int64],
        NativeType::UInt8 => vec![DataType::UInt8],
        NativeType::UInt16 => vec![DataType::UInt16],
        NativeType::UInt32 => vec![DataType::UInt32],
        NativeType::UInt64 => vec![DataType::UInt64],
        NativeType::Float16 => vec![DataType::Float16],
        NativeType::Float32 => vec![DataType::Float32],
        NativeType::Float64 => vec![DataType::Float64],
        NativeType::Date => vec![DataType::Date32, DataType::Date64],
        NativeType::Binary => vec![
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ],
        NativeType::String => {
            vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
        }
        // TODO: support other native types
        _ => vec![],
    }
}

/// Represents type coercion rules for function arguments, specifying both the desired type
/// and optional implicit coercion rules for source types.
///
/// # Examples
///
/// ```
/// use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
/// use datafusion_common::types::{NativeType, logical_binary, logical_string};
///
/// // Exact coercion that only accepts timestamp types
/// let exact = Coercion::new_exact(TypeSignatureClass::Timestamp);
///
/// // Implicit coercion that accepts string types but can coerce from binary types
/// let implicit = Coercion::new_implicit(
///     TypeSignatureClass::Native(logical_string()),
///     vec![TypeSignatureClass::Native(logical_binary())],
///     NativeType::String
/// );
/// ```
///
/// There are two variants:
///
/// * `Exact` - Only accepts arguments that exactly match the desired type
/// * `Implicit` - Accepts the desired type and can coerce from specified source types
#[derive(Debug, Clone, Eq, PartialOrd)]
pub enum Coercion {
    /// Coercion that only accepts arguments exactly matching the desired type.
    Exact {
        /// The required type for the argument
        desired_type: TypeSignatureClass,
    },

    /// Coercion that accepts the desired type and can implicitly coerce from other types.
    Implicit {
        /// The primary desired type for the argument
        desired_type: TypeSignatureClass,
        /// Rules for implicit coercion from other types
        implicit_coercion: ImplicitCoercion,
    },
}

impl Coercion {
    pub fn new_exact(desired_type: TypeSignatureClass) -> Self {
        Self::Exact { desired_type }
    }

    /// Create a new coercion with implicit coercion rules.
    ///
    /// `allowed_source_types` defines the possible types that can be coerced to `desired_type`.
    /// `default_casted_type` is the default type to be used for coercion if we cast from other types via `allowed_source_types`.
    pub fn new_implicit(
        desired_type: TypeSignatureClass,
        allowed_source_types: Vec<TypeSignatureClass>,
        default_casted_type: NativeType,
    ) -> Self {
        Self::Implicit {
            desired_type,
            implicit_coercion: ImplicitCoercion {
                allowed_source_types,
                default_casted_type,
            },
        }
    }

    pub fn allowed_source_types(&self) -> &[TypeSignatureClass] {
        match self {
            Coercion::Exact { .. } => &[],
            Coercion::Implicit {
                implicit_coercion, ..
            } => implicit_coercion.allowed_source_types.as_slice(),
        }
    }

    pub fn default_casted_type(&self) -> Option<&NativeType> {
        match self {
            Coercion::Exact { .. } => None,
            Coercion::Implicit {
                implicit_coercion, ..
            } => Some(&implicit_coercion.default_casted_type),
        }
    }

    pub fn desired_type(&self) -> &TypeSignatureClass {
        match self {
            Coercion::Exact { desired_type } => desired_type,
            Coercion::Implicit { desired_type, .. } => desired_type,
        }
    }

    pub fn implicit_coercion(&self) -> Option<&ImplicitCoercion> {
        match self {
            Coercion::Exact { .. } => None,
            Coercion::Implicit {
                implicit_coercion, ..
            } => Some(implicit_coercion),
        }
    }
}

impl Display for Coercion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Coercion({}", self.desired_type())?;
        if let Some(implicit_coercion) = self.implicit_coercion() {
            write!(f, ", implicit_coercion={implicit_coercion}",)
        } else {
            write!(f, ")")
        }
    }
}

impl PartialEq for Coercion {
    fn eq(&self, other: &Self) -> bool {
        self.desired_type() == other.desired_type()
            && self.implicit_coercion() == other.implicit_coercion()
    }
}

impl Hash for Coercion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.desired_type().hash(state);
        self.implicit_coercion().hash(state);
    }
}

/// Defines rules for implicit type coercion, specifying which source types can be
/// coerced and the default type to use when coercing.
///
/// This is used by functions to specify which types they can accept via implicit
/// coercion in addition to their primary desired type.
///
/// # Examples
///
/// ```
/// use arrow::datatypes::TimeUnit;
///
/// use datafusion_expr_common::signature::{Coercion, ImplicitCoercion, TypeSignatureClass};
/// use datafusion_common::types::{NativeType, logical_binary};
///
/// // Allow coercing from binary types to timestamp, coerce to specific timestamp unit and timezone
/// let implicit = Coercion::new_implicit(
///     TypeSignatureClass::Timestamp,
///     vec![TypeSignatureClass::Native(logical_binary())],
///     NativeType::Timestamp(TimeUnit::Second, None),
/// );
/// ```
#[derive(Debug, Clone, Eq, PartialOrd)]
pub struct ImplicitCoercion {
    /// The types that can be coerced from via implicit casting
    allowed_source_types: Vec<TypeSignatureClass>,

    /// The default type to use when coercing from allowed source types.
    /// This is particularly important for types like Timestamp that have multiple
    /// possible configurations (different time units and timezones).
    default_casted_type: NativeType,
}

impl Display for ImplicitCoercion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ImplicitCoercion({:?}, default_type={:?})",
            self.allowed_source_types, self.default_casted_type
        )
    }
}

impl PartialEq for ImplicitCoercion {
    fn eq(&self, other: &Self) -> bool {
        self.allowed_source_types == other.allowed_source_types
            && self.default_casted_type == other.default_casted_type
    }
}

impl Hash for ImplicitCoercion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.allowed_source_types.hash(state);
        self.default_casted_type.hash(state);
    }
}

/// Defines the supported argument types ([`TypeSignature`]) and [`Volatility`] for a function.
///
/// DataFusion will automatically coerce (cast) argument types to one of the supported
/// function signatures, if possible.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Signature {
    /// The data types that the function accepts. See [TypeSignature] for more information.
    pub type_signature: TypeSignature,
    /// The volatility of the function. See [Volatility] for more information.
    pub volatility: Volatility,
}

impl Signature {
    /// Creates a new Signature from a given type signature and volatility.
    pub fn new(type_signature: TypeSignature, volatility: Volatility) -> Self {
        Signature {
            type_signature,
            volatility,
        }
    }
    /// An arbitrary number of arguments with the same type, from those listed in `common_types`.
    pub fn variadic(common_types: Vec<DataType>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Variadic(common_types),
            volatility,
        }
    }
    /// User-defined coercion rules for the function.
    pub fn user_defined(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::UserDefined,
            volatility,
        }
    }

    /// A specified number of numeric arguments
    pub fn numeric(arg_count: usize, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Numeric(arg_count),
            volatility,
        }
    }

    /// A specified number of numeric arguments
    pub fn string(arg_count: usize, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::String(arg_count),
            volatility,
        }
    }

    /// An arbitrary number of arguments of any type.
    pub fn variadic_any(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicAny,
            volatility,
        }
    }
    /// A fixed number of arguments of the same type, from those listed in `valid_types`.
    pub fn uniform(
        arg_count: usize,
        valid_types: Vec<DataType>,
        volatility: Volatility,
    ) -> Self {
        Self {
            type_signature: TypeSignature::Uniform(arg_count, valid_types),
            volatility,
        }
    }
    /// Exactly matches the types in `exact_types`, in order.
    pub fn exact(exact_types: Vec<DataType>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Exact(exact_types),
            volatility,
        }
    }

    /// Target coerce types in order
    pub fn coercible(target_types: Vec<Coercion>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Coercible(target_types),
            volatility,
        }
    }

    /// Used for function that expects comparable data types, it will try to coerced all the types into single final one.
    pub fn comparable(arg_count: usize, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Comparable(arg_count),
            volatility,
        }
    }

    pub fn nullary(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Nullary,
            volatility,
        }
    }

    /// A specified number of arguments of any type
    pub fn any(arg_count: usize, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Any(arg_count),
            volatility,
        }
    }

    /// Any one of a list of [TypeSignature]s.
    pub fn one_of(type_signatures: Vec<TypeSignature>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::OneOf(type_signatures),
            volatility,
        }
    }

    /// Specialized [Signature] for ArrayAppend and similar functions.
    pub fn array_and_element(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Array,
                        ArrayFunctionArgument::Element,
                    ],
                    array_coercion: Some(ListCoercion::FixedSizedListToList),
                },
            ),
            volatility,
        }
    }

    /// Specialized [Signature] for ArrayPrepend and similar functions.
    pub fn element_and_array(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Element,
                        ArrayFunctionArgument::Array,
                    ],
                    array_coercion: Some(ListCoercion::FixedSizedListToList),
                },
            ),
            volatility,
        }
    }

    /// Specialized [Signature] for functions that take a fixed number of arrays.
    pub fn arrays(
        n: usize,
        coercion: Option<ListCoercion>,
        volatility: Volatility,
    ) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::Array {
                    arguments: vec![ArrayFunctionArgument::Array; n],
                    array_coercion: coercion,
                },
            ),
            volatility,
        }
    }

    /// Specialized [Signature] for Array functions with an optional index.
    pub fn array_and_element_and_optional_index(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::OneOf(vec![
                TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Array,
                        ArrayFunctionArgument::Element,
                    ],
                    array_coercion: None,
                }),
                TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Array,
                        ArrayFunctionArgument::Element,
                        ArrayFunctionArgument::Index,
                    ],
                    array_coercion: None,
                }),
            ]),
            volatility,
        }
    }

    /// Specialized [Signature] for ArrayElement and similar functions.
    pub fn array_and_index(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::Array {
                    arguments: vec![
                        ArrayFunctionArgument::Array,
                        ArrayFunctionArgument::Index,
                    ],
                    array_coercion: Some(ListCoercion::FixedSizedListToList),
                },
            ),
            volatility,
        }
    }

    /// Specialized [Signature] for ArrayEmpty and similar functions.
    pub fn array(volatility: Volatility) -> Self {
        Signature::arrays(1, Some(ListCoercion::FixedSizedListToList), volatility)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::types::{logical_int64, logical_string};

    use super::*;

    #[test]
    fn supports_zero_argument_tests() {
        // Testing `TypeSignature`s which supports 0 arg
        let positive_cases = vec![
            TypeSignature::Exact(vec![]),
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int8]),
                TypeSignature::Nullary,
                TypeSignature::Uniform(1, vec![DataType::Int8]),
            ]),
            TypeSignature::Nullary,
        ];

        for case in positive_cases {
            assert!(
                case.supports_zero_argument(),
                "Expected {case:?} to support zero arguments"
            );
        }

        // Testing `TypeSignature`s which doesn't support 0 arg
        let negative_cases = vec![
            TypeSignature::Exact(vec![DataType::Utf8]),
            TypeSignature::Uniform(1, vec![DataType::Float64]),
            TypeSignature::Any(1),
            TypeSignature::VariadicAny,
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int8]),
                TypeSignature::Uniform(1, vec![DataType::Int8]),
            ]),
        ];

        for case in negative_cases {
            assert!(
                !case.supports_zero_argument(),
                "Expected {case:?} not to support zero arguments"
            );
        }
    }

    #[test]
    fn type_signature_partial_ord() {
        // Test validates that partial ord is defined for TypeSignature and Signature.
        assert!(TypeSignature::UserDefined < TypeSignature::VariadicAny);
        assert!(TypeSignature::UserDefined < TypeSignature::Any(1));

        assert!(
            TypeSignature::Uniform(1, vec![DataType::Null])
                < TypeSignature::Uniform(1, vec![DataType::Boolean])
        );
        assert!(
            TypeSignature::Uniform(1, vec![DataType::Null])
                < TypeSignature::Uniform(2, vec![DataType::Null])
        );
        assert!(
            TypeSignature::Uniform(usize::MAX, vec![DataType::Null])
                < TypeSignature::Exact(vec![DataType::Null])
        );
    }

    #[test]
    fn test_get_possible_types() {
        let type_signature = TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(possible_types, vec![vec![DataType::Int32, DataType::Int64]]);

        let type_signature = TypeSignature::OneOf(vec![
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Float32, DataType::Float64]),
        ]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Int32, DataType::Int64],
                vec![DataType::Float32, DataType::Float64]
            ]
        );

        let type_signature = TypeSignature::OneOf(vec![
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Float32, DataType::Float64]),
            TypeSignature::Exact(vec![DataType::Utf8]),
        ]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Int32, DataType::Int64],
                vec![DataType::Float32, DataType::Float64],
                vec![DataType::Utf8]
            ]
        );

        let type_signature =
            TypeSignature::Uniform(2, vec![DataType::Float32, DataType::Int64]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Float32, DataType::Float32],
                vec![DataType::Int64, DataType::Int64]
            ]
        );

        let type_signature = TypeSignature::Coercible(vec![
            Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
            Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
        ]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Utf8, DataType::Int64],
                vec![DataType::LargeUtf8, DataType::Int64],
                vec![DataType::Utf8View, DataType::Int64]
            ]
        );

        let type_signature =
            TypeSignature::Variadic(vec![DataType::Int32, DataType::Int64]);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![vec![DataType::Int32], vec![DataType::Int64]]
        );

        let type_signature = TypeSignature::Numeric(2);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Int8, DataType::Int8],
                vec![DataType::Int16, DataType::Int16],
                vec![DataType::Int32, DataType::Int32],
                vec![DataType::Int64, DataType::Int64],
                vec![DataType::UInt8, DataType::UInt8],
                vec![DataType::UInt16, DataType::UInt16],
                vec![DataType::UInt32, DataType::UInt32],
                vec![DataType::UInt64, DataType::UInt64],
                vec![DataType::Float32, DataType::Float32],
                vec![DataType::Float64, DataType::Float64]
            ]
        );

        let type_signature = TypeSignature::String(2);
        let possible_types = type_signature.get_example_types();
        assert_eq!(
            possible_types,
            vec![
                vec![DataType::Utf8, DataType::Utf8],
                vec![DataType::LargeUtf8, DataType::LargeUtf8],
                vec![DataType::Utf8View, DataType::Utf8View]
            ]
        );
    }
}

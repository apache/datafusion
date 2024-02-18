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

use crate::type_coercion::binary::comparison_coercion;
use arrow::datatypes::DataType;
use datafusion_common::utils::coerced_fixed_size_list_to_list;
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};

/// Constant that is used as a placeholder for any valid timezone.
/// This is used where a function can accept a timestamp type with any
/// valid timezone, it exists to avoid the need to enumerate all possible
/// timezones. See [`TypeSignature`] for more details.
///
/// Type coercion always ensures that functions will be executed using
/// timestamp arrays that have a valid time zone. Functions must never
/// return results with this timezone.
pub const TIMEZONE_WILDCARD: &str = "+TZ";

///A function's volatility, which defines the functions eligibility for certain optimizations
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Volatility {
    /// An immutable function will always return the same output when given the same
    /// input. An example of this is [super::BuiltinScalarFunction::Cos]. DataFusion
    /// will attempt to inline immutable functions during planning.
    Immutable,
    /// A stable function may return different values given the same input across different
    /// queries but must return the same value for a given input within a query. An example of
    /// this is [super::BuiltinScalarFunction::Now]. DataFusion
    /// will attempt to inline `Stable` functions during planning, when possible.
    /// For query `select col1, now() from t1`, it might take a while to execute but
    /// `now()` column will be the same for each output row, which is evaluated
    /// during planning.
    Stable,
    /// A volatile function may change the return value from evaluation to evaluation.
    /// Multiple invocations of a volatile function may return different results when used in the
    /// same query. An example of this is [super::BuiltinScalarFunction::Random]. DataFusion
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
/// that calles `cos` with a different argument type, such as `cos(int_column)`, type coercion automatically
/// adds a cast such as `cos(CAST int_column AS DOUBLE)` during planning.
///
/// # Data Types
/// Types to match are represented using Arrow's [`DataType`].  [`DataType::Timestamp`] has an optional variable
/// timezone specification. To specify a function can handle a timestamp with *ANY* timezone, use
/// the [`TIMEZONE_WILDCARD`]. For example:
///
/// ```
/// # use arrow::datatypes::{DataType, TimeUnit};
/// # use datafusion_expr::{TIMEZONE_WILDCARD, TypeSignature};
/// let type_signature = TypeSignature::Exact(vec![
///   // A nanosecond precision timestamp with ANY timezone
///   // matches  Timestamp(Nanosecond, Some("+0:00"))
///   // matches  Timestamp(Nanosecond, Some("+5:00"))
///   // does not match  Timestamp(Nanosecond, None)
///   DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeSignature {
    /// One or more arguments of an common type out of a list of valid types.
    ///
    /// # Examples
    /// A function such as `concat` is `Variadic(vec![DataType::Utf8, DataType::LargeUtf8])`
    Variadic(Vec<DataType>),
    /// One or more arguments of an arbitrary but equal type.
    /// DataFusion attempts to coerce all argument types to match the first argument's type
    ///
    /// # Examples
    /// Given types in signature should be coercible to the same final type.
    /// A function such as `make_array` is `VariadicEqual`.
    ///
    /// `make_array(i32, i64) -> make_array(i64, i64)`
    VariadicEqual,
    /// One or more arguments with arbitrary types
    VariadicAny,
    /// Fixed number of arguments of an arbitrary but equal type out of a list of valid types.
    ///
    /// # Examples
    /// 1. A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`
    /// 2. A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    Uniform(usize, Vec<DataType>),
    /// Exact number of arguments of an exact type
    Exact(Vec<DataType>),
    /// Fixed number of arguments of arbitrary types
    /// If a function takes 0 argument, its `TypeSignature` should be `Any(0)`
    Any(usize),
    /// Matches exactly one of a list of [`TypeSignature`]s. Coercion is attempted to match
    /// the signatures in order, and stops after the first success, if any.
    ///
    /// # Examples
    /// Function `make_array` takes 0 or more arguments with arbitrary types, its `TypeSignature`
    /// is `OneOf(vec![Any(0), VariadicAny])`.
    OneOf(Vec<TypeSignature>),
    /// Specifies Signatures for array functions
    ArraySignature(ArrayFunctionSignature),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArrayFunctionSignature {
    /// Specialized Signature for ArrayAppend and similar functions
    /// If `allow_null` is true, the function also accepts a single argument of type Null.
    /// The first argument should be List/LargeList/FixedSizedList, and the second argument should be non-list or list.
    /// The second argument's list dimension should be one dimension less than the first argument's list dimension.
    /// List dimension of the List/LargeList is equivalent to the number of List.
    /// List dimension of the non-list is 0.
    ArrayAndElement(bool),
    /// Specialized Signature for ArrayPrepend and similar functions
    /// If `allow_null` is true, the function also accepts a single argument of type Null.
    /// The first argument should be non-list or list, and the second argument should be List/LargeList.
    /// The first argument's list dimension should be one dimension less than the second argument's list dimension.
    ElementAndArray(bool),
    /// Specialized Signature for Array functions of the form (List/LargeList, Index)
    /// If `allow_null` is true, the function also accepts a single argument of type Null.
    /// The first argument should be List/LargeList/FixedSizedList, and the second argument should be Int64.
    ArrayAndIndex(bool),
    /// Specialized Signature for Array functions of the form (List/LargeList, Element, Optional Index)
    ArrayAndElementAndOptionalIndex,
    /// Specialized Signature for ArrayEmpty and similar functions
    /// The function takes a single argument that must be a List/LargeList/FixedSizeList
    /// or something that can be coerced to one of those types.
    /// If `allow_null` is true, the function also accepts a single argument of type Null.
    Array(bool),
}

impl ArrayFunctionSignature {
    /// Arguments to ArrayFunctionSignature
    /// `current_types` - The data types of the arguments
    /// `allow_null_coercion` - Whether null type coercion is allowed
    /// Returns the valid types for the function signature
    pub fn get_type_signature(
        &self,
        current_types: &[DataType],
    ) -> Result<Vec<Vec<DataType>>> {
        fn array_append_or_prepend_valid_types(
            current_types: &[DataType],
            is_append: bool,
            allow_null_coercion: bool,
        ) -> Result<Vec<Vec<DataType>>> {
            if current_types.len() != 2 {
                return Ok(vec![vec![]]);
            }

            let (array_type, elem_type) = if is_append {
                (&current_types[0], &current_types[1])
            } else {
                (&current_types[1], &current_types[0])
            };

            // We follow Postgres on `array_append(Null, T)`, which is not valid.
            if array_type.eq(&DataType::Null) && !allow_null_coercion {
                return Ok(vec![vec![]]);
            }

            // We need to find the coerced base type, mainly for cases like:
            // `array_append(List(null), i64)` -> `List(i64)`
            let array_base_type = datafusion_common::utils::base_type(array_type);
            let elem_base_type = datafusion_common::utils::base_type(elem_type);
            let new_base_type = comparison_coercion(&array_base_type, &elem_base_type);

            let new_base_type = new_base_type.ok_or_else(|| {
                internal_datafusion_err!(
                    "Coercion from {array_base_type:?} to {elem_base_type:?} not supported."
                )
            })?;

            let array_type = datafusion_common::utils::coerced_type_with_base_type_only(
                array_type,
                &new_base_type,
            );

            match array_type {
                DataType::List(ref field)
                | DataType::LargeList(ref field)
                | DataType::FixedSizeList(ref field, _) => {
                    let elem_type = field.data_type();
                    if is_append {
                        Ok(vec![vec![array_type.clone(), elem_type.clone()]])
                    } else {
                        Ok(vec![vec![elem_type.to_owned(), array_type.clone()]])
                    }
                }
                _ => Ok(vec![vec![]]),
            }
        }
        fn array_and_index(
            current_types: &[DataType],
            allow_null_coercion: bool,
        ) -> Result<Vec<Vec<DataType>>> {
            if current_types.len() != 2 {
                return Ok(vec![vec![]]);
            }

            let array_type = &current_types[0];

            if array_type.eq(&DataType::Null) && !allow_null_coercion {
                return Ok(vec![vec![]]);
            }

            match array_type {
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _) => {
                    let array_type = coerced_fixed_size_list_to_list(array_type);
                    Ok(vec![vec![array_type, DataType::Int64]])
                }
                DataType::Null => Ok(vec![vec![array_type.clone(), DataType::Int64]]),
                _ => Ok(vec![vec![]]),
            }
        }
        fn array(
            current_types: &[DataType],
            allow_null_coercion: bool,
        ) -> Result<Vec<Vec<DataType>>> {
            if current_types.len() != 1
                || (current_types[0].is_null() && !allow_null_coercion)
            {
                return Ok(vec![vec![]]);
            }

            let array_type = &current_types[0];

            match array_type {
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _) => {
                    let array_type = coerced_fixed_size_list_to_list(array_type);
                    Ok(vec![vec![array_type]])
                }
                DataType::Null => Ok(vec![vec![array_type.clone()]]),
                _ => Ok(vec![vec![]]),
            }
        }
        match self {
            ArrayFunctionSignature::ArrayAndElement(allow_null) => {
                array_append_or_prepend_valid_types(current_types, true, *allow_null)
            }
            ArrayFunctionSignature::ElementAndArray(allow_null) => {
                array_append_or_prepend_valid_types(current_types, false, *allow_null)
            }
            ArrayFunctionSignature::ArrayAndIndex(allow_null) => {
                array_and_index(current_types, *allow_null)
            }
            ArrayFunctionSignature::Array(allow_null) => {
                array(current_types, *allow_null)
            }
        }
    }
}

impl std::fmt::Display for ArrayFunctionSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrayFunctionSignature::ArrayAndElement(allow_null) => {
                write!(f, "ArrayAndElement({})", *allow_null)
            }
            ArrayFunctionSignature::ArrayAndElementAndOptionalIndex => {
                write!(f, "array, element, [index]")
            }
            ArrayFunctionSignature::ElementAndArray(allow_null) => {
                write!(f, "ElementAndArray({})", *allow_null)
            }
            ArrayFunctionSignature::ArrayAndIndex(allow_null) => {
                write!(f, "ArrayAndIndex({})", *allow_null)
            }
            ArrayFunctionSignature::Array(allow_null) => {
                write!(f, "Array({})", *allow_null)
            }
        }
    }
}

impl TypeSignature {
    pub(crate) fn to_string_repr(&self) -> Vec<String> {
        match self {
            TypeSignature::Variadic(types) => {
                vec![format!("{}, ..", Self::join_types(types, "/"))]
            }
            TypeSignature::Uniform(arg_count, valid_types) => {
                vec![std::iter::repeat(Self::join_types(valid_types, "/"))
                    .take(*arg_count)
                    .collect::<Vec<String>>()
                    .join(", ")]
            }
            TypeSignature::Exact(types) => {
                vec![Self::join_types(types, ", ")]
            }
            TypeSignature::Any(arg_count) => {
                vec![std::iter::repeat("Any")
                    .take(*arg_count)
                    .collect::<Vec<&str>>()
                    .join(", ")]
            }
            TypeSignature::VariadicEqual => {
                vec!["CoercibleT, .., CoercibleT".to_string()]
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
    pub(crate) fn join_types<T: std::fmt::Display>(
        types: &[T],
        delimiter: &str,
    ) -> String {
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
            TypeSignature::Uniform(0, _) | TypeSignature::Any(0) => true,
            TypeSignature::OneOf(types) => types
                .iter()
                .any(|type_sig| type_sig.supports_zero_argument()),
            _ => false,
        }
    }
}

/// Defines the supported argument types ([`TypeSignature`]) and [`Volatility`] for a function.
///
/// DataFusion will automatically coerce (cast) argument types to one of the supported
/// function signatures, if possible.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// An arbitrary number of arguments of the same type.
    pub fn variadic_equal(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicEqual,
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
    /// Specialized Signature for ArrayAppend and similar functions
    pub fn array_and_element(allow_null: bool, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::ArrayAndElement(allow_null),
            ),
            volatility,
        }
    }
    /// Specialized Signature for Array functions with an optional index
    pub fn array_and_element_and_optional_index(volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::ArrayAndElementAndOptionalIndex,
            ),
            volatility,
        }
    }
    /// Specialized Signature for ArrayPrepend and similar functions
    pub fn element_and_array(allow_null: bool, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::ElementAndArray(allow_null),
            ),
            volatility,
        }
    }
    /// Specialized Signature for ArrayElement and similar functions
    pub fn array_and_index(allow_null: bool, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(
                ArrayFunctionSignature::ArrayAndIndex(allow_null),
            ),
            volatility,
        }
    }
    /// Specialized Signature for ArrayEmpty and similar functions
    pub fn array(allow_null: bool, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::ArraySignature(ArrayFunctionSignature::Array(
                allow_null,
            )),
            volatility,
        }
    }
}

/// Monotonicity of the `ScalarFunctionExpr` with respect to its arguments.
/// Each element of this vector corresponds to an argument and indicates whether
/// the function's behavior is monotonic, or non-monotonic/unknown for that argument, namely:
/// - `None` signifies unknown monotonicity or non-monotonicity.
/// - `Some(true)` indicates that the function is monotonically increasing w.r.t. the argument in question.
/// - Some(false) indicates that the function is monotonically decreasing w.r.t. the argument in question.
pub type FuncMonotonicity = Vec<Option<bool>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_zero_argument_tests() {
        // Testing `TypeSignature`s which supports 0 arg
        let positive_cases = vec![
            TypeSignature::Exact(vec![]),
            TypeSignature::Uniform(0, vec![DataType::Float64]),
            TypeSignature::Any(0),
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int8]),
                TypeSignature::Any(0),
                TypeSignature::Uniform(1, vec![DataType::Int8]),
            ]),
        ];

        for case in positive_cases {
            assert!(
                case.supports_zero_argument(),
                "Expected {:?} to support zero arguments",
                case
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
                "Expected {:?} not to support zero arguments",
                case
            );
        }
    }
}

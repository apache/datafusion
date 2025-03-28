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

//! Coercion rules for matching argument types for binary operators

use std::collections::HashSet;
use std::sync::Arc;

use crate::operator::Operator;

use arrow::array::{new_empty_array, Array};
use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
};
use datafusion_common::types::NativeType;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_datafusion_err, plan_err, Diagnostic,
    Result, Span, Spans,
};
use itertools::Itertools;

/// The type signature of an instantiation of binary operator expression such as
/// `lhs + rhs`
///
/// Note this is different than [`crate::signature::Signature`] which
/// describes the type signature of a function.
struct Signature {
    /// The type to coerce the left argument to
    lhs: DataType,
    /// The type to coerce the right argument to
    rhs: DataType,
    /// The return type of the expression
    ret: DataType,
}

impl Signature {
    /// A signature where the inputs are the same type as the output
    fn uniform(t: DataType) -> Self {
        Self {
            lhs: t.clone(),
            rhs: t.clone(),
            ret: t,
        }
    }

    /// A signature where the inputs are the same type with a boolean output
    fn comparison(t: DataType) -> Self {
        Self {
            lhs: t.clone(),
            rhs: t,
            ret: DataType::Boolean,
        }
    }
}

/// Provides type information about a binary expression, coercing different
/// input types into a sensible output type.
pub struct BinaryTypeCoercer<'a> {
    lhs: &'a DataType,
    op: &'a Operator,
    rhs: &'a DataType,

    lhs_spans: Spans,
    op_spans: Spans,
    rhs_spans: Spans,
}

impl<'a> BinaryTypeCoercer<'a> {
    /// Creates a new [`BinaryTypeCoercer`], for reasoning about the input
    /// and output types of a binary expression.
    pub fn new(lhs: &'a DataType, op: &'a Operator, rhs: &'a DataType) -> Self {
        Self {
            lhs,
            op,
            rhs,
            lhs_spans: Spans::new(),
            op_spans: Spans::new(),
            rhs_spans: Spans::new(),
        }
    }

    /// Sets the spans information for the left side of the binary expression,
    /// so better diagnostics can be provided in case of errors.
    pub fn set_lhs_spans(&mut self, spans: Spans) {
        self.lhs_spans = spans;
    }

    /// Sets the spans information for the operator of the binary expression, so
    /// better diagnostics can be provided in case of errors.
    pub fn set_op_spans(&mut self, spans: Spans) {
        self.op_spans = spans;
    }

    /// Sets the spans information for the right side of the binary expression,
    /// so better diagnostics can be provided in case of errors.
    pub fn set_rhs_spans(&mut self, spans: Spans) {
        self.rhs_spans = spans;
    }

    fn span(&self) -> Option<Span> {
        Span::union_iter(
            [self.lhs_spans.first(), self.rhs_spans.first()]
                .iter()
                .copied()
                .flatten(),
        )
    }

    /// Returns a [`Signature`] for applying `op` to arguments of type `lhs` and `rhs`
    fn signature(&'a self) -> Result<Signature> {
        use arrow::datatypes::DataType::*;
        use Operator::*;
        let result = match self.op {
        Eq |
        NotEq |
        Lt |
        LtEq |
        Gt |
        GtEq |
        IsDistinctFrom |
        IsNotDistinctFrom => {
            comparison_coercion(self.lhs, self.rhs).map(Signature::comparison).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common argument type for comparison operation {} {} {}",
                    self.lhs,
                    self.op,
                    self.rhs
                )
            })
        }
        And | Or => if matches!((self.lhs, self.rhs), (Boolean | Null, Boolean | Null)) {
            // Logical binary boolean operators can only be evaluated for
            // boolean or null arguments.                   
            Ok(Signature::uniform(Boolean))
        } else {
            plan_err!(
                "Cannot infer common argument type for logical boolean operation {} {} {}", self.lhs, self.op, self.rhs
            )
        }
        RegexMatch | RegexIMatch | RegexNotMatch | RegexNotIMatch => {
            regex_coercion(self.lhs, self.rhs).map(Signature::comparison).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common argument type for regex operation {} {} {}", self.lhs, self.op, self.rhs
                )
            })
        }
        LikeMatch | ILikeMatch | NotLikeMatch | NotILikeMatch => {
            regex_coercion(self.lhs, self.rhs).map(Signature::comparison).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common argument type for regex operation {} {} {}", self.lhs, self.op, self.rhs
                )
            })
        }
        BitwiseAnd | BitwiseOr | BitwiseXor | BitwiseShiftRight | BitwiseShiftLeft => {
            bitwise_coercion(self.lhs, self.rhs).map(Signature::uniform).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common type for bitwise operation {} {} {}", self.lhs, self.op, self.rhs
                )
            })
        }
        StringConcat => {
            string_concat_coercion(self.lhs, self.rhs).map(Signature::uniform).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common string type for string concat operation {} {} {}", self.lhs, self.op, self.rhs
                )
            })
        }
        AtArrow | ArrowAt => {
            // Array contains or search (similar to LIKE) operation
            array_coercion(self.lhs, self.rhs)
                .or_else(|| like_coercion(self.lhs, self.rhs)).map(Signature::comparison).ok_or_else(|| {
                    plan_datafusion_err!(
                        "Cannot infer common argument type for operation {} {} {}", self.lhs, self.op, self.rhs
                    )
                })
        }
        AtAt => {
            // text search has similar signature to LIKE
            like_coercion(self.lhs, self.rhs).map(Signature::comparison).ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot infer common argument type for AtAt operation {} {} {}", self.lhs, self.op, self.rhs
                )
            })
        }
        Plus | Minus | Multiply | Divide | Modulo  =>  {
            let get_result = |lhs, rhs| {
                use arrow::compute::kernels::numeric::*;
                let l = new_empty_array(lhs);
                let r = new_empty_array(rhs);

                let result = match self.op {
                    Plus => add_wrapping(&l, &r),
                    Minus => sub_wrapping(&l, &r),
                    Multiply => mul_wrapping(&l, &r),
                    Divide => div(&l, &r),
                    Modulo => rem(&l, &r),
                    _ => unreachable!(),
                };
                result.map(|x| x.data_type().clone())
            };

            if let Ok(ret) = get_result(self.lhs, self.rhs) {
                // Temporal arithmetic, e.g. Date32 + Interval
                Ok(Signature{
                    lhs: self.lhs.clone(),
                    rhs: self.rhs.clone(),
                    ret,
                })
            } else if let Some(coerced) = temporal_coercion_strict_timezone(self.lhs, self.rhs) {
                // Temporal arithmetic by first coercing to a common time representation
                // e.g. Date32 - Timestamp
                let ret = get_result(&coerced, &coerced).map_err(|e| {
                    plan_datafusion_err!(
                        "Cannot get result type for temporal operation {coerced} {} {coerced}: {e}", self.op
                    )
                })?;
                Ok(Signature{
                    lhs: coerced.clone(),
                    rhs: coerced,
                    ret,
                })
            } else if let Some((lhs, rhs)) = math_decimal_coercion(self.lhs, self.rhs) {
                // Decimal arithmetic, e.g. Decimal(10, 2) + Decimal(10, 0)
                let ret = get_result(&lhs, &rhs).map_err(|e| {
                    plan_datafusion_err!(
                        "Cannot get result type for decimal operation {} {} {}: {e}", self.lhs, self.op, self.rhs
                    )
                })?;
                Ok(Signature{
                    lhs,
                    rhs,
                    ret,
                })
            } else if let Some(numeric) = mathematics_numerical_coercion(self.lhs, self.rhs) {
                // Numeric arithmetic, e.g. Int32 + Int32
                Ok(Signature::uniform(numeric))
            } else {
                plan_err!(
                    "Cannot coerce arithmetic expression {} {} {} to valid types", self.lhs, self.op, self.rhs
                )
            }
        },
        IntegerDivide | Arrow | LongArrow | HashArrow | HashLongArrow
        | HashMinus | AtQuestion | Question | QuestionAnd | QuestionPipe => {
            not_impl_err!("Operator {} is not yet supported", self.op)
        }
    };
        result.map_err(|err| {
            let diagnostic =
                Diagnostic::new_error("expressions have incompatible types", self.span())
                    .with_note(format!("has type {}", self.lhs), self.lhs_spans.first())
                    .with_note(format!("has type {}", self.rhs), self.rhs_spans.first());
            err.with_diagnostic(diagnostic)
        })
    }

    /// Returns the resulting type of a binary expression evaluating the `op` with the left and right hand types
    pub fn get_result_type(&'a self) -> Result<DataType> {
        self.signature().map(|sig| sig.ret)
    }

    /// Returns the coerced input types for a binary expression evaluating the `op` with the left and right hand types
    pub fn get_input_types(&'a self) -> Result<(DataType, DataType)> {
        self.signature().map(|sig| (sig.lhs, sig.rhs))
    }
}

// TODO Move the rest inside of BinaryTypeCoercer

/// Coercion rules for mathematics operators between decimal and non-decimal types.
fn math_decimal_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<(DataType, DataType)> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        (Dictionary(_, value_type), _) => {
            let (value_type, rhs_type) = math_decimal_coercion(value_type, rhs_type)?;
            Some((value_type, rhs_type))
        }
        (_, Dictionary(_, value_type)) => {
            let (lhs_type, value_type) = math_decimal_coercion(lhs_type, value_type)?;
            Some((lhs_type, value_type))
        }
        (Null, dec_type @ Decimal128(_, _)) | (dec_type @ Decimal128(_, _), Null) => {
            Some((dec_type.clone(), dec_type.clone()))
        }
        (Decimal128(_, _), Decimal128(_, _)) | (Decimal256(_, _), Decimal256(_, _)) => {
            Some((lhs_type.clone(), rhs_type.clone()))
        }
        // Unlike with comparison we don't coerce to a decimal in the case of floating point
        // numbers, instead falling back to floating point arithmetic instead
        (Decimal128(_, _), Int8 | Int16 | Int32 | Int64) => {
            Some((lhs_type.clone(), coerce_numeric_type_to_decimal(rhs_type)?))
        }
        (Int8 | Int16 | Int32 | Int64, Decimal128(_, _)) => {
            Some((coerce_numeric_type_to_decimal(lhs_type)?, rhs_type.clone()))
        }
        (Decimal256(_, _), Int8 | Int16 | Int32 | Int64) => Some((
            lhs_type.clone(),
            coerce_numeric_type_to_decimal256(rhs_type)?,
        )),
        (Int8 | Int16 | Int32 | Int64, Decimal256(_, _)) => Some((
            coerce_numeric_type_to_decimal256(lhs_type)?,
            rhs_type.clone(),
        )),
        _ => None,
    }
}

/// Returns the output type of applying bitwise operations such as
/// `&`, `|`, or `xor`to arguments of `lhs_type` and `rhs_type`.
fn bitwise_coercion(left_type: &DataType, right_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    if !both_numeric_or_null_and_numeric(left_type, right_type) {
        return None;
    }

    if left_type == right_type {
        return Some(left_type.clone());
    }

    match (left_type, right_type) {
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int64, _)
        | (_, Int64)
        | (UInt32, Int8)
        | (Int8, UInt32)
        | (UInt32, Int16)
        | (Int16, UInt32)
        | (UInt32, Int32)
        | (Int32, UInt32) => Some(Int64),
        (Int32, _)
        | (_, Int32)
        | (UInt16, Int16)
        | (Int16, UInt16)
        | (UInt16, Int8)
        | (Int8, UInt16) => Some(Int32),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum TypeCategory {
    Array,
    Boolean,
    Numeric,
    // String, well-defined type, but are considered as unknown type.
    DateTime,
    Composite,
    Unknown,
    NotSupported,
}

impl From<&DataType> for TypeCategory {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            // Dict is a special type in arrow, we check the value type
            DataType::Dictionary(_, v) => {
                let v = v.as_ref();
                TypeCategory::from(v)
            }
            _ => {
                if data_type.is_numeric() {
                    return TypeCategory::Numeric;
                }

                if matches!(data_type, DataType::Boolean) {
                    return TypeCategory::Boolean;
                }

                if matches!(
                    data_type,
                    DataType::List(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::LargeList(_)
                ) {
                    return TypeCategory::Array;
                }

                // It is categorized as unknown type because the type will be resolved later on
                if matches!(
                    data_type,
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Null
                ) {
                    return TypeCategory::Unknown;
                }

                if matches!(
                    data_type,
                    DataType::Date32
                        | DataType::Date64
                        | DataType::Time32(_)
                        | DataType::Time64(_)
                        | DataType::Timestamp(_, _)
                        | DataType::Interval(_)
                        | DataType::Duration(_)
                ) {
                    return TypeCategory::DateTime;
                }

                if matches!(
                    data_type,
                    DataType::Map(_, _) | DataType::Struct(_) | DataType::Union(_, _)
                ) {
                    return TypeCategory::Composite;
                }

                TypeCategory::NotSupported
            }
        }
    }
}

/// Coerce dissimilar data types to a single data type.
/// UNION, INTERSECT, EXCEPT, CASE, ARRAY, VALUES, and the GREATEST and LEAST functions are
/// examples that has the similar resolution rules.
/// See <https://www.postgresql.org/docs/current/typeconv-union-case.html> for more information.
/// The rules in the document provide a clue, but adhering strictly to them doesn't precisely
/// align with the behavior of Postgres. Therefore, we've made slight adjustments to the rules
/// to better match the behavior of both Postgres and DuckDB. For example, we expect adjusted
/// decimal precision and scale when coercing decimal types.
///
/// This function doesn't preserve correct field name and nullability for the struct type, we only care about data type.
///
/// Returns Option because we might want to continue on the code even if the data types are not coercible to the common type
pub fn type_union_resolution(data_types: &[DataType]) -> Option<DataType> {
    if data_types.is_empty() {
        return None;
    }

    // If all the data_types is the same return first one
    if data_types.iter().all(|t| t == &data_types[0]) {
        return Some(data_types[0].clone());
    }

    // If all the data_types are null, return string
    if data_types.iter().all(|t| t == &DataType::Null) {
        return Some(DataType::Utf8);
    }

    // Ignore Nulls, if any data_type category is not the same, return None
    let data_types_category: Vec<TypeCategory> = data_types
        .iter()
        .filter(|&t| t != &DataType::Null)
        .map(|t| t.into())
        .collect();

    if data_types_category
        .iter()
        .any(|t| t == &TypeCategory::NotSupported)
    {
        return None;
    }

    // Check if there is only one category excluding Unknown
    let categories: HashSet<TypeCategory> = HashSet::from_iter(
        data_types_category
            .iter()
            .filter(|&c| c != &TypeCategory::Unknown)
            .cloned(),
    );
    if categories.len() > 1 {
        return None;
    }

    // Ignore Nulls
    let mut candidate_type: Option<DataType> = None;
    for data_type in data_types.iter() {
        if data_type == &DataType::Null {
            continue;
        }
        if let Some(ref candidate_t) = candidate_type {
            // Find candidate type that all the data types can be coerced to
            // Follows the behavior of Postgres and DuckDB
            // Coerced type may be different from the candidate and current data type
            // For example,
            //  i64 and decimal(7, 2) are expect to get coerced type decimal(22, 2)
            //  numeric string ('1') and numeric (2) are expect to get coerced type numeric (1, 2)
            if let Some(t) = type_union_resolution_coercion(data_type, candidate_t) {
                candidate_type = Some(t);
            } else {
                return None;
            }
        } else {
            candidate_type = Some(data_type.clone());
        }
    }

    candidate_type
}

/// Coerce `lhs_type` and `rhs_type` to a common type for [type_union_resolution]
/// See [type_union_resolution] for more information.
fn type_union_resolution_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(lhs_index_type, lhs_value_type),
            DataType::Dictionary(rhs_index_type, rhs_value_type),
        ) => {
            let new_index_type =
                type_union_resolution_coercion(lhs_index_type, rhs_index_type);
            let new_value_type =
                type_union_resolution_coercion(lhs_value_type, rhs_value_type);
            if let (Some(new_index_type), Some(new_value_type)) =
                (new_index_type, new_value_type)
            {
                Some(DataType::Dictionary(
                    Box::new(new_index_type),
                    Box::new(new_value_type),
                ))
            } else {
                None
            }
        }
        (DataType::Dictionary(index_type, value_type), other_type)
        | (other_type, DataType::Dictionary(index_type, value_type)) => {
            match type_union_resolution_coercion(value_type, other_type) {
                // Dict with View type is redundant, use value type instead
                // TODO: Add binary view, list view with tests
                Some(DataType::Utf8View) => Some(DataType::Utf8View),
                Some(new_value_type) => Some(DataType::Dictionary(
                    index_type.clone(),
                    Box::new(new_value_type),
                )),
                None => None,
            }
        }
        (DataType::Struct(lhs), DataType::Struct(rhs)) => {
            if lhs.len() != rhs.len() {
                return None;
            }

            // Search the field in the right hand side with the SAME field name
            fn search_corresponding_coerced_type(
                lhs_field: &FieldRef,
                rhs: &Fields,
            ) -> Option<DataType> {
                for rhs_field in rhs.iter() {
                    if lhs_field.name() == rhs_field.name() {
                        if let Some(t) = type_union_resolution_coercion(
                            lhs_field.data_type(),
                            rhs_field.data_type(),
                        ) {
                            return Some(t);
                        } else {
                            return None;
                        }
                    }
                }

                None
            }

            let coerced_types = lhs
                .iter()
                .map(|lhs_field| search_corresponding_coerced_type(lhs_field, rhs))
                .collect::<Option<Vec<_>>>()?;

            // preserve the field name and nullability
            let orig_fields = std::iter::zip(lhs.iter(), rhs.iter());

            let fields: Vec<FieldRef> = coerced_types
                .into_iter()
                .zip(orig_fields)
                .map(|(datatype, (lhs, rhs))| coerce_fields(datatype, lhs, rhs))
                .collect();
            Some(DataType::Struct(fields.into()))
        }
        _ => {
            // Numeric coercion is the same as comparison coercion, both find the narrowest type
            // that can accommodate both types
            binary_numeric_coercion(lhs_type, rhs_type)
                .or_else(|| list_coercion(lhs_type, rhs_type))
                .or_else(|| temporal_coercion_nonstrict_timezone(lhs_type, rhs_type))
                .or_else(|| string_coercion(lhs_type, rhs_type))
                .or_else(|| numeric_string_coercion(lhs_type, rhs_type))
                .or_else(|| binary_coercion(lhs_type, rhs_type))
        }
    }
}

/// Handle type union resolution including struct type and others.
pub fn try_type_union_resolution(data_types: &[DataType]) -> Result<Vec<DataType>> {
    let err = match try_type_union_resolution_with_struct(data_types) {
        Ok(struct_types) => return Ok(struct_types),
        Err(e) => Some(e),
    };

    if let Some(new_type) = type_union_resolution(data_types) {
        Ok(vec![new_type; data_types.len()])
    } else {
        exec_err!("Fail to find the coerced type, errors: {:?}", err)
    }
}

// Handle struct where we only change the data type but preserve the field name and nullability.
// Since field name is the key of the struct, so it shouldn't be updated to the common column name like "c0" or "c1"
pub fn try_type_union_resolution_with_struct(
    data_types: &[DataType],
) -> Result<Vec<DataType>> {
    let mut keys_string: Option<String> = None;
    for data_type in data_types {
        if let DataType::Struct(fields) = data_type {
            let keys = fields.iter().map(|f| f.name().to_owned()).join(",");
            if let Some(ref k) = keys_string {
                if *k != keys {
                    return exec_err!("Expect same keys for struct type but got mismatched pair {} and {}", *k, keys);
                }
            } else {
                keys_string = Some(keys);
            }
        } else {
            return exec_err!("Expect to get struct but got {}", data_type);
        }
    }

    let mut struct_types: Vec<DataType> = if let DataType::Struct(fields) = &data_types[0]
    {
        fields.iter().map(|f| f.data_type().to_owned()).collect()
    } else {
        return internal_err!("Struct type is checked is the previous function, so this should be unreachable");
    };

    for data_type in data_types.iter().skip(1) {
        if let DataType::Struct(fields) = data_type {
            let incoming_struct_types: Vec<DataType> =
                fields.iter().map(|f| f.data_type().to_owned()).collect();
            // The order of field is verified above
            for (lhs_type, rhs_type) in
                struct_types.iter_mut().zip(incoming_struct_types.iter())
            {
                if let Some(coerced_type) =
                    type_union_resolution_coercion(lhs_type, rhs_type)
                {
                    *lhs_type = coerced_type;
                } else {
                    return exec_err!(
                        "Fail to find the coerced type for {} and {}",
                        lhs_type,
                        rhs_type
                    );
                }
            }
        } else {
            return exec_err!("Expect to get struct but got {}", data_type);
        }
    }

    let mut final_struct_types = vec![];
    for s in data_types {
        let mut new_fields = vec![];
        if let DataType::Struct(fields) = s {
            for (i, f) in fields.iter().enumerate() {
                let field = Arc::unwrap_or_clone(Arc::clone(f))
                    .with_data_type(struct_types[i].to_owned());
                new_fields.push(Arc::new(field));
            }
        }
        final_struct_types.push(DataType::Struct(new_fields.into()))
    }

    Ok(final_struct_types)
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a
/// comparison operation
///
/// Example comparison operations are `lhs = rhs` and `lhs > rhs`
///
/// Binary comparison kernels require the two arguments to be the (exact) same
/// data type. However, users can write queries where the two arguments are
/// different data types. In such cases, the data types are automatically cast
/// (coerced) to a single data type to pass to the kernels.
///
/// # Numeric comparisons
///
/// When comparing numeric values, the lower precision type is coerced to the
/// higher precision type to avoid losing data. For example when comparing
/// `Int32` to `Int64` the coerced type is `Int64` so the `Int32` argument will
/// be cast.
///
/// # Numeric / String comparisons
///
/// When comparing numeric values and strings, both values will be coerced to
/// strings.  For example when comparing `'2' > 1`,  the arguments will be
/// coerced to `Utf8` for comparison
pub fn comparison_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type.equals_datatype(rhs_type) {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_comparison_coercion(lhs_type, rhs_type, true))
        .or_else(|| temporal_coercion_nonstrict_timezone(lhs_type, rhs_type))
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| list_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
        .or_else(|| string_temporal_coercion(lhs_type, rhs_type))
        .or_else(|| binary_coercion(lhs_type, rhs_type))
        .or_else(|| struct_coercion(lhs_type, rhs_type))
}

/// Similar to [`comparison_coercion`] but prefers numeric if compares with
/// numeric and string
///
/// # Numeric comparisons
///
/// When comparing numeric values and strings, the values will be coerced to the
/// numeric type.  For example, `'2' > 1` if `1` is an `Int32`, the arguments
/// will be coerced to `Int32`.
pub fn comparison_coercion_numeric(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion_as_numeric(lhs_type, rhs_type))
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one is numeric and one is `Utf8`/`LargeUtf8`.
fn string_numeric_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, _) if rhs_type.is_numeric() => Some(Utf8),
        (LargeUtf8, _) if rhs_type.is_numeric() => Some(LargeUtf8),
        (Utf8View, _) if rhs_type.is_numeric() => Some(Utf8View),
        (_, Utf8) if lhs_type.is_numeric() => Some(Utf8),
        (_, LargeUtf8) if lhs_type.is_numeric() => Some(LargeUtf8),
        (_, Utf8View) if lhs_type.is_numeric() => Some(Utf8View),
        _ => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one is numeric and one is `Utf8`/`LargeUtf8`.
fn string_numeric_coercion_as_numeric(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    let lhs_logical_type = NativeType::from(lhs_type);
    let rhs_logical_type = NativeType::from(rhs_type);
    if lhs_logical_type.is_numeric() && rhs_logical_type == NativeType::String {
        return Some(lhs_type.to_owned());
    }
    if rhs_logical_type.is_numeric() && lhs_logical_type == NativeType::String {
        return Some(rhs_type.to_owned());
    }

    None
}

/// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
/// where one is temporal and one is `Utf8View`/`Utf8`/`LargeUtf8`.
///
/// Note this cannot be performed in case of arithmetic as there is insufficient information
/// to correctly determine the type of argument. Consider
///
/// ```sql
/// timestamp > now() - '1 month'
/// interval > now() - '1970-01-2021'
/// ```
///
/// In the absence of a full type inference system, we can't determine the correct type
/// to parse the string argument
fn string_temporal_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    fn match_rule(l: &DataType, r: &DataType) -> Option<DataType> {
        match (l, r) {
            // Coerce Utf8View/Utf8/LargeUtf8 to Date32/Date64/Time32/Time64/Timestamp
            (Utf8, temporal) | (LargeUtf8, temporal) | (Utf8View, temporal) => {
                match temporal {
                    Date32 | Date64 => Some(temporal.clone()),
                    Time32(_) | Time64(_) => {
                        if is_time_with_valid_unit(temporal.to_owned()) {
                            Some(temporal.to_owned())
                        } else {
                            None
                        }
                    }
                    Timestamp(_, tz) => Some(Timestamp(TimeUnit::Nanosecond, tz.clone())),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    match_rule(lhs_type, rhs_type).or_else(|| match_rule(rhs_type, lhs_type))
}

/// Coerce `lhs_type` and `rhs_type` to a common type where both are numeric
pub fn binary_numeric_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    if !lhs_type.is_numeric() || !rhs_type.is_numeric() {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    if let Some(t) = decimal_coercion(lhs_type, rhs_type) {
        return Some(t);
    }

    numerical_coercion(lhs_type, rhs_type)
}

/// Decimal coercion rules.
pub fn decimal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        // Prefer decimal data type over floating point for comparison operation
        (Decimal128(_, _), Decimal128(_, _)) => {
            get_wider_decimal_type(lhs_type, rhs_type)
        }
        (Decimal128(_, _), _) => get_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal128(_, _)) => get_common_decimal_type(rhs_type, lhs_type),
        (Decimal256(_, _), Decimal256(_, _)) => {
            get_wider_decimal_type(lhs_type, rhs_type)
        }
        (Decimal256(_, _), _) => get_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal256(_, _)) => get_common_decimal_type(rhs_type, lhs_type),
        (_, _) => None,
    }
}

/// Coerce `lhs_type` and `rhs_type` to a common type.
fn get_common_decimal_type(
    decimal_type: &DataType,
    other_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match decimal_type {
        Decimal128(_, _) => {
            let other_decimal_type = coerce_numeric_type_to_decimal(other_type)?;
            get_wider_decimal_type(decimal_type, &other_decimal_type)
        }
        Decimal256(_, _) => {
            let other_decimal_type = coerce_numeric_type_to_decimal256(other_type)?;
            get_wider_decimal_type(decimal_type, &other_decimal_type)
        }
        _ => None,
    }
}

/// Returns a `DataType::Decimal128` that can store any value from either
/// `lhs_decimal_type` and `rhs_decimal_type`
///
/// The result decimal type is `(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))`.
fn get_wider_decimal_type(
    lhs_decimal_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    match (lhs_decimal_type, rhs_type) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i8 - s1).max(*p2 as i8 - s2);
            Some(create_decimal_type((range + s) as u8, s))
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i8 - s1).max(*p2 as i8 - s2);
            Some(create_decimal256_type((range + s) as u8, s))
        }
        (_, _) => None,
    }
}

/// Convert the numeric data type to the decimal data type.
/// We support signed and unsigned integer types and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    // This conversion rule is from spark
    // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
    match numeric_type {
        Int8 | UInt8 => Some(Decimal128(3, 0)),
        Int16 | UInt16 => Some(Decimal128(5, 0)),
        Int32 | UInt32 => Some(Decimal128(10, 0)),
        Int64 | UInt64 => Some(Decimal128(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        Float32 => Some(Decimal128(14, 7)),
        Float64 => Some(Decimal128(30, 15)),
        _ => None,
    }
}

/// Convert the numeric data type to the decimal data type.
/// We support signed and unsigned integer types and floating-point type.
fn coerce_numeric_type_to_decimal256(numeric_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    // This conversion rule is from spark
    // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
    match numeric_type {
        Int8 | UInt8 => Some(Decimal256(3, 0)),
        Int16 | UInt16 => Some(Decimal256(5, 0)),
        Int32 | UInt32 => Some(Decimal256(10, 0)),
        Int64 | UInt64 => Some(Decimal256(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        Float32 => Some(Decimal256(14, 7)),
        Float64 => Some(Decimal256(30, 15)),
        _ => None,
    }
}

fn struct_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Struct(lhs_fields), Struct(rhs_fields)) => {
            if lhs_fields.len() != rhs_fields.len() {
                return None;
            }

            let coerced_types = std::iter::zip(lhs_fields.iter(), rhs_fields.iter())
                .map(|(lhs, rhs)| comparison_coercion(lhs.data_type(), rhs.data_type()))
                .collect::<Option<Vec<DataType>>>()?;

            // preserve the field name and nullability
            let orig_fields = std::iter::zip(lhs_fields.iter(), rhs_fields.iter());

            let fields: Vec<FieldRef> = coerced_types
                .into_iter()
                .zip(orig_fields)
                .map(|(datatype, (lhs, rhs))| coerce_fields(datatype, lhs, rhs))
                .collect();
            Some(Struct(fields.into()))
        }
        _ => None,
    }
}

/// returns the result of coercing two fields to a common type
fn coerce_fields(common_type: DataType, lhs: &FieldRef, rhs: &FieldRef) -> FieldRef {
    let is_nullable = lhs.is_nullable() || rhs.is_nullable();
    let name = lhs.name(); // pick the name from the left field
    Arc::new(Field::new(name, common_type, is_nullable))
}

/// Returns the output type of applying mathematics operations such as
/// `+` to arguments of `lhs_type` and `rhs_type`.
fn mathematics_numerical_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // Error on any non-numeric type
    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None;
    };

    // These are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            mathematics_numerical_coercion(lhs_value_type, rhs_value_type)
        }
        (Dictionary(_, value_type), _) => {
            mathematics_numerical_coercion(value_type, rhs_type)
        }
        (_, Dictionary(_, value_type)) => {
            mathematics_numerical_coercion(lhs_type, value_type)
        }
        _ => numerical_coercion(lhs_type, rhs_type),
    }
}

/// A common set of numerical coercions that are applied for mathematical and binary ops
/// to `lhs_type` and `rhs_type`.
fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        // The following match arms encode the following logic: Given the two
        // integral types, we choose the narrowest possible integral type that
        // accommodates all values of both types. Note that to avoid information
        // loss when combining UInt64 with signed integers we use Decimal128(20, 0).
        (UInt64, Int64 | Int32 | Int16 | Int8)
        | (Int64 | Int32 | Int16 | Int8, UInt64) => Some(Decimal128(20, 0)),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int64, _)
        | (_, Int64)
        | (UInt32, Int32 | Int16 | Int8)
        | (Int32 | Int16 | Int8, UInt32) => Some(Int64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int32, _) | (_, Int32) | (UInt16, Int16 | Int8) | (Int16 | Int8, UInt16) => {
            Some(Int32)
        }
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn create_decimal_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(
        DECIMAL128_MAX_PRECISION.min(precision),
        DECIMAL128_MAX_SCALE.min(scale),
    )
}

fn create_decimal256_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal256(
        DECIMAL256_MAX_PRECISION.min(precision),
        DECIMAL256_MAX_SCALE.min(scale),
    )
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (_, Null) => lhs_type.is_numeric(),
        (Null, _) => rhs_type.is_numeric(),
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            lhs_value_type.is_numeric() && rhs_value_type.is_numeric()
        }
        (Dictionary(_, value_type), _) => {
            value_type.is_numeric() && rhs_type.is_numeric()
        }
        (_, Dictionary(_, value_type)) => {
            lhs_type.is_numeric() && value_type.is_numeric()
        }
        _ => lhs_type.is_numeric() && rhs_type.is_numeric(),
    }
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// Not all operators support dictionaries, if `preserve_dictionaries` is true
/// dictionaries will be preserved if possible
fn dictionary_comparison_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
    preserve_dictionaries: bool,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (
            Dictionary(_lhs_index_type, lhs_value_type),
            Dictionary(_rhs_index_type, rhs_value_type),
        ) => comparison_coercion(lhs_value_type, rhs_value_type),
        (d @ Dictionary(_, value_type), other_type)
        | (other_type, d @ Dictionary(_, value_type))
            if preserve_dictionaries && value_type.as_ref() == other_type =>
        {
            Some(d.clone())
        }
        (Dictionary(_index_type, value_type), _) => {
            comparison_coercion(value_type, rhs_type)
        }
        (_, Dictionary(_index_type, value_type)) => {
            comparison_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for string concat.
/// This is a union of string coercion rules and specified rules:
/// 1. At least one side of lhs and rhs should be string type (Utf8 / LargeUtf8)
/// 2. Data type of the other side should be able to cast to string type
fn string_concat_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    string_coercion(lhs_type, rhs_type).or(match (lhs_type, rhs_type) {
        (Utf8View, from_type) | (from_type, Utf8View) => {
            string_concat_internal_coercion(from_type, &Utf8View)
        }
        (Utf8, from_type) | (from_type, Utf8) => {
            string_concat_internal_coercion(from_type, &Utf8)
        }
        (LargeUtf8, from_type) | (from_type, LargeUtf8) => {
            string_concat_internal_coercion(from_type, &LargeUtf8)
        }
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            string_coercion(lhs_value_type, rhs_value_type).or(None)
        }
        _ => None,
    })
}

fn array_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type.equals_datatype(rhs_type) {
        Some(lhs_type.to_owned())
    } else {
        None
    }
}

/// If `from_type` can be casted to `to_type`, return `to_type`, otherwise
/// return `None`.
fn string_concat_internal_coercion(
    from_type: &DataType,
    to_type: &DataType,
) -> Option<DataType> {
    if can_cast_types(from_type, to_type) {
        Some(to_type.to_owned())
    } else {
        None
    }
}

/// Coercion rules for string view types (Utf8/LargeUtf8/Utf8View):
/// If at least one argument is a string view, we coerce to string view
/// based on the observation that StringArray to StringViewArray is cheap but not vice versa.
///
/// Between Utf8 and LargeUtf8, we coerce to LargeUtf8.
pub fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        // If Utf8View is in any side, we coerce to Utf8View.
        (Utf8View, Utf8View | Utf8 | LargeUtf8) | (Utf8 | LargeUtf8, Utf8View) => {
            Some(Utf8View)
        }
        // Then, if LargeUtf8 is in any side, we coerce to LargeUtf8.
        (LargeUtf8, Utf8 | LargeUtf8) | (Utf8, LargeUtf8) => Some(LargeUtf8),
        // Utf8 coerces to Utf8
        (Utf8, Utf8) => Some(Utf8),
        _ => None,
    }
}

fn numeric_string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8 | LargeUtf8, other_type) | (other_type, Utf8 | LargeUtf8)
            if other_type.is_numeric() =>
        {
            Some(other_type.clone())
        }
        _ => None,
    }
}

/// Coerces two fields together, ensuring the field data (name and nullability) is correctly set.
fn coerce_list_children(lhs_field: &FieldRef, rhs_field: &FieldRef) -> Option<FieldRef> {
    let data_types = vec![lhs_field.data_type().clone(), rhs_field.data_type().clone()];
    Some(Arc::new(
        (**lhs_field)
            .clone()
            .with_data_type(type_union_resolution(&data_types)?)
            .with_nullable(lhs_field.is_nullable() || rhs_field.is_nullable()),
    ))
}

/// Coercion rules for list types.
fn list_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        // Coerce to the left side FixedSizeList type if the list lengths are the same,
        // otherwise coerce to list with the left type for dynamic length
        (FixedSizeList(lhs_field, ls), FixedSizeList(rhs_field, rs)) => {
            if ls == rs {
                Some(FixedSizeList(
                    coerce_list_children(lhs_field, rhs_field)?,
                    *rs,
                ))
            } else {
                Some(List(coerce_list_children(lhs_field, rhs_field)?))
            }
        }
        // LargeList on any side
        (
            LargeList(lhs_field),
            List(rhs_field) | LargeList(rhs_field) | FixedSizeList(rhs_field, _),
        )
        | (List(lhs_field) | FixedSizeList(lhs_field, _), LargeList(rhs_field)) => {
            Some(LargeList(coerce_list_children(lhs_field, rhs_field)?))
        }
        // Lists on both sides
        (List(lhs_field), List(rhs_field) | FixedSizeList(rhs_field, _))
        | (FixedSizeList(lhs_field, _), List(rhs_field)) => {
            Some(List(coerce_list_children(lhs_field, rhs_field)?))
        }
        _ => None,
    }
}

/// Coercion rules for binary (Binary/LargeBinary) to string (Utf8/LargeUtf8):
/// If one argument is binary and the other is a string then coerce to string
/// (e.g. for `like`)
pub fn binary_to_string_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Binary, Utf8) => Some(Utf8),
        (Binary, LargeUtf8) => Some(LargeUtf8),
        (BinaryView, Utf8) => Some(Utf8View),
        (BinaryView, LargeUtf8) => Some(LargeUtf8),
        (LargeBinary, Utf8) => Some(LargeUtf8),
        (LargeBinary, LargeUtf8) => Some(LargeUtf8),
        (Utf8, Binary) => Some(Utf8),
        (Utf8, LargeBinary) => Some(LargeUtf8),
        (Utf8, BinaryView) => Some(Utf8View),
        (LargeUtf8, Binary) => Some(LargeUtf8),
        (LargeUtf8, LargeBinary) => Some(LargeUtf8),
        (LargeUtf8, BinaryView) => Some(LargeUtf8),
        _ => None,
    }
}

/// Coercion rules for binary types (Binary/LargeBinary/BinaryView): If at least one argument is
/// a binary type and both arguments can be coerced into a binary type, coerce
/// to binary type.
fn binary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        // If BinaryView is in any side, we coerce to BinaryView.
        (BinaryView, BinaryView | Binary | LargeBinary | Utf8 | LargeUtf8 | Utf8View)
        | (LargeBinary | Binary | Utf8 | LargeUtf8 | Utf8View, BinaryView) => {
            Some(BinaryView)
        }
        // Prefer LargeBinary over Binary
        (LargeBinary | Binary | Utf8 | LargeUtf8 | Utf8View, LargeBinary)
        | (LargeBinary, Binary | Utf8 | LargeUtf8 | Utf8View) => Some(LargeBinary),

        // If Utf8View/LargeUtf8 presents need to be large Binary
        (Utf8View | LargeUtf8, Binary) | (Binary, Utf8View | LargeUtf8) => {
            Some(LargeBinary)
        }
        (Binary, Utf8) | (Utf8, Binary) => Some(Binary),
        _ => None,
    }
}

/// Coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| list_coercion(lhs_type, rhs_type))
        .or_else(|| binary_to_string_coercion(lhs_type, rhs_type))
        .or_else(|| dictionary_comparison_coercion(lhs_type, rhs_type, false))
        .or_else(|| regex_null_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// Coercion rules for regular expression comparison operations with NULL input.
fn regex_null_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Null, Utf8View | Utf8 | LargeUtf8) => Some(rhs_type.clone()),
        (Utf8View | Utf8 | LargeUtf8, Null) => Some(lhs_type.clone()),
        (Null, Null) => Some(Utf8),
        _ => None,
    }
}

/// Coercion rules for regular expression comparison operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn regex_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_comparison_coercion(lhs_type, rhs_type, false))
        .or_else(|| regex_null_coercion(lhs_type, rhs_type))
}

/// Checks if the TimeUnit associated with a Time32 or Time64 type is consistent,
/// as Time32 can only be used to Second and Millisecond accuracy, while Time64
/// is exclusively used to Microsecond and Nanosecond accuracy
fn is_time_with_valid_unit(datatype: DataType) -> bool {
    matches!(
        datatype,
        DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
    )
}

/// Non-strict Timezone Coercion is useful in scenarios where we can guarantee
/// a stable relationship between two timestamps of different timezones.
///
/// An example of this is binary comparisons (<, >, ==, etc). Arrow stores timestamps
/// as relative to UTC epoch, and then adds the timezone as an offset. As a result, we can always
/// do a binary comparison between the two times.
///
/// Timezone coercion is handled by the following rules:
/// - If only one has a timezone, coerce the other to match
/// - If both have a timezone, coerce to the left type
/// - "UTC" and "+00:00" are considered equivalent
fn temporal_coercion_nonstrict_timezone(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // If both have a timezone, use the left timezone.
                (Some(lhs_tz), Some(_rhs_tz)) => Some(Arc::clone(lhs_tz)),
                (Some(lhs_tz), None) => Some(Arc::clone(lhs_tz)),
                (None, Some(rhs_tz)) => Some(Arc::clone(rhs_tz)),
                (None, None) => None,
            };

            let unit = timeunit_coercion(lhs_unit, rhs_unit);

            Some(Timestamp(unit, tz))
        }
        _ => temporal_coercion(lhs_type, rhs_type),
    }
}

/// Strict Timezone coercion is useful in scenarios where we cannot guarantee a stable relationship
/// between two timestamps with different timezones or do not want implicit coercion between them.
///
/// An example of this when attempting to coerce function arguments. Functions already have a mechanism
/// for defining which timestamp types they want to support, so we do not want to do any further coercion.
///
/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
/// For interval arithmetic, it doesn't handle datetime type +/- interval
/// Timezone coercion is handled by the following rules:
/// - If only one has a timezone, coerce the other to match
/// - If both have a timezone, throw an error
/// - "UTC" and "+00:00" are considered equivalent
fn temporal_coercion_strict_timezone(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    match (lhs_type, rhs_type) {
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                (Some(lhs_tz), Some(rhs_tz)) => {
                    match (lhs_tz.as_ref(), rhs_tz.as_ref()) {
                        // UTC and "+00:00" are the same by definition. Most other timezones
                        // do not have a 1-1 mapping between timezone and an offset from UTC
                        ("UTC", "+00:00") | ("+00:00", "UTC") => Some(Arc::clone(lhs_tz)),
                        (lhs, rhs) if lhs == rhs => Some(Arc::clone(lhs_tz)),
                        // can't cast across timezones
                        _ => {
                            return None;
                        }
                    }
                }
                (Some(lhs_tz), None) => Some(Arc::clone(lhs_tz)),
                (None, Some(rhs_tz)) => Some(Arc::clone(rhs_tz)),
                (None, None) => None,
            };

            let unit = timeunit_coercion(lhs_unit, rhs_unit);

            Some(Timestamp(unit, tz))
        }
        _ => temporal_coercion(lhs_type, rhs_type),
    }
}

fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::IntervalUnit::*;
    use arrow::datatypes::TimeUnit::*;

    match (lhs_type, rhs_type) {
        (Interval(_) | Duration(_), Interval(_) | Duration(_)) => {
            Some(Interval(MonthDayNano))
        }
        (Date64, Date32) | (Date32, Date64) => Some(Date64),
        (Timestamp(_, None), Date64) | (Date64, Timestamp(_, None)) => {
            Some(Timestamp(Nanosecond, None))
        }
        (Timestamp(_, _tz), Date64) | (Date64, Timestamp(_, _tz)) => {
            Some(Timestamp(Nanosecond, None))
        }
        (Timestamp(_, None), Date32) | (Date32, Timestamp(_, None)) => {
            Some(Timestamp(Nanosecond, None))
        }
        (Timestamp(_, _tz), Date32) | (Date32, Timestamp(_, _tz)) => {
            Some(Timestamp(Nanosecond, None))
        }
        _ => None,
    }
}

fn timeunit_coercion(lhs_unit: &TimeUnit, rhs_unit: &TimeUnit) -> TimeUnit {
    use arrow::datatypes::TimeUnit::*;
    match (lhs_unit, rhs_unit) {
        (Second, Millisecond) => Second,
        (Second, Microsecond) => Second,
        (Second, Nanosecond) => Second,
        (Millisecond, Second) => Second,
        (Millisecond, Microsecond) => Millisecond,
        (Millisecond, Nanosecond) => Millisecond,
        (Microsecond, Second) => Second,
        (Microsecond, Millisecond) => Millisecond,
        (Microsecond, Nanosecond) => Microsecond,
        (Nanosecond, Second) => Second,
        (Nanosecond, Millisecond) => Millisecond,
        (Nanosecond, Microsecond) => Microsecond,
        (l, r) => {
            assert_eq!(l, r);
            *l
        }
    }
}

/// Coercion rules from NULL type. Since NULL can be casted to any other type in arrow,
/// either lhs or rhs is NULL, if NULL can be casted to type of the other side, the coercion is valid.
fn null_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (DataType::Null, other_type) | (other_type, DataType::Null) => {
            if can_cast_types(&DataType::Null, other_type) {
                Some(other_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_common::assert_contains;

    #[test]
    fn test_coercion_error() -> Result<()> {
        let coercer =
            BinaryTypeCoercer::new(&DataType::Float32, &Operator::Plus, &DataType::Utf8);
        let result_type = coercer.get_input_types();

        let e = result_type.unwrap_err();
        assert_eq!(e.strip_backtrace(), "Error during planning: Cannot coerce arithmetic expression Float32 + Utf8 to valid types");
        Ok(())
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal128(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 10),
            DataType::Decimal128(20, 8),
            DataType::Null,
        ];
        let result_types = [
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(23, 3),
            DataType::Decimal128(24, 7),
            DataType::Decimal128(32, 15),
            DataType::Decimal128(38, 10),
            DataType::Decimal128(25, 8),
            DataType::Decimal128(20, 3),
        ];
        let comparison_op_types = [
            Operator::NotEq,
            Operator::Eq,
            Operator::Gt,
            Operator::GtEq,
            Operator::Lt,
            Operator::LtEq,
        ];
        for (i, input_type) in input_types.iter().enumerate() {
            let expect_type = &result_types[i];
            for op in comparison_op_types {
                let (lhs, rhs) = BinaryTypeCoercer::new(&input_decimal, &op, input_type)
                    .get_input_types()?;
                assert_eq!(expect_type, &lhs);
                assert_eq!(expect_type, &rhs);
            }
        }
        // negative test
        let result_type =
            BinaryTypeCoercer::new(&input_decimal, &Operator::Eq, &DataType::Boolean)
                .get_input_types();
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal128(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal128(30, 15)
        );
    }

    #[test]
    fn test_dictionary_type_coercion() {
        use DataType::*;

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
            Some(Int32)
        );
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
            Some(Int32)
        );

        // Since we can coerce values of Int16 to Utf8 can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
            Some(Utf8)
        );

        // Since we can coerce values of Utf8 to Binary can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
            Some(Binary)
        );

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
            Some(Utf8)
        );
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
            Some(lhs_type.clone())
        );

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, false),
            Some(Utf8)
        );
        assert_eq!(
            dictionary_comparison_coercion(&lhs_type, &rhs_type, true),
            Some(rhs_type.clone())
        );
    }

    /// Test coercion rules for binary operators
    ///
    /// Applies coercion rules for `$LHS_TYPE $OP $RHS_TYPE` and asserts that
    /// the result type is `$RESULT_TYPE`
    macro_rules! test_coercion_binary_rule {
        ($LHS_TYPE:expr, $RHS_TYPE:expr, $OP:expr, $RESULT_TYPE:expr) => {{
            let (lhs, rhs) =
                BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &$RHS_TYPE).get_input_types()?;
            assert_eq!(lhs, $RESULT_TYPE);
            assert_eq!(rhs, $RESULT_TYPE);
        }};
    }

    /// Test coercion rules for binary operators
    ///
    /// Applies coercion rules for each RHS_TYPE in $RHS_TYPES such that
    /// `$LHS_TYPE $OP RHS_TYPE` and asserts that the result type is `$RESULT_TYPE`.
    /// Also tests that the inverse `RHS_TYPE $OP $LHS_TYPE` is true
    macro_rules! test_coercion_binary_rule_multiple {
        ($LHS_TYPE:expr, $RHS_TYPES:expr, $OP:expr, $RESULT_TYPE:expr) => {{
            for rh_type in $RHS_TYPES {
                let (lhs, rhs) = BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &rh_type)
                    .get_input_types()?;
                assert_eq!(lhs, $RESULT_TYPE);
                assert_eq!(rhs, $RESULT_TYPE);

                BinaryTypeCoercer::new(&rh_type, &$OP, &$LHS_TYPE).get_input_types()?;
                assert_eq!(lhs, $RESULT_TYPE);
                assert_eq!(rhs, $RESULT_TYPE);
            }
        }};
    }

    /// Test coercion rules for like
    ///
    /// Applies coercion rules for both
    /// * `$LHS_TYPE LIKE $RHS_TYPE`
    /// * `$RHS_TYPE LIKE $LHS_TYPE`
    ///
    /// And asserts the result type is `$RESULT_TYPE`
    macro_rules! test_like_rule {
        ($LHS_TYPE:expr, $RHS_TYPE:expr, $RESULT_TYPE:expr) => {{
            println!("Coercing {} LIKE {}", $LHS_TYPE, $RHS_TYPE);
            let result = like_coercion(&$LHS_TYPE, &$RHS_TYPE);
            assert_eq!(result, $RESULT_TYPE);
            // reverse the order
            let result = like_coercion(&$RHS_TYPE, &$LHS_TYPE);
            assert_eq!(result, $RESULT_TYPE);
        }};
    }

    #[test]
    fn test_date_timestamp_arithmetic_error() -> Result<()> {
        let (lhs, rhs) = BinaryTypeCoercer::new(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &Operator::Minus,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .get_input_types()?;
        assert_eq!(lhs.to_string(), "Timestamp(Millisecond, None)");
        assert_eq!(rhs.to_string(), "Timestamp(Millisecond, None)");

        let err =
            BinaryTypeCoercer::new(&DataType::Date32, &Operator::Plus, &DataType::Date64)
                .get_input_types()
                .unwrap_err()
                .to_string();

        assert_contains!(
            &err,
            "Cannot get result type for temporal operation Date64 + Date64"
        );

        Ok(())
    }

    #[test]
    fn test_like_coercion() {
        // string coerce to strings
        test_like_rule!(DataType::Utf8, DataType::Utf8, Some(DataType::Utf8));
        test_like_rule!(
            DataType::LargeUtf8,
            DataType::Utf8,
            Some(DataType::LargeUtf8)
        );
        test_like_rule!(
            DataType::Utf8,
            DataType::LargeUtf8,
            Some(DataType::LargeUtf8)
        );
        test_like_rule!(
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            Some(DataType::LargeUtf8)
        );

        // Also coerce binary to strings
        test_like_rule!(DataType::Binary, DataType::Utf8, Some(DataType::Utf8));
        test_like_rule!(
            DataType::LargeBinary,
            DataType::Utf8,
            Some(DataType::LargeUtf8)
        );
        test_like_rule!(
            DataType::Binary,
            DataType::LargeUtf8,
            Some(DataType::LargeUtf8)
        );
        test_like_rule!(
            DataType::LargeBinary,
            DataType::LargeUtf8,
            Some(DataType::LargeUtf8)
        );
    }

    #[test]
    fn test_type_coercion() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date32,
            Operator::Eq,
            DataType::Date32
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date64,
            Operator::Lt,
            DataType::Date64
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Second),
            Operator::Eq,
            DataType::Time32(TimeUnit::Second)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Millisecond),
            Operator::Eq,
            DataType::Time32(TimeUnit::Millisecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Microsecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Nanosecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Nanosecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8View,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8View,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8View,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8View,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8View,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Utf8View,
            DataType::Utf8View,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8View,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8View,
            Operator::RegexMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8,
            Operator::RegexIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8View,
            Operator::RegexIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8View,
            Operator::RegexIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8View,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8View,
            Operator::RegexNotMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8View,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
            DataType::Utf8View,
            Operator::RegexNotIMatch,
            DataType::Utf8View
        );
        test_coercion_binary_rule!(
            DataType::Int16,
            DataType::Int64,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::UInt64,
            Operator::BitwiseAnd,
            DataType::UInt64
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::Int32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::Int16,
            Operator::BitwiseAnd,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        Ok(())
    }

    #[test]
    fn test_type_coercion_arithmetic() -> Result<()> {
        use DataType::*;

        // (Float64, _) | (_, Float64) => Some(Float64),
        test_coercion_binary_rule_multiple!(
            Float64,
            [
                Float64, Float32, Float16, Int64, UInt64, Int32, UInt32, Int16, UInt16,
                Int8, UInt8
            ],
            Operator::Plus,
            Float64
        );
        // (_, Float32) | (Float32, _) => Some(Float32),
        test_coercion_binary_rule_multiple!(
            Float32,
            [
                Float32, Float16, Int64, UInt64, Int32, UInt32, Int16, UInt16, Int8,
                UInt8
            ],
            Operator::Plus,
            Float32
        );
        // (UInt64, Int64 | Int32 | Int16 | Int8) | (Int64 | Int32 | Int16 | Int8, UInt64)  => Some(Decimal128(20, 0)),
        test_coercion_binary_rule_multiple!(
            UInt64,
            [Int64, Int32, Int16, Int8],
            Operator::Divide,
            Decimal128(20, 0)
        );
        // (UInt64, _) | (_, UInt64) => Some(UInt64),
        test_coercion_binary_rule_multiple!(
            UInt64,
            [UInt64, UInt32, UInt16, UInt8],
            Operator::Modulo,
            UInt64
        );
        // (Int64, _) | (_, Int64) => Some(Int64),
        test_coercion_binary_rule_multiple!(
            Int64,
            [Int64, Int32, UInt32, Int16, UInt16, Int8, UInt8],
            Operator::Modulo,
            Int64
        );
        // (UInt32, Int32 | Int16 | Int8) | (Int32 | Int16 | Int8, UInt32) => Some(Int64)
        test_coercion_binary_rule_multiple!(
            UInt32,
            [Int32, Int16, Int8],
            Operator::Modulo,
            Int64
        );
        // (UInt32, _) | (_, UInt32) => Some(UInt32),
        test_coercion_binary_rule_multiple!(
            UInt32,
            [UInt32, UInt16, UInt8],
            Operator::Modulo,
            UInt32
        );
        // (Int32, _) | (_, Int32) => Some(Int32),
        test_coercion_binary_rule_multiple!(
            Int32,
            [Int32, Int16, Int8],
            Operator::Modulo,
            Int32
        );
        // (UInt16, Int16 | Int8) | (Int16 | Int8, UInt16) => Some(Int32)
        test_coercion_binary_rule_multiple!(
            UInt16,
            [Int16, Int8],
            Operator::Minus,
            Int32
        );
        // (UInt16, _) | (_, UInt16) => Some(UInt16),
        test_coercion_binary_rule_multiple!(
            UInt16,
            [UInt16, UInt8, UInt8],
            Operator::Plus,
            UInt16
        );
        // (Int16, _) | (_, Int16) => Some(Int16),
        test_coercion_binary_rule_multiple!(Int16, [Int16, Int8], Operator::Plus, Int16);
        // (UInt8, Int8) | (Int8, UInt8) => Some(Int16)
        test_coercion_binary_rule!(Int8, UInt8, Operator::Minus, Int16);
        test_coercion_binary_rule!(UInt8, Int8, Operator::Multiply, Int16);
        // (UInt8, _) | (_, UInt8) => Some(UInt8),
        test_coercion_binary_rule!(UInt8, UInt8, Operator::Minus, UInt8);
        // (Int8, _) | (_, Int8) => Some(Int8),
        test_coercion_binary_rule!(Int8, Int8, Operator::Plus, Int8);

        Ok(())
    }

    fn test_math_decimal_coercion_rule(
        lhs_type: DataType,
        rhs_type: DataType,
        expected_lhs_type: DataType,
        expected_rhs_type: DataType,
    ) {
        // The coerced types for lhs and rhs, if any of them is not decimal
        let (lhs_type, rhs_type) = math_decimal_coercion(&lhs_type, &rhs_type).unwrap();
        assert_eq!(lhs_type, expected_lhs_type);
        assert_eq!(rhs_type, expected_rhs_type);
    }

    #[test]
    fn test_coercion_arithmetic_decimal() -> Result<()> {
        test_math_decimal_coercion_rule(
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 0),
            DataType::Decimal128(10, 2),
        );

        Ok(())
    }

    #[test]
    fn test_type_coercion_compare() -> Result<()> {
        // boolean
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Eq,
            DataType::Boolean
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int64,
            Operator::Eq,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::GtEq,
            DataType::Float64
        );
        // signed integer
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int64
        );
        // unsigned integer
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt8,
            Operator::Gt,
            DataType::UInt32
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::UInt8,
            Operator::Eq,
            DataType::UInt64
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::Int64,
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        // numeric/decimal
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 2),
            Operator::Lt,
            DataType::Decimal128(22, 2)
        );
        test_coercion_binary_rule!(
            DataType::Float64,
            DataType::Decimal128(10, 3),
            Operator::Gt,
            DataType::Decimal128(30, 15)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Decimal128(14, 2),
            DataType::Decimal128(10, 3),
            Operator::GtEq,
            DataType::Decimal128(15, 3)
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::Decimal128(20, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );

        // Binary
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::Binary,
            Operator::Eq,
            DataType::Binary
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Binary,
            Operator::Eq,
            DataType::Binary
        );
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::Utf8,
            Operator::Eq,
            DataType::Binary
        );

        // LargeBinary
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::Binary,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::Binary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::Utf8,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeUtf8,
            DataType::LargeBinary,
            Operator::Eq,
            DataType::LargeBinary
        );
        test_coercion_binary_rule!(
            DataType::LargeBinary,
            DataType::LargeUtf8,
            Operator::Eq,
            DataType::LargeBinary
        );

        // Timestamps
        let utc: Option<Arc<str>> = Some("UTC".into());
        test_coercion_binary_rule!(
            DataType::Timestamp(TimeUnit::Second, utc.clone()),
            DataType::Timestamp(TimeUnit::Second, utc.clone()),
            Operator::Eq,
            DataType::Timestamp(TimeUnit::Second, utc.clone())
        );
        test_coercion_binary_rule!(
            DataType::Timestamp(TimeUnit::Second, utc.clone()),
            DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
            Operator::Eq,
            DataType::Timestamp(TimeUnit::Second, utc.clone())
        );
        test_coercion_binary_rule!(
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
            Operator::Eq,
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()))
        );
        test_coercion_binary_rule!(
            DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
            DataType::Timestamp(TimeUnit::Second, utc),
            Operator::Eq,
            DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into()))
        );

        // list
        let inner_field = Arc::new(Field::new_list_field(DataType::Int64, true));
        test_coercion_binary_rule!(
            DataType::List(Arc::clone(&inner_field)),
            DataType::List(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::List(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::List(Arc::clone(&inner_field)),
            DataType::LargeList(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::LargeList(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::LargeList(Arc::clone(&inner_field)),
            DataType::List(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::LargeList(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::LargeList(Arc::clone(&inner_field)),
            DataType::LargeList(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::LargeList(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            Operator::Eq,
            DataType::FixedSizeList(Arc::clone(&inner_field), 10)
        );
        test_coercion_binary_rule!(
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            DataType::LargeList(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::LargeList(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::LargeList(Arc::clone(&inner_field)),
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            Operator::Eq,
            DataType::LargeList(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::List(Arc::clone(&inner_field)),
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            Operator::Eq,
            DataType::List(Arc::clone(&inner_field))
        );
        test_coercion_binary_rule!(
            DataType::FixedSizeList(Arc::clone(&inner_field), 10),
            DataType::List(Arc::clone(&inner_field)),
            Operator::Eq,
            DataType::List(Arc::clone(&inner_field))
        );

        // Negative test: inner_timestamp_field and inner_field are not compatible because their inner types are not compatible
        let inner_timestamp_field = Arc::new(Field::new_list_field(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let result_type = BinaryTypeCoercer::new(
            &DataType::List(Arc::clone(&inner_field)),
            &Operator::Eq,
            &DataType::List(Arc::clone(&inner_timestamp_field)),
        )
        .get_input_types();
        assert!(result_type.is_err());

        // TODO add other data type
        Ok(())
    }

    #[test]
    fn test_list_coercion() {
        let lhs_type = DataType::List(Arc::new(Field::new("lhs", DataType::Int8, false)));

        let rhs_type = DataType::List(Arc::new(Field::new("rhs", DataType::Int64, true)));

        let coerced_type = list_coercion(&lhs_type, &rhs_type).unwrap();
        assert_eq!(
            coerced_type,
            DataType::List(Arc::new(Field::new("lhs", DataType::Int64, true)))
        ); // nullable because the RHS is nullable
    }

    #[test]
    fn test_type_coercion_logical_op() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );

        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        Ok(())
    }
}

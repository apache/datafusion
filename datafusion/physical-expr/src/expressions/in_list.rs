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

//! Implementation of `InList` expressions: [`InListExpr`]

use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::physical_expr::physical_exprs_bag_equal;

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::kernels::boolean::{not, or_kleene};
use arrow::compute::{SortOptions, take};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::hash_utils::with_hashes;
use datafusion_common::{
    DFSchema, HashSet, Result, ScalarValue, assert_or_internal_err, exec_datafusion_err,
    exec_err,
};
use datafusion_expr::{ColumnarValue, expr_vec_fmt};

use ahash::RandomState;
use datafusion_common::HashMap;
use hashbrown::hash_map::RawEntryMut;

/// Trait for InList static filters
trait StaticFilter {
    fn null_count(&self) -> usize;

    /// Checks if values in `v` are contained in the filter
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
}

/// InList
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
    static_filter: Option<Arc<dyn StaticFilter + Send + Sync>>,
}

impl Debug for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("InListExpr")
            .field("expr", &self.expr)
            .field("list", &self.list)
            .field("negated", &self.negated)
            .finish()
    }
}

/// Static filter for InList that stores the array and hash set for O(1) lookups
#[derive(Debug, Clone)]
struct ArrayStaticFilter {
    in_array: ArrayRef,
    state: RandomState,
    /// Used to provide a lookup from value to in list index
    ///
    /// Note: usize::hash is not used, instead the raw entry
    /// API is used to store entries w.r.t their value
    map: HashMap<usize, (), ()>,
}

impl StaticFilter for ArrayStaticFilter {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    /// Checks if values in `v` are contained in the `in_array` using this hash set for lookup.
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        // Null type comparisons always return null (SQL three-valued logic)
        if v.data_type() == &DataType::Null
            || self.in_array.data_type() == &DataType::Null
        {
            let nulls = NullBuffer::new_null(v.len());
            return Ok(BooleanArray::new(
                BooleanBuffer::new_unset(v.len()),
                Some(nulls),
            ));
        }

        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(downcast_array(result.as_ref()))
            }
            _ => {}
        }

        let needle_nulls = v.logical_nulls();
        let needle_nulls = needle_nulls.as_ref();
        let haystack_has_nulls = self.in_array.null_count() != 0;

        with_hashes([v], &self.state, |hashes| {
            let cmp = make_comparator(v, &self.in_array, SortOptions::default())?;
            Ok((0..v.len())
                .map(|i| {
                    // SQL three-valued logic: null IN (...) is always null
                    if needle_nulls.is_some_and(|nulls| nulls.is_null(i)) {
                        return None;
                    }

                    let hash = hashes[i];
                    let contains = self
                        .map
                        .raw_entry()
                        .from_hash(hash, |idx| cmp(i, *idx).is_eq())
                        .is_some();

                    match contains {
                        true => Some(!negated),
                        false if haystack_has_nulls => None,
                        false => Some(negated),
                    }
                })
                .collect())
        })
    }
}

fn instantiate_static_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    match in_array.data_type() {
        // Integer primitive types
        DataType::Int8 => Ok(Arc::new(Int8StaticFilter::try_new(&in_array)?)),
        DataType::Int16 => Ok(Arc::new(Int16StaticFilter::try_new(&in_array)?)),
        DataType::Int32 => Ok(Arc::new(Int32StaticFilter::try_new(&in_array)?)),
        DataType::Int64 => Ok(Arc::new(Int64StaticFilter::try_new(&in_array)?)),
        DataType::UInt8 => Ok(Arc::new(UInt8StaticFilter::try_new(&in_array)?)),
        DataType::UInt16 => Ok(Arc::new(UInt16StaticFilter::try_new(&in_array)?)),
        DataType::UInt32 => Ok(Arc::new(UInt32StaticFilter::try_new(&in_array)?)),
        DataType::UInt64 => Ok(Arc::new(UInt64StaticFilter::try_new(&in_array)?)),
        // Float primitive types (use ordered wrappers for Hash/Eq)
        DataType::Float32 => Ok(Arc::new(Float32StaticFilter::try_new(&in_array)?)),
        DataType::Float64 => Ok(Arc::new(Float64StaticFilter::try_new(&in_array)?)),
        _ => {
            /* fall through to generic implementation for unsupported types (Struct, etc.) */
            Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
        }
    }
}

impl ArrayStaticFilter {
    /// Computes a [`StaticFilter`] for the provided [`Array`] if there
    /// are nulls present or there are more than the configured number of
    /// elements.
    ///
    /// Note: This is split into a separate function as higher-rank trait bounds currently
    /// cause type inference to misbehave
    fn try_new(in_array: ArrayRef) -> Result<ArrayStaticFilter> {
        // Null type has no natural order - return empty hash set
        if in_array.data_type() == &DataType::Null {
            return Ok(ArrayStaticFilter {
                in_array,
                state: RandomState::new(),
                map: HashMap::with_hasher(()),
            });
        }

        let state = RandomState::new();
        let mut map: HashMap<usize, (), ()> = HashMap::with_hasher(());

        with_hashes([&in_array], &state, |hashes| -> Result<()> {
            let cmp = make_comparator(&in_array, &in_array, SortOptions::default())?;

            let insert_value = |idx| {
                let hash = hashes[idx];
                if let RawEntryMut::Vacant(v) = map
                    .raw_entry_mut()
                    .from_hash(hash, |x| cmp(*x, idx).is_eq())
                {
                    v.insert_with_hasher(hash, idx, (), |x| hashes[*x]);
                }
            };

            match in_array.nulls() {
                Some(nulls) => {
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                        .for_each(insert_value)
                }
                None => (0..in_array.len()).for_each(insert_value),
            }

            Ok(())
        })?;

        Ok(Self {
            in_array,
            state,
            map,
        })
    }
}

/// Wrapper for f32 that implements Hash and Eq using bit comparison.
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat32(f32);

impl Hash for OrderedFloat32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat32 {}

impl From<f32> for OrderedFloat32 {
    fn from(v: f32) -> Self {
        Self(v)
    }
}

/// Wrapper for f64 that implements Hash and Eq using bit comparison.
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat64(f64);

impl Hash for OrderedFloat64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat64 {}

impl From<f64> for OrderedFloat64 {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

// Macro to generate specialized StaticFilter implementations for primitive types
macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        struct $Name {
            null_count: usize,
            values: HashSet<<$ArrowType as ArrowPrimitiveType>::Native>,
        }

        impl $Name {
            fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array
                    .as_primitive_opt::<$ArrowType>()
                    .ok_or_else(|| exec_datafusion_err!("Failed to downcast an array to a '{}' array", stringify!($ArrowType)))?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(v);
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                // Handle dictionary arrays by recursing on the values
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v
                    .as_primitive_opt::<$ArrowType>()
                    .ok_or_else(|| exec_datafusion_err!("Failed to downcast an array to a '{}' array", stringify!($ArrowType)))?;

                let haystack_has_nulls = self.null_count > 0;

                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                // Truth table for `value [NOT] IN (set)` with SQL three-valued logic:
                // ("-" means the value doesn't affect the result)
                //
                // | needle_null | haystack_null | negated | in set? | result |
                // |-------------|---------------|---------|---------|--------|
                // | true        | -             | false   | -       | null   |
                // | true        | -             | true    | -       | null   |
                // | false       | true          | false   | yes     | true   |
                // | false       | true          | false   | no      | null   |
                // | false       | true          | true    | yes     | false  |
                // | false       | true          | true    | no      | null   |
                // | false       | false         | false   | yes     | true   |
                // | false       | false         | false   | no      | false  |
                // | false       | false         | true    | yes     | false  |
                // | false       | false         | true    | no      | true   |

                // Compute the "contains" result using collect_bool (fast batched approach)
                // This ignores nulls - we handle them separately
                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&needle_values[i])
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&needle_values[i])
                    })
                };

                // Compute the null mask
                // Output is null when:
                // 1. needle value is null, OR
                // 2. needle value is not in set AND haystack has nulls
                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => {
                        // No nulls anywhere
                        None
                    }
                    (true, false) => {
                        // Only needle has nulls - just use needle's null mask
                        needle_nulls.cloned()
                    }
                    (false, true) => {
                        // Only haystack has nulls - result is null when value not in set
                        // Valid (not null) when original "in set" is true
                        // For NOT IN: contains_buffer = !original, so validity = !contains_buffer
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        // Both have nulls - combine needle nulls with haystack-induced nulls
                        let needle_validity = needle_nulls.map(|n| n.inner().clone())
                            .unwrap_or_else(|| BooleanBuffer::new_set(needle_values.len()));

                        // Valid when original "in set" is true (see above)
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };

                        // Combined validity: valid only where both are valid
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

// Generate specialized filters for all integer primitive types
primitive_static_filter!(Int8StaticFilter, Int8Type);
primitive_static_filter!(Int16StaticFilter, Int16Type);
primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt8StaticFilter, UInt8Type);
primitive_static_filter!(UInt16StaticFilter, UInt16Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

// Macro to generate specialized StaticFilter implementations for float types
// Floats require a wrapper type (OrderedFloat*) to implement Hash/Eq due to NaN semantics
macro_rules! float_static_filter {
    ($Name:ident, $ArrowType:ty, $OrderedType:ty) => {
        struct $Name {
            null_count: usize,
            values: HashSet<$OrderedType>,
        }

        impl $Name {
            fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array
                    .as_primitive_opt::<$ArrowType>()
                    .ok_or_else(|| exec_datafusion_err!("Failed to downcast an array to a '{}' array", stringify!($ArrowType)))?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(<$OrderedType>::from(v));
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                // Handle dictionary arrays by recursing on the values
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v
                    .as_primitive_opt::<$ArrowType>()
                    .ok_or_else(|| exec_datafusion_err!("Failed to downcast an array to a '{}' array", stringify!($ArrowType)))?;

                let haystack_has_nulls = self.null_count > 0;

                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                // Truth table for `value [NOT] IN (set)` with SQL three-valued logic:
                // ("-" means the value doesn't affect the result)
                //
                // | needle_null | haystack_null | negated | in set? | result |
                // |-------------|---------------|---------|---------|--------|
                // | true        | -             | false   | -       | null   |
                // | true        | -             | true    | -       | null   |
                // | false       | true          | false   | yes     | true   |
                // | false       | true          | false   | no      | null   |
                // | false       | true          | true    | yes     | false  |
                // | false       | true          | true    | no      | null   |
                // | false       | false         | false   | yes     | true   |
                // | false       | false         | false   | no      | false  |
                // | false       | false         | true    | yes     | false  |
                // | false       | false         | true    | no      | true   |

                // Compute the "contains" result using collect_bool (fast batched approach)
                // This ignores nulls - we handle them separately
                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                };

                // Compute the null mask
                // Output is null when:
                // 1. needle value is null, OR
                // 2. needle value is not in set AND haystack has nulls
                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => {
                        // No nulls anywhere
                        None
                    }
                    (true, false) => {
                        // Only needle has nulls - just use needle's null mask
                        needle_nulls.cloned()
                    }
                    (false, true) => {
                        // Only haystack has nulls - result is null when value not in set
                        // Valid (not null) when original "in set" is true
                        // For NOT IN: contains_buffer = !original, so validity = !contains_buffer
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        // Both have nulls - combine needle nulls with haystack-induced nulls
                        let needle_validity = needle_nulls.map(|n| n.inner().clone())
                            .unwrap_or_else(|| BooleanBuffer::new_set(needle_values.len()));

                        // Valid when original "in set" is true (see above)
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };

                        // Combined validity: valid only where both are valid
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

// Generate specialized filters for float types using ordered wrappers
float_static_filter!(Float32StaticFilter, Float32Type, OrderedFloat32);
float_static_filter!(Float64StaticFilter, Float64Type, OrderedFloat64);

/// Evaluates the list of expressions into an array, flattening any dictionaries
fn evaluate_list(
    list: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let scalars = list
        .iter()
        .map(|expr| {
            expr.evaluate(batch).and_then(|r| match r {
                ColumnarValue::Array(_) => {
                    exec_err!("InList expression must evaluate to a scalar")
                }
                // Flatten dictionary values
                ColumnarValue::Scalar(ScalarValue::Dictionary(_, v)) => Ok(*v),
                ColumnarValue::Scalar(s) => Ok(s),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    ScalarValue::iter_to_array(scalars)
}

/// Try to evaluate a list of expressions as constants.
///
/// Returns:
/// - `Ok(Some(ArrayRef))` if all expressions are constants (can be evaluated on an empty RecordBatch)
/// - `Ok(None)` if the list contains non-constant expressions
/// - `Err(...)` only for actual errors (not for non-constant expressions)
///
/// This is used to detect when a list contains only literals, casts of literals,
/// or other constant expressions.
fn try_evaluate_constant_list(
    list: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<Option<ArrayRef>> {
    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
    match evaluate_list(list, &batch) {
        Ok(array) => Ok(Some(array)),
        Err(_) => {
            // Non-constant expressions can't be evaluated on an empty batch
            // This is not an error, just means we can't use a static filter
            Ok(None)
        }
    }
}

impl InListExpr {
    /// Create a new InList expression
    fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        static_filter: Option<Arc<dyn StaticFilter + Send + Sync>>,
    ) -> Self {
        Self {
            expr,
            list,
            negated,
            static_filter,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.list
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Create a new InList expression directly from an array, bypassing expression evaluation.
    ///
    /// This is more efficient than `in_list()` when you already have the list as an array,
    /// as it avoids the conversion: `ArrayRef -> Vec<PhysicalExpr> -> ArrayRef -> StaticFilter`.
    /// Instead it goes directly: `ArrayRef -> StaticFilter`.
    ///
    /// The `list` field will be empty when using this constructor, as the array is stored
    /// directly in the static filter.
    ///
    /// This does not make the expression any more performant at runtime, but it does make it slightly
    /// cheaper to build.
    pub fn try_new_from_array(
        expr: Arc<dyn PhysicalExpr>,
        array: ArrayRef,
        negated: bool,
    ) -> Result<Self> {
        let list = (0..array.len())
            .map(|i| {
                let scalar = ScalarValue::try_from_array(array.as_ref(), i)?;
                Ok(crate::expressions::lit(scalar) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(
            expr,
            list,
            negated,
            Some(instantiate_static_filter(array)?),
        ))
    }

    /// Create a new InList expression, using a static filter when possible.
    ///
    /// This validates data types and attempts to create a static filter for constant
    /// list expressions. Uses specialized StaticFilter implementations for better
    /// performance (e.g., Int32StaticFilter for Int32).
    ///
    /// Returns an error if data types don't match. If the list contains non-constant
    /// expressions, falls back to dynamic evaluation at runtime.
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        schema: &Schema,
    ) -> Result<Self> {
        // Check the data types match
        let expr_data_type = expr.data_type(schema)?;
        for list_expr in list.iter() {
            let list_expr_data_type = list_expr.data_type(schema)?;
            assert_or_internal_err!(
                DFSchema::datatype_is_logically_equal(
                    &expr_data_type,
                    &list_expr_data_type
                ),
                "The data type inlist should be same, the value type is {expr_data_type}, one of list expr type is {list_expr_data_type}"
            );
        }

        // Try to create a static filter if all list expressions are constants
        let static_filter = match try_evaluate_constant_list(&list, schema)? {
            Some(in_array) => Some(instantiate_static_filter(in_array)?),
            None => None, // Non-constant expressions, fall back to dynamic evaluation
        };

        Ok(Self::new(expr, list, negated, static_filter))
    }
}
impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let list = expr_vec_fmt!(self.list);

        if self.negated {
            if self.static_filter.is_some() {
                write!(f, "{} NOT IN (SET) ([{list}])", self.expr)
            } else {
                write!(f, "{} NOT IN ([{list}])", self.expr)
            }
        } else if self.static_filter.is_some() {
            write!(f, "{} IN (SET) ([{list}])", self.expr)
        } else {
            write!(f, "{} IN ([{list}])", self.expr)
        }
    }
}

impl PhysicalExpr for InListExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        if self.expr.nullable(input_schema)? {
            return Ok(true);
        }

        if let Some(static_filter) = &self.static_filter {
            Ok(static_filter.null_count() > 0)
        } else {
            for expr in &self.list {
                if expr.nullable(input_schema)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let value = self.expr.evaluate(batch)?;
        let r = match &self.static_filter {
            Some(filter) => {
                match value {
                    ColumnarValue::Array(array) => {
                        filter.contains(&array, self.negated)?
                    }
                    ColumnarValue::Scalar(scalar) => {
                        if scalar.is_null() {
                            // SQL three-valued logic: null IN (...) is always null
                            // The code below would handle this correctly but this is a faster path
                            let nulls = NullBuffer::new_null(num_rows);
                            return Ok(ColumnarValue::Array(Arc::new(
                                BooleanArray::new(
                                    BooleanBuffer::new_unset(num_rows),
                                    Some(nulls),
                                ),
                            )));
                        }
                        // Use a 1 row array to avoid code duplication/branching
                        // Since all we do is compute hash and lookup this should be efficient enough
                        let array = scalar.to_array()?;
                        let result_array =
                            filter.contains(array.as_ref(), self.negated)?;
                        // Broadcast the single result to all rows
                        // Must check is_null() to preserve NULL values (SQL three-valued logic)
                        if result_array.is_null(0) {
                            let nulls = NullBuffer::new_null(num_rows);
                            BooleanArray::new(
                                BooleanBuffer::new_unset(num_rows),
                                Some(nulls),
                            )
                        } else if result_array.value(0) {
                            BooleanArray::new(BooleanBuffer::new_set(num_rows), None)
                        } else {
                            BooleanArray::new(BooleanBuffer::new_unset(num_rows), None)
                        }
                    }
                }
            }
            None => {
                // No static filter: iterate through each expression, compare, and OR results
                let value = value.into_array(num_rows)?;
                let found = self.list.iter().map(|expr| expr.evaluate(batch)).try_fold(
                    BooleanArray::new(BooleanBuffer::new_unset(num_rows), None),
                    |result, expr| -> Result<BooleanArray> {
                        let rhs = match expr? {
                            ColumnarValue::Array(array) => {
                                let cmp = make_comparator(
                                    value.as_ref(),
                                    array.as_ref(),
                                    SortOptions::default(),
                                )?;
                                (0..num_rows)
                                    .map(|i| {
                                        if value.is_null(i) || array.is_null(i) {
                                            return None;
                                        }
                                        Some(cmp(i, i).is_eq())
                                    })
                                    .collect::<BooleanArray>()
                            }
                            ColumnarValue::Scalar(scalar) => {
                                // Check if scalar is null once, before the loop
                                if scalar.is_null() {
                                    // If scalar is null, all comparisons return null
                                    BooleanArray::from(vec![None; num_rows])
                                } else {
                                    // Convert scalar to 1-element array
                                    let array = scalar.to_array()?;
                                    let cmp = make_comparator(
                                        value.as_ref(),
                                        array.as_ref(),
                                        SortOptions::default(),
                                    )?;
                                    // Compare each row of value with the single scalar element
                                    (0..num_rows)
                                        .map(|i| {
                                            if value.is_null(i) {
                                                None
                                            } else {
                                                Some(cmp(i, 0).is_eq())
                                            }
                                        })
                                        .collect::<BooleanArray>()
                                }
                            }
                        };
                        Ok(or_kleene(&result, &rhs)?)
                    },
                )?;

                if self.negated { not(&found)? } else { found }
            }
        };
        Ok(ColumnarValue::Array(Arc::new(r)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![&self.expr];
        children.extend(&self.list);
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // assume the static_filter will not change during the rewrite process
        Ok(Arc::new(InListExpr::new(
            Arc::clone(&children[0]),
            children[1..].to_vec(),
            self.negated,
            self.static_filter.as_ref().map(Arc::clone),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt_sql(f)?;
        if self.negated {
            write!(f, " NOT")?;
        }

        write!(f, " IN (")?;
        for (i, expr) in self.list.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, ")")
    }
}

impl PartialEq for InListExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && physical_exprs_bag_equal(&self.list, &other.list)
            && self.negated == other.negated
    }
}

impl Eq for InListExpr {}

impl Hash for InListExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.negated.hash(state);
        // Add `self.static_filter` when hash is available
        self.list.hash(state);
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(InListExpr::try_new(expr, list, *negated, schema)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit, try_cast};
    use arrow::buffer::NullBuffer;
    use datafusion_common::plan_err;
    use datafusion_expr::type_coercion::binary::comparison_coercion;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;
    use insta::assert_snapshot;
    use itertools::Itertools;

    type InListCastResult = (Arc<dyn PhysicalExpr>, Vec<Arc<dyn PhysicalExpr>>);

    // Try to do the type coercion for list physical expr.
    // It's just used in the test
    fn in_list_cast(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Result<InListCastResult> {
        let expr_type = &expr.data_type(input_schema)?;
        let list_types: Vec<DataType> = list
            .iter()
            .map(|list_expr| list_expr.data_type(input_schema).unwrap())
            .collect();
        let result_type = get_coerce_type(expr_type, &list_types);
        match result_type {
            None => plan_err!(
                "Can not find compatible types to compare {expr_type} with [{}]",
                list_types.iter().join(", ")
            ),
            Some(data_type) => {
                // find the coerced type
                let cast_expr = try_cast(expr, input_schema, data_type.clone())?;
                let cast_list_expr = list
                    .into_iter()
                    .map(|list_expr| {
                        try_cast(list_expr, input_schema, data_type.clone()).unwrap()
                    })
                    .collect();
                Ok((cast_expr, cast_list_expr))
            }
        }
    }

    // Attempts to coerce the types of `list_type` to be comparable with the
    // `expr_type`
    fn get_coerce_type(expr_type: &DataType, list_type: &[DataType]) -> Option<DataType> {
        list_type
            .iter()
            .try_fold(expr_type.clone(), |left_type, right_type| {
                comparison_coercion(&left_type, right_type)
            })
    }

    /// Test helper macro that evaluates an IN LIST expression with automatic type casting.
    ///
    /// # Parameters
    /// - `$BATCH`: The `RecordBatch` containing the input data to evaluate against
    /// - `$LIST`: A `Vec<Arc<dyn PhysicalExpr>>` of literal expressions representing the IN list values
    /// - `$NEGATED`: A `&bool` indicating whether this is a NOT IN operation (true) or IN operation (false)
    /// - `$EXPECTED`: A `Vec<Option<bool>>` representing the expected boolean results for each row
    /// - `$COL`: An `Arc<dyn PhysicalExpr>` representing the column expression to evaluate
    /// - `$SCHEMA`: A `&Schema` reference for the input batch
    ///
    /// This macro first applies type casting to the column and list expressions to ensure
    /// type compatibility, then delegates to `in_list_raw!` to perform the evaluation and assertion.
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr, $COL:expr, $SCHEMA:expr) => {{
            let (cast_expr, cast_list_exprs) = in_list_cast($COL, $LIST, $SCHEMA)?;
            in_list_raw!(
                $BATCH,
                cast_list_exprs,
                $NEGATED,
                $EXPECTED,
                cast_expr,
                $SCHEMA
            );
        }};
    }

    /// Test helper macro that evaluates an IN LIST expression without automatic type casting.
    ///
    /// # Parameters
    /// - `$BATCH`: The `RecordBatch` containing the input data to evaluate against
    /// - `$LIST`: A `Vec<Arc<dyn PhysicalExpr>>` of literal expressions representing the IN list values
    /// - `$NEGATED`: A `&bool` indicating whether this is a NOT IN operation (true) or IN operation (false)
    /// - `$EXPECTED`: A `Vec<Option<bool>>` representing the expected boolean results for each row
    /// - `$COL`: An `Arc<dyn PhysicalExpr>` representing the column expression to evaluate
    /// - `$SCHEMA`: A `&Schema` reference for the input batch
    ///
    /// This macro creates an IN LIST expression, evaluates it against the batch, converts the result
    /// to a `BooleanArray`, and asserts that it matches the expected output. Use this when the column
    /// and list expressions are already the correct types and don't require casting.
    macro_rules! in_list_raw {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr, $COL:expr, $SCHEMA:expr) => {{
            let col_expr = $COL;
            let expr = in_list(Arc::clone(&col_expr), $LIST, $NEGATED, $SCHEMA).unwrap();
            let result = expr
                .evaluate(&$BATCH)?
                .into_array($BATCH.num_rows())
                .expect("Failed to convert to array");
            let result = as_boolean_array(&result);
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(
                expected,
                result,
                "Failed for: {}\n{}: {:?}",
                fmt_sql(expr.as_ref()),
                fmt_sql(col_expr.as_ref()),
                col_expr
                    .evaluate(&$BATCH)?
                    .into_array($BATCH.num_rows())
                    .unwrap()
            );
        }};
    }

    /// Test case for primitive types following the standard IN LIST pattern.
    ///
    /// Each test case represents a data type with:
    /// - `value_in`: A value that appears in both the test array and the IN list (matches → true)
    /// - `value_not_in`: A value that appears in the test array but NOT in the IN list (doesn't match → false)
    /// - `other_list_values`: Additional values in the IN list besides `value_in`
    /// - `null_value`: Optional null scalar value for NULL handling tests. When None, tests
    ///   without nulls are run, exercising the `(false, false)` and `(false, true)` branches.
    struct InListPrimitiveTestCase {
        name: &'static str,
        value_in: ScalarValue,
        value_not_in: ScalarValue,
        other_list_values: Vec<ScalarValue>,
        null_value: Option<ScalarValue>,
    }

    /// Generic test data struct for primitive types.
    ///
    /// Holds test values needed for IN LIST tests, allowing the data
    /// to be declared explicitly and reused across multiple types.
    #[derive(Clone)]
    struct PrimitiveTestCaseData<T> {
        value_in: T,
        value_not_in: T,
        other_list_values: Vec<T>,
    }

    /// Helper to create test cases for any primitive type using generic data.
    ///
    /// Uses TryInto for flexible type conversion, allowing test data to be
    /// declared in any convertible type (e.g., i32 for all integer types).
    /// Creates a test case WITH null support (for null handling tests).
    fn primitive_test_case<T, D, F>(
        name: &'static str,
        constructor: F,
        data: PrimitiveTestCaseData<D>,
    ) -> InListPrimitiveTestCase
    where
        D: TryInto<T> + Clone,
        <D as TryInto<T>>::Error: Debug,
        F: Fn(Option<T>) -> ScalarValue,
        T: Clone,
    {
        InListPrimitiveTestCase {
            name,
            value_in: constructor(Some(data.value_in.try_into().unwrap())),
            value_not_in: constructor(Some(data.value_not_in.try_into().unwrap())),
            other_list_values: data
                .other_list_values
                .into_iter()
                .map(|v| constructor(Some(v.try_into().unwrap())))
                .collect(),
            null_value: Some(constructor(None)),
        }
    }

    /// Helper to create test cases WITHOUT null support.
    /// These test cases exercise the `(false, true)` branch (no nulls, negated).
    fn primitive_test_case_no_nulls<T, D, F>(
        name: &'static str,
        constructor: F,
        data: PrimitiveTestCaseData<D>,
    ) -> InListPrimitiveTestCase
    where
        D: TryInto<T> + Clone,
        <D as TryInto<T>>::Error: Debug,
        F: Fn(Option<T>) -> ScalarValue,
        T: Clone,
    {
        InListPrimitiveTestCase {
            name,
            value_in: constructor(Some(data.value_in.try_into().unwrap())),
            value_not_in: constructor(Some(data.value_not_in.try_into().unwrap())),
            other_list_values: data
                .other_list_values
                .into_iter()
                .map(|v| constructor(Some(v.try_into().unwrap())))
                .collect(),
            null_value: None,
        }
    }

    /// Runs test cases for multiple types, providing detailed SQL error messages on failure.
    ///
    /// For each test case, runs IN LIST scenarios based on whether null_value is Some or None:
    /// - With null_value (Some): 4 tests including null handling
    /// - Without null_value (None): 2 tests exercising the no-nulls paths
    fn run_test_cases(test_cases: Vec<InListPrimitiveTestCase>) -> Result<()> {
        for test_case in test_cases {
            let test_name = test_case.name;

            // Get the data type from the scalar value
            let data_type = test_case.value_in.data_type();

            // Build the base list: [value_in, ...other_list_values]
            let build_base_list = || -> Vec<Arc<dyn PhysicalExpr>> {
                let mut list = vec![lit(test_case.value_in.clone())];
                list.extend(test_case.other_list_values.iter().map(|v| lit(v.clone())));
                list
            };

            match &test_case.null_value {
                Some(null_val) => {
                    // Tests WITH nulls in the needle array
                    let schema =
                        Schema::new(vec![Field::new("a", data_type.clone(), true)]);

                    // Create array from scalar values: [value_in, value_not_in, None]
                    let array = ScalarValue::iter_to_array(vec![
                        test_case.value_in.clone(),
                        test_case.value_not_in.clone(),
                        null_val.clone(),
                    ])?;

                    let col_a = col("a", &schema)?;
                    let batch = RecordBatch::try_new(
                        Arc::new(schema.clone()),
                        vec![Arc::clone(&array)],
                    )?;

                    // Test 1: a IN (list) → [true, false, null]
                    let list = build_base_list();
                    in_list!(
                        batch,
                        list,
                        &false,
                        vec![Some(true), Some(false), None],
                        Arc::clone(&col_a),
                        &schema
                    );

                    // Test 2: a NOT IN (list) → [false, true, null]
                    let list = build_base_list();
                    in_list!(
                        batch,
                        list,
                        &true,
                        vec![Some(false), Some(true), None],
                        Arc::clone(&col_a),
                        &schema
                    );

                    // Test 3: a IN (list, NULL) → [true, null, null]
                    let mut list = build_base_list();
                    list.push(lit(null_val.clone()));
                    in_list!(
                        batch,
                        list,
                        &false,
                        vec![Some(true), None, None],
                        Arc::clone(&col_a),
                        &schema
                    );

                    // Test 4: a NOT IN (list, NULL) → [false, null, null]
                    let mut list = build_base_list();
                    list.push(lit(null_val.clone()));
                    in_list!(
                        batch,
                        list,
                        &true,
                        vec![Some(false), None, None],
                        Arc::clone(&col_a),
                        &schema
                    );
                }
                None => {
                    // Tests WITHOUT nulls - exercises the (false, false) and (false, true) branches
                    let schema =
                        Schema::new(vec![Field::new("a", data_type.clone(), false)]);

                    // Create array from scalar values: [value_in, value_not_in] (no NULL)
                    let array = ScalarValue::iter_to_array(vec![
                        test_case.value_in.clone(),
                        test_case.value_not_in.clone(),
                    ])?;

                    let col_a = col("a", &schema)?;
                    let batch = RecordBatch::try_new(
                        Arc::new(schema.clone()),
                        vec![Arc::clone(&array)],
                    )?;

                    // Test 1: a IN (list) → [true, false] - exercises (false, false) branch
                    let list = build_base_list();
                    in_list!(
                        batch,
                        list,
                        &false,
                        vec![Some(true), Some(false)],
                        Arc::clone(&col_a),
                        &schema
                    );

                    // Test 2: a NOT IN (list) → [false, true] - exercises (false, true) branch
                    let list = build_base_list();
                    in_list!(
                        batch,
                        list,
                        &true,
                        vec![Some(false), Some(true)],
                        Arc::clone(&col_a),
                        &schema
                    );

                    eprintln!(
                        "Test '{test_name}': exercised (false, true) branch (no nulls, negated)",
                    );
                }
            }
        }

        Ok(())
    }

    /// Test IN LIST for all integer types (Int8/16/32/64, UInt8/16/32/64).
    ///
    /// Test data: 0 (in list), 2 (not in list), [1, 3, 5] (other list values)
    #[test]
    fn in_list_int_types() -> Result<()> {
        let int_data = PrimitiveTestCaseData {
            value_in: 0,
            value_not_in: 2,
            other_list_values: vec![1, 3, 5],
        };

        run_test_cases(vec![
            // Tests WITH nulls
            primitive_test_case("int8", ScalarValue::Int8, int_data.clone()),
            primitive_test_case("int16", ScalarValue::Int16, int_data.clone()),
            primitive_test_case("int32", ScalarValue::Int32, int_data.clone()),
            primitive_test_case("int64", ScalarValue::Int64, int_data.clone()),
            primitive_test_case("uint8", ScalarValue::UInt8, int_data.clone()),
            primitive_test_case("uint16", ScalarValue::UInt16, int_data.clone()),
            primitive_test_case("uint32", ScalarValue::UInt32, int_data.clone()),
            primitive_test_case("uint64", ScalarValue::UInt64, int_data.clone()),
            // Tests WITHOUT nulls - exercises (false, true) branch
            primitive_test_case_no_nulls("int32_no_nulls", ScalarValue::Int32, int_data),
        ])
    }

    /// Test IN LIST for all string types (Utf8, LargeUtf8, Utf8View).
    ///
    /// Test data: "a" (in list), "d" (not in list), ["b", "c"] (other list values)
    #[test]
    fn in_list_string_types() -> Result<()> {
        let string_data = PrimitiveTestCaseData {
            value_in: "a",
            value_not_in: "d",
            other_list_values: vec!["b", "c"],
        };

        run_test_cases(vec![
            primitive_test_case("utf8", ScalarValue::Utf8, string_data.clone()),
            primitive_test_case(
                "large_utf8",
                ScalarValue::LargeUtf8,
                string_data.clone(),
            ),
            primitive_test_case("utf8_view", ScalarValue::Utf8View, string_data),
        ])
    }

    /// Test IN LIST for all binary types (Binary, LargeBinary, BinaryView).
    ///
    /// Test data: [1,2,3] (in list), [1,2,2] (not in list), [[4,5,6], [7,8,9]] (other list values)
    #[test]
    fn in_list_binary_types() -> Result<()> {
        let binary_data = PrimitiveTestCaseData {
            value_in: vec![1_u8, 2, 3],
            value_not_in: vec![1_u8, 2, 2],
            other_list_values: vec![vec![4_u8, 5, 6], vec![7_u8, 8, 9]],
        };

        run_test_cases(vec![
            primitive_test_case("binary", ScalarValue::Binary, binary_data.clone()),
            primitive_test_case(
                "large_binary",
                ScalarValue::LargeBinary,
                binary_data.clone(),
            ),
            primitive_test_case("binary_view", ScalarValue::BinaryView, binary_data),
        ])
    }

    /// Test IN LIST for date types (Date32, Date64).
    ///
    /// Test data: 0 (in list), 2 (not in list), [1, 3] (other list values)
    #[test]
    fn in_list_date_types() -> Result<()> {
        let date_data = PrimitiveTestCaseData {
            value_in: 0,
            value_not_in: 2,
            other_list_values: vec![1, 3],
        };

        run_test_cases(vec![
            primitive_test_case("date32", ScalarValue::Date32, date_data.clone()),
            primitive_test_case("date64", ScalarValue::Date64, date_data),
        ])
    }

    /// Test IN LIST for Decimal128 type.
    ///
    /// Test data: 0 (in list), 200 (not in list), [100, 300] (other list values) with precision=10, scale=2
    #[test]
    fn in_list_decimal() -> Result<()> {
        run_test_cases(vec![InListPrimitiveTestCase {
            name: "decimal128",
            value_in: ScalarValue::Decimal128(Some(0), 10, 2),
            value_not_in: ScalarValue::Decimal128(Some(200), 10, 2),
            other_list_values: vec![
                ScalarValue::Decimal128(Some(100), 10, 2),
                ScalarValue::Decimal128(Some(300), 10, 2),
            ],
            null_value: Some(ScalarValue::Decimal128(None, 10, 2)),
        }])
    }

    /// Test IN LIST for timestamp types.
    ///
    /// Test data: 0 (in list), 2000 (not in list), [1000, 3000] (other list values)
    #[test]
    fn in_list_timestamp_types() -> Result<()> {
        run_test_cases(vec![InListPrimitiveTestCase {
            name: "timestamp_nanosecond",
            value_in: ScalarValue::TimestampNanosecond(Some(0), None),
            value_not_in: ScalarValue::TimestampNanosecond(Some(2000), None),
            other_list_values: vec![
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(3000), None),
            ],
            null_value: Some(ScalarValue::TimestampNanosecond(None, None)),
        }])
    }

    #[test]
    fn in_list_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![
            Some(0.0),
            Some(0.2),
            None,
            Some(f64::NAN),
            Some(-f64::NAN),
        ]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0.0, 0.1)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None, Some(false), Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0.0, 0.1)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None, Some(true), Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0.0, 0.1, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0.0, 0.1, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0.0, 0.1, NaN)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(f64::NAN)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None, Some(true), Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0.0, 0.1, NaN)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(f64::NAN)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None, Some(false), Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0.0, 0.1, -NaN)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(-f64::NAN)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None, Some(false), Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0.0, 0.1, -NaN)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(-f64::NAN)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None, Some(true), Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    macro_rules! test_nullable {
        ($COL:expr, $LIST:expr, $SCHEMA:expr, $EXPECTED:expr) => {{
            let (cast_expr, cast_list_exprs) = in_list_cast($COL, $LIST, $SCHEMA)?;
            let expr = in_list(cast_expr, cast_list_exprs, &false, $SCHEMA).unwrap();
            let result = expr.nullable($SCHEMA)?;
            assert_eq!($EXPECTED, result);
        }};
    }

    #[test]
    fn in_list_nullable() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1_nullable", DataType::Int64, true),
            Field::new("c2_non_nullable", DataType::Int64, false),
        ]);

        let c1_nullable = col("c1_nullable", &schema)?;
        let c2_non_nullable = col("c2_non_nullable", &schema)?;

        // static_filter has no nulls
        let list = vec![lit(1_i64), lit(2_i64)];
        test_nullable!(Arc::clone(&c1_nullable), list.clone(), &schema, true);
        test_nullable!(Arc::clone(&c2_non_nullable), list.clone(), &schema, false);

        // static_filter has nulls
        let list = vec![lit(1_i64), lit(2_i64), lit(ScalarValue::Null)];
        test_nullable!(Arc::clone(&c1_nullable), list.clone(), &schema, true);
        test_nullable!(Arc::clone(&c2_non_nullable), list.clone(), &schema, true);

        let list = vec![Arc::clone(&c1_nullable)];
        test_nullable!(Arc::clone(&c2_non_nullable), list.clone(), &schema, true);

        let list = vec![Arc::clone(&c2_non_nullable)];
        test_nullable!(Arc::clone(&c1_nullable), list.clone(), &schema, true);

        let list = vec![Arc::clone(&c2_non_nullable), Arc::clone(&c2_non_nullable)];
        test_nullable!(Arc::clone(&c2_non_nullable), list.clone(), &schema, false);

        Ok(())
    }

    #[test]
    fn in_list_no_cols() -> Result<()> {
        // test logic when the in_list expression doesn't have any columns
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(2), None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        let list = vec![lit(ScalarValue::from(1i32)), lit(ScalarValue::from(6i32))];

        // 1 IN (1, 6)
        let expr = lit(ScalarValue::Int32(Some(1)));
        in_list!(
            batch,
            list.clone(),
            &false,
            // should have three outputs, as the input batch has three rows
            vec![Some(true), Some(true), Some(true)],
            expr,
            &schema
        );

        // 2 IN (1, 6)
        let expr = lit(ScalarValue::Int32(Some(2)));
        in_list!(
            batch,
            list.clone(),
            &false,
            // should have three outputs, as the input batch has three rows
            vec![Some(false), Some(false), Some(false)],
            expr,
            &schema
        );

        // NULL IN (1, 6)
        let expr = lit(ScalarValue::Int32(None));
        in_list!(
            batch,
            list.clone(),
            &false,
            // should have three outputs, as the input batch has three rows
            vec![None, None, None],
            expr,
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_utf8_with_dict_types() -> Result<()> {
        fn dict_lit(key_type: DataType, value: &str) -> Arc<dyn PhysicalExpr> {
            lit(ScalarValue::Dictionary(
                Box::new(key_type),
                Box::new(ScalarValue::new_utf8(value.to_string())),
            ))
        }

        fn null_dict_lit(key_type: DataType) -> Arc<dyn PhysicalExpr> {
            lit(ScalarValue::Dictionary(
                Box::new(key_type),
                Box::new(ScalarValue::Utf8(None)),
            ))
        }

        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        )]);
        let a: UInt16DictionaryArray =
            vec![Some("a"), Some("d"), None].into_iter().collect();
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let lists = [
            vec![lit("a"), lit("b")],
            vec![
                dict_lit(DataType::Int8, "a"),
                dict_lit(DataType::UInt16, "b"),
            ],
        ];
        for list in lists.iter() {
            in_list_raw!(
                batch,
                list.clone(),
                &false,
                vec![Some(true), Some(false), None],
                Arc::clone(&col_a),
                &schema
            );
        }

        // expression: "a not in ("a", "b")"
        for list in lists.iter() {
            in_list_raw!(
                batch,
                list.clone(),
                &true,
                vec![Some(false), Some(true), None],
                Arc::clone(&col_a),
                &schema
            );
        }

        // expression: "a in ("a", "b", null)"
        let lists = [
            vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))],
            vec![
                dict_lit(DataType::Int8, "a"),
                dict_lit(DataType::UInt16, "b"),
                null_dict_lit(DataType::UInt16),
            ],
        ];
        for list in lists.iter() {
            in_list_raw!(
                batch,
                list.clone(),
                &false,
                vec![Some(true), None, None],
                Arc::clone(&col_a),
                &schema
            );
        }

        // expression: "a not in ("a", "b", null)"
        for list in lists.iter() {
            in_list_raw!(
                batch,
                list.clone(),
                &true,
                vec![Some(false), None, None],
                Arc::clone(&col_a),
                &schema
            );
        }

        Ok(())
    }

    #[test]
    fn test_fmt_sql_1() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let col_a = col("a", &schema)?;

        // Test: a IN ('a', 'b')
        let list = vec![lit("a"), lit("b")];
        let expr = in_list(Arc::clone(&col_a), list, &false, &schema)?;
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        let display_string = expr.to_string();
        assert_snapshot!(sql_string, @"a IN (a, b)");
        assert_snapshot!(display_string, @"a@0 IN (SET) ([a, b])");
        Ok(())
    }

    #[test]
    fn test_fmt_sql_2() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let col_a = col("a", &schema)?;

        // Test: a NOT IN ('a', 'b')
        let list = vec![lit("a"), lit("b")];
        let expr = in_list(Arc::clone(&col_a), list, &true, &schema)?;
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        let display_string = expr.to_string();

        assert_snapshot!(sql_string, @"a NOT IN (a, b)");
        assert_snapshot!(display_string, @"a@0 NOT IN (SET) ([a, b])");
        Ok(())
    }

    #[test]
    fn test_fmt_sql_3() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let col_a = col("a", &schema)?;
        // Test: a IN ('a', 'b', NULL)
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        let expr = in_list(Arc::clone(&col_a), list, &false, &schema)?;
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        let display_string = expr.to_string();

        assert_snapshot!(sql_string, @"a IN (a, b, NULL)");
        assert_snapshot!(display_string, @"a@0 IN (SET) ([a, b, NULL])");
        Ok(())
    }

    #[test]
    fn test_fmt_sql_4() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let col_a = col("a", &schema)?;
        // Test: a NOT IN ('a', 'b', NULL)
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        let expr = in_list(Arc::clone(&col_a), list, &true, &schema)?;
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        let display_string = expr.to_string();
        assert_snapshot!(sql_string, @"a NOT IN (a, b, NULL)");
        assert_snapshot!(display_string, @"a@0 NOT IN (SET) ([a, b, NULL])");
        Ok(())
    }

    #[test]
    fn in_list_struct() -> Result<()> {
        // Create schema with a struct column
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Struct(struct_fields.clone()),
            true,
        )]);

        // Create test data: array of structs
        let x_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let y_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let struct_array =
            StructArray::new(struct_fields.clone(), vec![x_array, y_array], None);

        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(struct_array)])?;

        // Create literal structs for the IN list
        // Struct {x: 1, y: "a"}
        let struct1 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
            None,
        )));

        // Struct {x: 3, y: "c"}
        let struct3 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["c"])),
            ],
            None,
        )));

        // Test: a IN ({1, "a"}, {3, "c"})
        let list = vec![lit(struct1.clone()), lit(struct3.clone())];
        in_list_raw!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false), Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // Test: a NOT IN ({1, "a"}, {3, "c"})
        in_list_raw!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_struct_with_nulls() -> Result<()> {
        // Create schema with a struct column
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Struct(struct_fields.clone()),
            true,
        )]);

        // Create test data with a null struct
        let x_array = Arc::new(Int32Array::from(vec![1, 2]));
        let y_array = Arc::new(StringArray::from(vec!["a", "b"]));
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![x_array, y_array],
            Some(NullBuffer::from(vec![true, false])),
        );

        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(struct_array)])?;

        // Create literal struct for the IN list
        let struct1 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
            None,
        )));

        // Test: a IN ({1, "a"})
        let list = vec![lit(struct1.clone())];
        in_list_raw!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // Test: a NOT IN ({1, "a"})
        in_list_raw!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_struct_with_null_in_list() -> Result<()> {
        // Create schema with a struct column
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Struct(struct_fields.clone()),
            true,
        )]);

        // Create test data
        let x_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let y_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let struct_array =
            StructArray::new(struct_fields.clone(), vec![x_array, y_array], None);

        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(struct_array)])?;

        // Create literal structs including a NULL
        let struct1 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
            None,
        )));

        let null_struct = ScalarValue::Struct(Arc::new(StructArray::new_null(
            struct_fields.clone(),
            1,
        )));

        // Test: a IN ({1, "a"}, NULL)
        let list = vec![lit(struct1), lit(null_struct.clone())];
        in_list_raw!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // Test: a NOT IN ({1, "a"}, NULL)
        in_list_raw!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_nested_struct() -> Result<()> {
        // Create nested struct schema
        let inner_struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let outer_struct_fields = Fields::from(vec![
            Field::new(
                "inner",
                DataType::Struct(inner_struct_fields.clone()),
                false,
            ),
            Field::new("c", DataType::Int32, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "x",
            DataType::Struct(outer_struct_fields.clone()),
            true,
        )]);

        // Create test data with nested structs
        let inner1 = Arc::new(StructArray::new(
            inner_struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
            None,
        ));
        let c_array = Arc::new(Int32Array::from(vec![10, 20]));
        let outer_array =
            StructArray::new(outer_struct_fields.clone(), vec![inner1, c_array], None);

        let col_x = col("x", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(outer_array)])?;

        // Create a nested struct literal matching the first row
        let inner_match = Arc::new(StructArray::new(
            inner_struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
            None,
        ));
        let outer_match = ScalarValue::Struct(Arc::new(StructArray::new(
            outer_struct_fields.clone(),
            vec![inner_match, Arc::new(Int32Array::from(vec![10]))],
            None,
        )));

        // Test: x IN ({{1, "x"}, 10})
        let list = vec![lit(outer_match)];
        in_list_raw!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false)],
            Arc::clone(&col_x),
            &schema
        );

        // Test: x NOT IN ({{1, "x"}, 10})
        in_list_raw!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true)],
            Arc::clone(&col_x),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_struct_with_exprs_not_array() -> Result<()> {
        // Test InList using expressions (not the array constructor) with structs
        // By using InListExpr::new directly, we bypass the array optimization
        // and use the Exprs variant, testing the expression evaluation path

        // Create schema with a struct column {x: Int32, y: Utf8}
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Struct(struct_fields.clone()),
            true,
        )]);

        // Create test data: array of structs [{1, "a"}, {2, "b"}, {3, "c"}]
        let x_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let y_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let struct_array =
            StructArray::new(struct_fields.clone(), vec![x_array, y_array], None);

        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(struct_array)])?;

        // Create struct literals with the SAME shape (so types are compatible)
        // Struct {x: 1, y: "a"}
        let struct1 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
            None,
        )));

        // Struct {x: 3, y: "c"}
        let struct3 = ScalarValue::Struct(Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["c"])),
            ],
            None,
        )));

        // Create list of struct expressions
        let list = vec![lit(struct1), lit(struct3)];

        // Use InListExpr::new directly (not in_list()) to bypass array optimization
        // This creates an InList without a static filter
        let expr = Arc::new(InListExpr::new(Arc::clone(&col_a), list, false, None));

        // Verify that the expression doesn't have a static filter
        // by checking the display string does NOT contain "(SET)"
        let display_string = expr.to_string();
        assert!(
            !display_string.contains("(SET)"),
            "Expected display string to NOT contain '(SET)' (should use Exprs variant), but got: {display_string}",
        );

        // Evaluate the expression
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result);

        // Expected: first row {1, "a"} matches struct1,
        //           second row {2, "b"} doesn't match,
        //           third row {3, "c"} matches struct3
        let expected = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        assert_eq!(result, &expected);

        // Test NOT IN as well
        let expr_not = Arc::new(InListExpr::new(
            Arc::clone(&col_a),
            vec![
                lit(ScalarValue::Struct(Arc::new(StructArray::new(
                    struct_fields.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![1])),
                        Arc::new(StringArray::from(vec!["a"])),
                    ],
                    None,
                )))),
                lit(ScalarValue::Struct(Arc::new(StructArray::new(
                    struct_fields.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![3])),
                        Arc::new(StringArray::from(vec!["c"])),
                    ],
                    None,
                )))),
            ],
            true,
            None,
        ));

        let result_not = expr_not.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result_not = as_boolean_array(&result_not);

        let expected_not = BooleanArray::from(vec![Some(false), Some(true), Some(false)]);
        assert_eq!(result_not, &expected_not);

        Ok(())
    }

    #[test]
    fn test_in_list_null_handling_comprehensive() -> Result<()> {
        // Comprehensive test demonstrating SQL three-valued logic for IN expressions
        // This test explicitly shows all possible outcomes: true, false, and null
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        // Test data: [1, 2, 3, null]
        // - 1 will match in both lists
        // - 2 will not match in either list
        // - 3 will not match in either list
        // - null is always null
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // Case 1: List WITHOUT null - demonstrates true/false/null outcomes
        // "a IN (1, 4)" - 1 matches, 2 and 3 don't match, null is null
        let list = vec![lit(1i64), lit(4i64)];
        in_list!(
            batch,
            list,
            &false,
            vec![
                Some(true),  // 1 is in the list → true
                Some(false), // 2 is not in the list → false
                Some(false), // 3 is not in the list → false
                None,        // null IN (...) → null (SQL three-valued logic)
            ],
            Arc::clone(&col_a),
            &schema
        );

        // Case 2: List WITH null - demonstrates null propagation for non-matches
        // "a IN (1, NULL)" - 1 matches (true), 2/3 don't match but list has null (null), null is null
        let list = vec![lit(1i64), lit(ScalarValue::Int64(None))];
        in_list!(
            batch,
            list,
            &false,
            vec![
                Some(true), // 1 is in the list → true (found match)
                None, // 2 is not in list, but list has NULL → null (might match NULL)
                None, // 3 is not in list, but list has NULL → null (might match NULL)
                None, // null IN (...) → null (SQL three-valued logic)
            ],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_in_list_with_only_nulls() -> Result<()> {
        // Edge case: IN list contains ONLY null values
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(1), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // "a IN (NULL, NULL)" - list has only nulls
        let list = vec![lit(ScalarValue::Int64(None)), lit(ScalarValue::Int64(None))];

        // All results should be NULL because:
        // - Non-null values (1, 2) can't match anything concrete, but list might contain matching value
        // - NULL value is always NULL in IN expressions
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        // "a NOT IN (NULL, NULL)" - list has only nulls
        // All results should still be NULL due to three-valued logic
        in_list!(
            batch,
            list,
            &true,
            vec![None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_in_list_multiple_nulls_deduplication() -> Result<()> {
        // Test that multiple NULLs in the list are handled correctly
        // This verifies deduplication doesn't break null handling
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let col_a = col("a", &schema)?;

        // Create array with multiple nulls: [1, 2, NULL, NULL, 3, NULL]
        let array = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ])) as ArrayRef;

        // Create InListExpr from array
        let expr = Arc::new(InListExpr::try_new_from_array(
            Arc::clone(&col_a),
            array,
            false,
        )?) as Arc<dyn PhysicalExpr>;

        // Create test data: [1, 2, 3, 4, null]
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // Evaluate the expression
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result);

        // Expected behavior with multiple NULLs in list:
        // - Values in the list (1,2,3) → true
        // - Values not in the list (4) → NULL (because list contains NULL)
        // - NULL input → NULL
        let expected = BooleanArray::from(vec![
            Some(true), // 1 is in list
            Some(true), // 2 is in list
            Some(true), // 3 is in list
            None,       // 4 not in list, but list has NULLs
            None,       // NULL input
        ]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_not_in_null_handling_comprehensive() -> Result<()> {
        // Comprehensive test demonstrating SQL three-valued logic for NOT IN expressions
        // This test explicitly shows all possible outcomes for NOT IN: true, false, and null
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        // Test data: [1, 2, 3, null]
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // Case 1: List WITHOUT null - demonstrates true/false/null outcomes for NOT IN
        // "a NOT IN (1, 4)" - 1 matches (false), 2 and 3 don't match (true), null is null
        let list = vec![lit(1i64), lit(4i64)];
        in_list!(
            batch,
            list,
            &true,
            vec![
                Some(false), // 1 is in the list → NOT IN returns false
                Some(true),  // 2 is not in the list → NOT IN returns true
                Some(true),  // 3 is not in the list → NOT IN returns true
                None,        // null NOT IN (...) → null (SQL three-valued logic)
            ],
            Arc::clone(&col_a),
            &schema
        );

        // Case 2: List WITH null - demonstrates null propagation for NOT IN
        // "a NOT IN (1, NULL)" - 1 matches (false), 2/3 don't match but list has null (null), null is null
        let list = vec![lit(1i64), lit(ScalarValue::Int64(None))];
        in_list!(
            batch,
            list,
            &true,
            vec![
                Some(false), // 1 is in the list → NOT IN returns false
                None, // 2 is not in known values, but list has NULL → null (can't prove it's not in list)
                None, // 3 is not in known values, but list has NULL → null (can't prove it's not in list)
                None, // null NOT IN (...) → null (SQL three-valued logic)
            ],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_in_list_null_type_column() -> Result<()> {
        // Test with a column that has DataType::Null (not just nullable values)
        // All values in a NullArray are null by definition
        let schema = Schema::new(vec![Field::new("a", DataType::Null, true)]);
        let a = NullArray::new(3);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // "null_column IN (1, 2)" - comparing Null type against Int64 list
        // Note: This tests type coercion behavior between Null and Int64
        let list = vec![lit(1i64), lit(2i64)];

        // All results should be NULL because:
        // - Every value in the column is null (DataType::Null)
        // - null IN (anything) always returns null per SQL three-valued logic
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        // "null_column NOT IN (1, 2)"
        // Same behavior for NOT IN - null NOT IN (anything) is still null
        in_list!(
            batch,
            list,
            &true,
            vec![None, None, None],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_in_list_null_type_list() -> Result<()> {
        // Test with a list that has DataType::Null
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(1), Some(2), None]);
        let col_a = col("a", &schema)?;

        // Create a NullArray as the list
        let null_array = Arc::new(NullArray::new(2)) as ArrayRef;

        // Try to create InListExpr with a NullArray list
        // This tests whether try_new_from_array can handle Null type arrays
        let expr = Arc::new(InListExpr::try_new_from_array(
            Arc::clone(&col_a),
            null_array,
            false,
        )?) as Arc<dyn PhysicalExpr>;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result);

        // If it succeeds, all results should be NULL
        // because the list contains only null type values
        let expected = BooleanArray::from(vec![None, None, None]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_in_list_null_type_both() -> Result<()> {
        // Test when both column and list are DataType::Null
        let schema = Schema::new(vec![Field::new("a", DataType::Null, true)]);
        let a = NullArray::new(3);
        let col_a = col("a", &schema)?;

        // Create a NullArray as the list
        let null_array = Arc::new(NullArray::new(2)) as ArrayRef;

        // Try to create InListExpr with both Null types
        let expr = Arc::new(InListExpr::try_new_from_array(
            Arc::clone(&col_a),
            null_array,
            false,
        )?) as Arc<dyn PhysicalExpr>;

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result);

        // If successful, all results should be NULL
        // null IN [null, null] -> null
        let expected = BooleanArray::from(vec![None, None, None]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_in_list_comprehensive_null_handling() -> Result<()> {
        // Comprehensive test for IN LIST operations with various NULL handling scenarios.
        // This test covers the key cases validated against DuckDB as the source of truth.
        //
        // Note: Some scalar literal tests (like NULL IN (1, 2)) are omitted as they
        // appear to expose an issue with static filter optimization. These are covered
        // by existing tests like in_list_no_cols().

        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let col_b = col("b", &schema)?;
        let null_i32 = ScalarValue::Int32(None);

        // Helper to create a batch
        let make_batch = |values: Vec<Option<i32>>| -> Result<RecordBatch> {
            let array = Arc::new(Int32Array::from(values));
            Ok(RecordBatch::try_new(Arc::clone(&schema), vec![array])?)
        };

        // Helper to run a test
        let run_test = |batch: &RecordBatch,
                        expr: Arc<dyn PhysicalExpr>,
                        list: Vec<Arc<dyn PhysicalExpr>>,
                        expected: Vec<Option<bool>>|
         -> Result<()> {
            let in_expr = in_list(expr, list, &false, schema.as_ref())?;
            let result = in_expr.evaluate(batch)?.into_array(batch.num_rows())?;
            let result = as_boolean_array(&result);
            assert_eq!(result, &BooleanArray::from(expected));
            Ok(())
        };

        // ========================================================================
        // COLUMN TESTS - col(b) IN [1, 2]
        // ========================================================================

        // [1] IN (1, 2) => [TRUE]
        let batch = make_batch(vec![Some(1)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), lit(2i32)],
            vec![Some(true)],
        )?;

        // [1, 2] IN (1, 2) => [TRUE, TRUE]
        let batch = make_batch(vec![Some(1), Some(2)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), lit(2i32)],
            vec![Some(true), Some(true)],
        )?;

        // [3, 4] IN (1, 2) => [FALSE, FALSE]
        let batch = make_batch(vec![Some(3), Some(4)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), lit(2i32)],
            vec![Some(false), Some(false)],
        )?;

        // [1, NULL] IN (1, 2) => [TRUE, NULL]
        let batch = make_batch(vec![Some(1), None])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), lit(2i32)],
            vec![Some(true), None],
        )?;

        // [3, NULL] IN (1, 2) => [FALSE, NULL] (no match, NULL is NULL)
        let batch = make_batch(vec![Some(3), None])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), lit(2i32)],
            vec![Some(false), None],
        )?;

        // ========================================================================
        // COLUMN WITH NULL IN LIST - col(b) IN [NULL, 1]
        // ========================================================================

        // [1] IN (NULL, 1) => [TRUE] (found match)
        let batch = make_batch(vec![Some(1)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(null_i32.clone()), lit(1i32)],
            vec![Some(true)],
        )?;

        // [2] IN (NULL, 1) => [NULL] (no match, but list has NULL)
        let batch = make_batch(vec![Some(2)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(null_i32.clone()), lit(1i32)],
            vec![None],
        )?;

        // [NULL] IN (NULL, 1) => [NULL]
        let batch = make_batch(vec![None])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(null_i32.clone()), lit(1i32)],
            vec![None],
        )?;

        // ========================================================================
        // COLUMN WITH ALL NULLS IN LIST - col(b) IN [NULL, NULL]
        // ========================================================================

        // [1] IN (NULL, NULL) => [NULL]
        let batch = make_batch(vec![Some(1)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(null_i32.clone()), lit(null_i32.clone())],
            vec![None],
        )?;

        // [NULL] IN (NULL, NULL) => [NULL]
        let batch = make_batch(vec![None])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(null_i32.clone()), lit(null_i32.clone())],
            vec![None],
        )?;

        // ========================================================================
        // LITERAL IN LIST WITH COLUMN - lit(1) IN [2, col(b)]
        // ========================================================================

        // 1 IN (2, [1]) => [TRUE] (matches column value)
        let batch = make_batch(vec![Some(1)])?;
        run_test(
            &batch,
            lit(1i32),
            vec![lit(2i32), Arc::clone(&col_b)],
            vec![Some(true)],
        )?;

        // 1 IN (2, [3]) => [FALSE] (no match)
        let batch = make_batch(vec![Some(3)])?;
        run_test(
            &batch,
            lit(1i32),
            vec![lit(2i32), Arc::clone(&col_b)],
            vec![Some(false)],
        )?;

        // 1 IN (2, [NULL]) => [NULL] (no match, column is NULL)
        let batch = make_batch(vec![None])?;
        run_test(
            &batch,
            lit(1i32),
            vec![lit(2i32), Arc::clone(&col_b)],
            vec![None],
        )?;

        // ========================================================================
        // COLUMN IN LIST CONTAINING ITSELF - col(b) IN [1, col(b)]
        // ========================================================================

        // [1] IN (1, [1]) => [TRUE] (always matches - either list literal or itself)
        let batch = make_batch(vec![Some(1)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), Arc::clone(&col_b)],
            vec![Some(true)],
        )?;

        // [2] IN (1, [2]) => [TRUE] (matches itself)
        let batch = make_batch(vec![Some(2)])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), Arc::clone(&col_b)],
            vec![Some(true)],
        )?;

        // [NULL] IN (1, [NULL]) => [NULL] (NULL is never equal to anything)
        let batch = make_batch(vec![None])?;
        run_test(
            &batch,
            Arc::clone(&col_b),
            vec![lit(1i32), Arc::clone(&col_b)],
            vec![None],
        )?;

        Ok(())
    }

    #[test]
    fn test_in_list_scalar_literal_cases() -> Result<()> {
        // Test scalar literal cases (both NULL and non-NULL) to ensure SQL three-valued
        // logic is correctly implemented. This covers the important case where a scalar
        // value is tested against a list containing NULL.

        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let null_i32 = ScalarValue::Int32(None);

        // Helper to create a batch
        let make_batch = |values: Vec<Option<i32>>| -> Result<RecordBatch> {
            let array = Arc::new(Int32Array::from(values));
            Ok(RecordBatch::try_new(Arc::clone(&schema), vec![array])?)
        };

        // Helper to run a test
        let run_test = |batch: &RecordBatch,
                        expr: Arc<dyn PhysicalExpr>,
                        list: Vec<Arc<dyn PhysicalExpr>>,
                        negated: bool,
                        expected: Vec<Option<bool>>|
         -> Result<()> {
            let in_expr = in_list(expr, list, &negated, schema.as_ref())?;
            let result = in_expr.evaluate(batch)?.into_array(batch.num_rows())?;
            let result = as_boolean_array(&result);
            let expected_array = BooleanArray::from(expected);
            assert_eq!(
                result,
                &expected_array,
                "Expected {:?}, got {:?}",
                expected_array,
                result.iter().collect::<Vec<_>>()
            );
            Ok(())
        };

        let batch = make_batch(vec![Some(1)])?;

        // ========================================================================
        // NULL LITERAL TESTS
        // According to SQL semantics, NULL IN (any_list) should always return NULL
        // ========================================================================

        // NULL IN (1, 1) => NULL
        run_test(
            &batch,
            lit(null_i32.clone()),
            vec![lit(1i32), lit(1i32)],
            false,
            vec![None],
        )?;

        // NULL IN (NULL, 1) => NULL
        run_test(
            &batch,
            lit(null_i32.clone()),
            vec![lit(null_i32.clone()), lit(1i32)],
            false,
            vec![None],
        )?;

        // NULL IN (NULL, NULL) => NULL
        run_test(
            &batch,
            lit(null_i32.clone()),
            vec![lit(null_i32.clone()), lit(null_i32.clone())],
            false,
            vec![None],
        )?;

        // ========================================================================
        // NON-NULL SCALAR LITERALS WITH NULL IN LIST - Int32
        // When a scalar value is NOT in a list containing NULL, the result is NULL
        // When a scalar value IS in the list, the result is TRUE (NULL doesn't matter)
        // ========================================================================

        // 3 IN (0, 1, 2, NULL) => NULL (not in list, but list has NULL)
        run_test(
            &batch,
            lit(3i32),
            vec![lit(0i32), lit(1i32), lit(2i32), lit(null_i32.clone())],
            false,
            vec![None],
        )?;

        // 3 NOT IN (0, 1, 2, NULL) => NULL (not in list, but list has NULL)
        run_test(
            &batch,
            lit(3i32),
            vec![lit(0i32), lit(1i32), lit(2i32), lit(null_i32.clone())],
            true,
            vec![None],
        )?;

        // 1 IN (0, 1, 2, NULL) => TRUE (found match, NULL doesn't matter)
        run_test(
            &batch,
            lit(1i32),
            vec![lit(0i32), lit(1i32), lit(2i32), lit(null_i32.clone())],
            false,
            vec![Some(true)],
        )?;

        // 1 NOT IN (0, 1, 2, NULL) => FALSE (found match, NULL doesn't matter)
        run_test(
            &batch,
            lit(1i32),
            vec![lit(0i32), lit(1i32), lit(2i32), lit(null_i32.clone())],
            true,
            vec![Some(false)],
        )?;

        // ========================================================================
        // NON-NULL SCALAR LITERALS WITH NULL IN LIST - String
        // Same semantics as Int32 but with string type
        // ========================================================================

        let schema_str =
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch_str = RecordBatch::try_new(
            Arc::clone(&schema_str),
            vec![Arc::new(StringArray::from(vec![Some("dummy")]))],
        )?;
        let null_str = ScalarValue::Utf8(None);

        let run_test_str = |expr: Arc<dyn PhysicalExpr>,
                            list: Vec<Arc<dyn PhysicalExpr>>,
                            negated: bool,
                            expected: Vec<Option<bool>>|
         -> Result<()> {
            let in_expr = in_list(expr, list, &negated, schema_str.as_ref())?;
            let result = in_expr
                .evaluate(&batch_str)?
                .into_array(batch_str.num_rows())?;
            let result = as_boolean_array(&result);
            let expected_array = BooleanArray::from(expected);
            assert_eq!(
                result,
                &expected_array,
                "Expected {:?}, got {:?}",
                expected_array,
                result.iter().collect::<Vec<_>>()
            );
            Ok(())
        };

        // 'c' IN ('a', 'b', NULL) => NULL (not in list, but list has NULL)
        run_test_str(
            lit("c"),
            vec![lit("a"), lit("b"), lit(null_str.clone())],
            false,
            vec![None],
        )?;

        // 'c' NOT IN ('a', 'b', NULL) => NULL (not in list, but list has NULL)
        run_test_str(
            lit("c"),
            vec![lit("a"), lit("b"), lit(null_str.clone())],
            true,
            vec![None],
        )?;

        // 'a' IN ('a', 'b', NULL) => TRUE (found match, NULL doesn't matter)
        run_test_str(
            lit("a"),
            vec![lit("a"), lit("b"), lit(null_str.clone())],
            false,
            vec![Some(true)],
        )?;

        // 'a' NOT IN ('a', 'b', NULL) => FALSE (found match, NULL doesn't matter)
        run_test_str(
            lit("a"),
            vec![lit("a"), lit("b"), lit(null_str.clone())],
            true,
            vec![Some(false)],
        )?;

        Ok(())
    }

    #[test]
    fn test_in_list_tuple_cases() -> Result<()> {
        // Test tuple/struct cases from the original request: (lit, lit) IN (lit, lit)
        // These test row-wise comparisons like (1, 2) IN ((1, 2), (3, 4))

        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));

        // Helper to create struct scalars for tuple comparisons
        let make_struct = |v1: Option<i32>, v2: Option<i32>| -> ScalarValue {
            let fields = Fields::from(vec![
                Field::new("field_0", DataType::Int32, true),
                Field::new("field_1", DataType::Int32, true),
            ]);
            ScalarValue::Struct(Arc::new(StructArray::new(
                fields,
                vec![
                    Arc::new(Int32Array::from(vec![v1])),
                    Arc::new(Int32Array::from(vec![v2])),
                ],
                None,
            )))
        };

        // Need a single row batch for scalar tests
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )?;

        // Helper to run tuple tests
        let run_tuple_test = |lhs: ScalarValue,
                              list: Vec<ScalarValue>,
                              expected: Vec<Option<bool>>|
         -> Result<()> {
            let expr = in_list(
                lit(lhs),
                list.into_iter().map(lit).collect(),
                &false,
                schema.as_ref(),
            )?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            let result = as_boolean_array(&result);
            assert_eq!(result, &BooleanArray::from(expected));
            Ok(())
        };

        // (NULL, NULL) IN ((1, 2)) => FALSE (tuples don't match)
        run_tuple_test(
            make_struct(None, None),
            vec![make_struct(Some(1), Some(2))],
            vec![Some(false)],
        )?;

        // (NULL, NULL) IN ((NULL, 1)) => FALSE
        run_tuple_test(
            make_struct(None, None),
            vec![make_struct(None, Some(1))],
            vec![Some(false)],
        )?;

        // (NULL, NULL) IN ((NULL, NULL)) => TRUE (exact match including nulls)
        run_tuple_test(
            make_struct(None, None),
            vec![make_struct(None, None)],
            vec![Some(true)],
        )?;

        // (NULL, 1) IN ((1, 2)) => FALSE
        run_tuple_test(
            make_struct(None, Some(1)),
            vec![make_struct(Some(1), Some(2))],
            vec![Some(false)],
        )?;

        // (NULL, 1) IN ((NULL, 1)) => TRUE (exact match)
        run_tuple_test(
            make_struct(None, Some(1)),
            vec![make_struct(None, Some(1))],
            vec![Some(true)],
        )?;

        // (NULL, 1) IN ((NULL, NULL)) => FALSE
        run_tuple_test(
            make_struct(None, Some(1)),
            vec![make_struct(None, None)],
            vec![Some(false)],
        )?;

        // (1, 2) IN ((1, 2)) => TRUE
        run_tuple_test(
            make_struct(Some(1), Some(2)),
            vec![make_struct(Some(1), Some(2))],
            vec![Some(true)],
        )?;

        // (1, 3) IN ((1, 2)) => FALSE
        run_tuple_test(
            make_struct(Some(1), Some(3)),
            vec![make_struct(Some(1), Some(2))],
            vec![Some(false)],
        )?;

        // (4, 4) IN ((1, 2)) => FALSE
        run_tuple_test(
            make_struct(Some(4), Some(4)),
            vec![make_struct(Some(1), Some(2))],
            vec![Some(false)],
        )?;

        // (1, 1) IN ((NULL, 1)) => FALSE
        run_tuple_test(
            make_struct(Some(1), Some(1)),
            vec![make_struct(None, Some(1))],
            vec![Some(false)],
        )?;

        // (1, 1) IN ((NULL, NULL)) => FALSE
        run_tuple_test(
            make_struct(Some(1), Some(1)),
            vec![make_struct(None, None)],
            vec![Some(false)],
        )?;

        Ok(())
    }

    #[test]
    fn test_in_list_dictionary_int32() -> Result<()> {
        // Create schema with dictionary-encoded Int32 column
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32));
        let schema = Schema::new(vec![Field::new("a", dict_type.clone(), false)]);
        let col_a = col("a", &schema)?;

        // Create IN list with Int32 literals: (100, 200, 300)
        let list = vec![lit(100i32), lit(200i32), lit(300i32)];

        // Create InListExpr via in_list() - this uses Int32StaticFilter for Int32 lists
        let expr = in_list(col_a, list, &false, &schema)?;

        // Create dictionary-encoded batch with values [100, 200, 500]
        // Dictionary: keys [0, 1, 2] -> values [100, 200, 500]
        // Using values clearly distinct from keys to avoid confusion
        let keys = Int8Array::from(vec![0, 1, 2]);
        let values = Int32Array::from(vec![100, 200, 500]);
        let dict_array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values))?);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![dict_array])?;

        // Expected: [100 IN (100,200,300), 200 IN (100,200,300), 500 IN (100,200,300)] = [true, true, false]
        let result = expr.evaluate(&batch)?.into_array(3)?;
        let result = as_boolean_array(&result);
        assert_eq!(result, &BooleanArray::from(vec![true, true, false]));
        Ok(())
    }

    #[test]
    fn test_in_list_dictionary_types() -> Result<()> {
        // Helper functions for creating dictionary literals
        fn dict_lit_int64(key_type: DataType, value: i64) -> Arc<dyn PhysicalExpr> {
            lit(ScalarValue::Dictionary(
                Box::new(key_type),
                Box::new(ScalarValue::Int64(Some(value))),
            ))
        }

        fn dict_lit_float64(key_type: DataType, value: f64) -> Arc<dyn PhysicalExpr> {
            lit(ScalarValue::Dictionary(
                Box::new(key_type),
                Box::new(ScalarValue::Float64(Some(value))),
            ))
        }

        // Test case structures
        struct DictNeedleTest {
            list_values: Vec<Arc<dyn PhysicalExpr>>,
            expected: Vec<Option<bool>>,
        }

        struct DictionaryInListTestCase {
            name: &'static str,
            dict_type: DataType,
            dict_keys: Vec<Option<i8>>,
            dict_values: ArrayRef,
            list_values_no_null: Vec<Arc<dyn PhysicalExpr>>,
            list_values_with_null: Vec<Arc<dyn PhysicalExpr>>,
            expected_1: Vec<Option<bool>>,
            expected_2: Vec<Option<bool>>,
            expected_3: Vec<Option<bool>>,
            expected_4: Vec<Option<bool>>,
            dict_needle_test: Option<DictNeedleTest>,
        }

        // Test harness function
        fn run_dictionary_in_list_test(
            test_case: DictionaryInListTestCase,
        ) -> Result<()> {
            // Create schema with dictionary type
            let schema =
                Schema::new(vec![Field::new("a", test_case.dict_type.clone(), true)]);
            let col_a = col("a", &schema)?;

            // Create dictionary array from keys and values
            let keys = Int8Array::from(test_case.dict_keys.clone());
            let dict_array: ArrayRef =
                Arc::new(DictionaryArray::try_new(keys, test_case.dict_values)?);
            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![dict_array])?;

            let exp1 = test_case.expected_1.clone();
            let exp2 = test_case.expected_2.clone();
            let exp3 = test_case.expected_3.clone();
            let exp4 = test_case.expected_4;

            // Test 1: a IN (values_no_null)
            in_list!(
                batch,
                test_case.list_values_no_null.clone(),
                &false,
                exp1,
                Arc::clone(&col_a),
                &schema
            );

            // Test 2: a NOT IN (values_no_null)
            in_list!(
                batch,
                test_case.list_values_no_null.clone(),
                &true,
                exp2,
                Arc::clone(&col_a),
                &schema
            );

            // Test 3: a IN (values_with_null)
            in_list!(
                batch,
                test_case.list_values_with_null.clone(),
                &false,
                exp3,
                Arc::clone(&col_a),
                &schema
            );

            // Test 4: a NOT IN (values_with_null)
            in_list!(
                batch,
                test_case.list_values_with_null,
                &true,
                exp4,
                Arc::clone(&col_a),
                &schema
            );

            // Optional: Dictionary needle test (if provided)
            if let Some(needle_test) = test_case.dict_needle_test {
                in_list_raw!(
                    batch,
                    needle_test.list_values,
                    &false,
                    needle_test.expected,
                    Arc::clone(&col_a),
                    &schema
                );
            }

            Ok(())
        }

        // Test case 1: UTF8
        // Dictionary: keys [0, 1, null] → values ["a", "d", -]
        // Rows: ["a", "d", null]
        let utf8_case = DictionaryInListTestCase {
            name: "dictionary_utf8",
            dict_type: DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8),
            ),
            dict_keys: vec![Some(0), Some(1), None],
            dict_values: Arc::new(StringArray::from(vec![Some("a"), Some("d")])),
            list_values_no_null: vec![lit("a"), lit("b")],
            list_values_with_null: vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))],
            expected_1: vec![Some(true), Some(false), None],
            expected_2: vec![Some(false), Some(true), None],
            expected_3: vec![Some(true), None, None],
            expected_4: vec![Some(false), None, None],
            dict_needle_test: None,
        };

        // Test case 2: Int64 with dictionary needles
        // Dictionary: keys [0, 1, null] → values [10, 20, -]
        // Rows: [10, 20, null]
        let int64_case = DictionaryInListTestCase {
            name: "dictionary_int64",
            dict_type: DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Int64),
            ),
            dict_keys: vec![Some(0), Some(1), None],
            dict_values: Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
            list_values_no_null: vec![lit(10i64), lit(15i64)],
            list_values_with_null: vec![
                lit(10i64),
                lit(15i64),
                lit(ScalarValue::Int64(None)),
            ],
            expected_1: vec![Some(true), Some(false), None],
            expected_2: vec![Some(false), Some(true), None],
            expected_3: vec![Some(true), None, None],
            expected_4: vec![Some(false), None, None],
            dict_needle_test: Some(DictNeedleTest {
                list_values: vec![
                    dict_lit_int64(DataType::Int16, 10),
                    dict_lit_int64(DataType::Int16, 15),
                ],
                expected: vec![Some(true), Some(false), None],
            }),
        };

        // Test case 3: Float64 with NaN and dictionary needles
        // Dictionary: keys [0, 1, null, 2] → values [1.5, 3.7, NaN, -]
        // Rows: [1.5, 3.7, null, NaN]
        // Note: NaN is a value (not null), so it goes in the values array
        let float64_case = DictionaryInListTestCase {
            name: "dictionary_float64",
            dict_type: DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Float64),
            ),
            dict_keys: vec![Some(0), Some(1), None, Some(2)],
            dict_values: Arc::new(Float64Array::from(vec![
                Some(1.5),      // index 0
                Some(3.7),      // index 1
                Some(f64::NAN), // index 2
            ])),
            list_values_no_null: vec![lit(1.5f64), lit(2.0f64)],
            list_values_with_null: vec![
                lit(1.5f64),
                lit(2.0f64),
                lit(ScalarValue::Float64(None)),
            ],
            // Test 1: a IN (1.5, 2.0) → [true, false, null, false]
            // NaN is false because NaN not in list and no NULL in list
            expected_1: vec![Some(true), Some(false), None, Some(false)],
            // Test 2: a NOT IN (1.5, 2.0) → [false, true, null, true]
            // NaN is true because NaN not in list
            expected_2: vec![Some(false), Some(true), None, Some(true)],
            // Test 3: a IN (1.5, 2.0, NULL) → [true, null, null, null]
            // 3.7 and NaN become null due to NULL in list (three-valued logic)
            expected_3: vec![Some(true), None, None, None],
            // Test 4: a NOT IN (1.5, 2.0, NULL) → [false, null, null, null]
            // 3.7 and NaN become null due to NULL in list
            expected_4: vec![Some(false), None, None, None],
            dict_needle_test: Some(DictNeedleTest {
                list_values: vec![
                    dict_lit_float64(DataType::UInt16, 1.5),
                    dict_lit_float64(DataType::UInt16, 2.0),
                ],
                expected: vec![Some(true), Some(false), None, Some(false)],
            }),
        };

        // Execute all test cases
        let test_name = utf8_case.name;
        run_dictionary_in_list_test(utf8_case).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Dictionary test '{test_name}' failed: {e}"
            ))
        })?;

        let test_name = int64_case.name;
        run_dictionary_in_list_test(int64_case).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Dictionary test '{test_name}' failed: {e}"
            ))
        })?;

        let test_name = float64_case.name;
        run_dictionary_in_list_test(float64_case).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Dictionary test '{test_name}' failed: {e}"
            ))
        })?;

        // Additional test: Dictionary deduplication with repeated keys
        // This tests that multiple rows with the same key (pointing to the same value)
        // are evaluated correctly
        let dedup_case = DictionaryInListTestCase {
            name: "dictionary_deduplication",
            dict_type: DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8),
            ),
            // Keys: [0, 1, 0, 1, null] - keys 0 and 1 are repeated
            // This creates data: ["a", "d", "a", "d", null]
            dict_keys: vec![Some(0), Some(1), Some(0), Some(1), None],
            dict_values: Arc::new(StringArray::from(vec![Some("a"), Some("d")])),
            list_values_no_null: vec![lit("a"), lit("b")],
            list_values_with_null: vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))],
            // Test 1: a IN ("a", "b") → [true, false, true, false, null]
            // Rows 0 and 2 both have key 0 → "a", so both are true
            expected_1: vec![Some(true), Some(false), Some(true), Some(false), None],
            // Test 2: a NOT IN ("a", "b") → [false, true, false, true, null]
            expected_2: vec![Some(false), Some(true), Some(false), Some(true), None],
            // Test 3: a IN ("a", "b", NULL) → [true, null, true, null, null]
            // "d" becomes null due to NULL in list
            expected_3: vec![Some(true), None, Some(true), None, None],
            // Test 4: a NOT IN ("a", "b", NULL) → [false, null, false, null, null]
            expected_4: vec![Some(false), None, Some(false), None, None],
            dict_needle_test: None,
        };

        let test_name = dedup_case.name;
        run_dictionary_in_list_test(dedup_case).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Dictionary test '{test_name}' failed: {e}"
            ))
        })?;

        // Additional test for Float64 NaN in IN list
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float64));
        let schema = Schema::new(vec![Field::new("a", dict_type.clone(), true)]);
        let col_a = col("a", &schema)?;

        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Float64Array::from(vec![Some(1.5), Some(3.7), Some(f64::NAN)]);
        let dict_array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values))?);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![dict_array])?;

        // Test: a IN (1.5, 2.0, NaN)
        let list_with_nan = vec![lit(1.5f64), lit(2.0f64), lit(f64::NAN)];
        in_list!(
            batch,
            list_with_nan,
            &false,
            vec![Some(true), Some(false), None, Some(true)],
            col_a,
            &schema
        );

        Ok(())
    }
}

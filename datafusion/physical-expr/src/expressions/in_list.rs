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

use crate::physical_expr::physical_exprs_bag_equal;
use crate::PhysicalExpr;

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::{not, or_kleene};
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use datafusion_common::cast::{
    as_binary_view_array, as_boolean_array, as_generic_binary_array, as_string_array,
    as_string_view_array, as_struct_array,
};
use datafusion_common::hash_utils::{create_hashes, HashValue};
use datafusion_common::{
    exec_err, internal_err, not_impl_err, DFSchema, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::datum::compare_with_eq;

use ahash::RandomState;
use datafusion_common::HashMap;
use hashbrown::hash_map::RawEntryMut;

/// Storage for InList values - either as an array or as expressions
pub(crate) enum InListStorage {
    /// Homogeneous list stored as an ArrayRef (memory efficient)
    Array(ArrayRef),
    /// Heterogeneous or dynamic list stored as expressions
    Exprs(Vec<Arc<dyn PhysicalExpr>>),
}

/// InList
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    pub(crate) list: InListStorage,
    negated: bool,
    static_filter: Option<Arc<dyn Set>>,
}

impl Debug for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("InListExpr");
        debug_struct.field("expr", &self.expr);

        match &self.list {
            InListStorage::Array(array) => {
                debug_struct.field("list", &format!("Array({:?})", array.data_type()));
            }
            InListStorage::Exprs(exprs) => {
                debug_struct.field("list", exprs);
            }
        }

        debug_struct.field("negated", &self.negated).finish()
    }
}

/// A type-erased container of array elements
pub trait Set: Send + Sync {
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
    fn has_nulls(&self) -> bool;
}

struct ArrayHashSet {
    state: RandomState,
    /// Used to provide a lookup from value to in list index
    ///
    /// Note: usize::hash is not used, instead the raw entry
    /// API is used to store entries w.r.t their value
    map: HashMap<usize, (), ()>,
}

struct ArraySet<T> {
    array: T,
    hash_set: ArrayHashSet,
}

impl<T> ArraySet<T>
where
    T: Array + From<ArrayData>,
{
    fn new(array: &T, hash_set: ArrayHashSet) -> Self {
        Self {
            array: downcast_array(array),
            hash_set,
        }
    }
}

impl<T> Set for ArraySet<T>
where
    T: Array + 'static,
    for<'a> &'a T: ArrayAccessor,
    for<'a> <&'a T as ArrayAccessor>::Item: IsEqual,
{
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(downcast_array(result.as_ref()))
            }
            _ => {}
        }

        let v = v.as_any().downcast_ref::<T>().unwrap();
        let in_array = &self.array;
        let has_nulls = in_array.null_count() != 0;

        Ok(ArrayIter::new(v)
            .map(|v| {
                v.and_then(|v| {
                    let hash = v.hash_one(&self.hash_set.state);
                    let contains = self
                        .hash_set
                        .map
                        .raw_entry()
                        .from_hash(hash, |idx| in_array.value(*idx).is_equal(&v))
                        .is_some();

                    match contains {
                        true => Some(!negated),
                        false if has_nulls => None,
                        false => Some(negated),
                    }
                })
            })
            .collect())
    }

    fn has_nulls(&self) -> bool {
        self.array.null_count() != 0
    }
}

/// Specialized Set implementation for StructArray
struct StructArraySet {
    array: Arc<StructArray>,
    hash_set: ArrayHashSet,
}

impl Set for StructArraySet {
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        // Handle dictionary arrays
        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(downcast_array(result.as_ref()))
            }
            _ => {}
        }

        let v = as_struct_array(v)?;
        let has_nulls = self.array.null_count() != 0;

        // Compute hashes for all rows in the input array
        let mut input_hashes = vec![0u64; v.len()];
        let v_arc = Arc::new(v.clone()) as ArrayRef;
        create_hashes(
            &[v_arc],
            &self.hash_set.state,
            &mut input_hashes,
        )?;

        // Check each row for membership
        let result: BooleanArray = (0..v.len())
            .map(|i| {
                // Handle null rows
                if v.is_null(i) {
                    return None;
                }

                let hash = input_hashes[i];
                let input_row = v.slice(i, 1);

                // Look up in hash map with equality check
                let contains = self
                    .hash_set
                    .map
                    .raw_entry()
                    .from_hash(hash, |&idx| {
                        let set_row = self.array.slice(idx, 1);
                        match compare_with_eq(&set_row, &input_row, true) {
                            Ok(result) => result.value(0),
                            Err(_) => false,
                        }
                    })
                    .is_some();

                match contains {
                    true => Some(!negated),
                    false if has_nulls => None,
                    false => Some(negated),
                }
            })
            .collect();

        Ok(result)
    }

    fn has_nulls(&self) -> bool {
        self.array.null_count() != 0
    }
}

/// Computes an [`ArrayHashSet`] for the provided [`Array`] if there
/// are nulls present or there are more than the configured number of
/// elements.
///
/// Note: This is split into a separate function as higher-rank trait bounds currently
/// cause type inference to misbehave
fn make_hash_set<T>(array: T) -> ArrayHashSet
where
    T: ArrayAccessor,
    T::Item: IsEqual,
{
    let state = RandomState::new();
    let mut map: HashMap<usize, (), ()> =
        HashMap::with_capacity_and_hasher(array.len(), ());

    let insert_value = |idx| {
        let value = array.value(idx);
        let hash = value.hash_one(&state);
        if let RawEntryMut::Vacant(v) = map
            .raw_entry_mut()
            .from_hash(hash, |x| array.value(*x).is_equal(&value))
        {
            v.insert_with_hasher(hash, idx, (), |x| array.value(*x).hash_one(&state));
        }
    };

    match array.nulls() {
        Some(nulls) => {
            BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                .for_each(insert_value)
        }
        None => (0..array.len()).for_each(insert_value),
    }

    ArrayHashSet { state, map }
}

/// Computes an [`ArrayHashSet`] for the provided [`StructArray`]
fn make_struct_hash_set(array: &Arc<StructArray>) -> Result<ArrayHashSet> {
    let state = RandomState::new();
    let mut map: HashMap<usize, (), ()> =
        HashMap::with_capacity_and_hasher(array.len(), ());

    // Compute hashes for all rows
    let mut row_hashes = vec![0u64; array.len()];
    create_hashes(&[Arc::clone(array) as ArrayRef], &state, &mut row_hashes)?;

    // Build hash set, deduplicating based on struct equality
    let insert_value = |idx: usize| {
        let hash = row_hashes[idx];
        if let RawEntryMut::Vacant(v) =
            map.raw_entry_mut().from_hash(hash, |&existing_idx| {
                // Compare the two struct rows for equality
                // Use slice to get single-row arrays for comparison
                let existing_row = array.slice(existing_idx, 1);
                let current_row = array.slice(idx, 1);

                // Use compare_op_for_nested for struct equality
                match compare_with_eq(&existing_row, &current_row, true) {
                    Ok(result) => result.value(0),
                    Err(_) => false,
                }
            })
        {
            v.insert_with_hasher(hash, idx, (), |&x| row_hashes[x]);
        }
    };

    match array.nulls() {
        Some(nulls) => {
            BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                .for_each(insert_value)
        }
        None => (0..array.len()).for_each(insert_value),
    }

    Ok(ArrayHashSet { state, map })
}

/// Creates a `Box<dyn Set>` for the given list of `IN` expressions and `batch`.
/// Accepts a parameter `f` which allows access to the underlying `ArrayHashSet`
/// and is used by [`in_list_from_array`] to collect unique indices.
fn make_set<F>(array: &dyn Array, mut f: F) -> Result<Arc<dyn Set>>
where
    F: FnMut(ArrayHashSet) -> Result<ArrayHashSet>,
{
    Ok(downcast_primitive_array! {
        array => {
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        },
        DataType::Boolean => {
            let array = as_boolean_array(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        },
        DataType::Utf8 => {
            let array = as_string_array(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::LargeUtf8 => {
            let array = as_largestring_array(array);
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::Utf8View => {
            let array = as_string_view_array(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::Binary => {
            let array = as_generic_binary_array::<i32>(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::LargeBinary => {
            let array = as_generic_binary_array::<i64>(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::BinaryView => {
            let array = as_binary_view_array(array)?;
            let hash_set = f(make_hash_set(array))?;
            Arc::new(ArraySet::new(array, hash_set))
        }
        DataType::Struct(_) => {
            let array = as_struct_array(array)?;
            let array_arc = Arc::new(array.clone());
            let hash_set = f(make_struct_hash_set(&array_arc)?)?;
            Arc::new(StructArraySet {
                array: array_arc,
                hash_set,
            })
        }
        DataType::Dictionary(_, _) => unreachable!("dictionary should have been flattened"),
        d => return not_impl_err!("DataType::{d} not supported in InList")
    })
}

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
/// Returns an ArrayRef if all expressions are constants (can be evaluated on an
/// empty RecordBatch), otherwise returns an error. This is used to detect when
/// a list contains only literals, casts of literals, or other constant expressions.
fn try_evaluate_constant_list(
    list: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<ArrayRef> {
    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
    evaluate_list(list, &batch)
}

fn try_cast_static_filter_to_set(
    list: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<Arc<dyn Set>> {
    let array = try_evaluate_constant_list(list, schema)?;
    make_set(array.as_ref(), Ok)
}

/// Custom equality check function which is used with [`ArrayHashSet`] for existence check.
trait IsEqual: HashValue {
    fn is_equal(&self, other: &Self) -> bool;
}

impl<T: IsEqual + ?Sized> IsEqual for &T {
    fn is_equal(&self, other: &Self) -> bool {
        T::is_equal(self, other)
    }
}

macro_rules! is_equal {
    ($($t:ty),+) => {
        $(impl IsEqual for $t {
            fn is_equal(&self, other: &Self) -> bool {
                self == other
            }
        })*
    };
}
is_equal!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64);
is_equal!(bool, str, [u8]);
is_equal!(IntervalDayTime, IntervalMonthDayNano);

macro_rules! is_equal_float {
    ($($t:ty),+) => {
        $(impl IsEqual for $t {
            fn is_equal(&self, other: &Self) -> bool {
                self.to_bits() == other.to_bits()
            }
        })*
    };
}
is_equal_float!(half::f16, f32, f64);

impl InListExpr {
    /// Create a new InList expression from a list of expressions
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        static_filter: Option<Arc<dyn Set>>,
    ) -> Self {
        Self {
            expr,
            list: InListStorage::Exprs(list),
            negated,
            static_filter,
        }
    }

    /// Create a new InList expression from an array
    ///
    /// This is more memory efficient than using [`Self::new`] when the list
    /// is homogeneous and stored as an array. The array is stored directly
    /// without converting to individual expression objects.
    pub fn new_from_array(
        expr: Arc<dyn PhysicalExpr>,
        array: ArrayRef,
        negated: bool,
        static_filter: Option<Arc<dyn Set>>,
    ) -> Self {
        Self {
            expr,
            list: InListStorage::Array(array),
            negated,
            static_filter,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Returns the number of items in the list
    pub fn len(&self) -> usize {
        match &self.list {
            InListStorage::Exprs(exprs) => exprs.len(),
            InListStorage::Array(array) => array.len(),
        }
    }

    /// Returns true if the list is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the list items as expressions.
    ///
    /// For homogeneous lists stored as arrays, this materializes the array
    /// elements into literal expressions. This is intended for external
    /// consumers; internal code should use direct field access to avoid
    /// materialization overhead.
    ///
    /// # Errors
    /// Returns an error if array elements cannot be converted to ScalarValues.
    pub fn list(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        match &self.list {
            InListStorage::Exprs(exprs) => Ok(exprs.clone()),
            InListStorage::Array(array) => {
                // Materialize array elements into literal expressions
                (0..array.len())
                    .map(|i| {
                        let scalar = ScalarValue::try_from_array(array, i)?;
                        Ok(crate::expressions::lit(scalar))
                    })
                    .collect::<Result<Vec<_>>>()
            }
        }
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }
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

impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let list_str = match &self.list {
            InListStorage::Array(array) => {
                // Format a few sample values from the array
                let sample_size = 3.min(array.len());
                let mut items = Vec::new();
                for i in 0..sample_size {
                    match ScalarValue::try_from_array(array, i) {
                        Ok(scalar) => items.push(format!("{scalar}")),
                        Err(_) => items.push("?".to_string()),
                    }
                }
                if array.len() > sample_size {
                    items.push(format!("... ({} total)", array.len()));
                }
                items.join(", ")
            }
            InListStorage::Exprs(exprs) => expr_vec_fmt!(exprs),
        };

        if self.negated {
            if self.static_filter.is_some() {
                write!(f, "{} NOT IN (SET) ([{list_str}])", self.expr)
            } else {
                write!(f, "{} NOT IN ([{list_str}])", self.expr)
            }
        } else if self.static_filter.is_some() {
            write!(f, "{} IN (SET) ([{list_str}])", self.expr)
        } else {
            write!(f, "{} IN ([{list_str}])", self.expr)
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
            Ok(static_filter.has_nulls())
        } else {
            match &self.list {
                InListStorage::Array(array) => Ok(array.null_count() > 0),
                InListStorage::Exprs(exprs) => {
                    for expr in exprs {
                        if expr.nullable(input_schema)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
            }
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let value = self.expr.evaluate(batch)?;
        let r = match &self.static_filter {
            Some(f) => f.contains(value.into_array(num_rows)?.as_ref(), self.negated)?,
            None => {
                match &self.list {
                    InListStorage::Array(_) => {
                        // Array storage should always have a static_filter
                        return internal_err!(
                            "InList with Array storage should always have a static_filter"
                        );
                    }
                    InListStorage::Exprs(exprs) => {
                        let value = value.into_array(num_rows)?;
                        let is_nested = value.data_type().is_nested();
                        let found =
                            exprs.iter().map(|expr| expr.evaluate(batch)).try_fold(
                                BooleanArray::new(
                                    BooleanBuffer::new_unset(num_rows),
                                    None,
                                ),
                                |result, expr| -> Result<BooleanArray> {
                                    let rhs = compare_with_eq(
                                        &value,
                                        &expr?.into_array(num_rows)?,
                                        is_nested,
                                    )?;
                                    Ok(or_kleene(&result, &rhs)?)
                                },
                            )?;

                        if self.negated {
                            not(&found)?
                        } else {
                            found
                        }
                    }
                }
            }
        };
        Ok(ColumnarValue::Array(Arc::new(r)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![&self.expr];
        match &self.list {
            InListStorage::Array(_) => {
                // Array storage has no expression children, just the tested expr
            }
            InListStorage::Exprs(exprs) => {
                children.extend(exprs);
            }
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match &self.list {
            InListStorage::Array(array) => {
                // Array case: only the expr changes, list stays the same
                Ok(Arc::new(InListExpr::new_from_array(
                    Arc::clone(&children[0]),
                    Arc::<dyn Array>::clone(array),
                    self.negated,
                    self.static_filter.clone(),
                )))
            }
            InListStorage::Exprs(_) => {
                // Exprs case: expr + list items all change
                Ok(Arc::new(InListExpr::new(
                    Arc::clone(&children[0]),
                    children[1..].to_vec(),
                    self.negated,
                    self.static_filter.clone(),
                )))
            }
        }
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt_sql(f)?;
        if self.negated {
            write!(f, " NOT")?;
        }

        write!(f, " IN (")?;
        match &self.list {
            InListStorage::Array(array) => {
                // Format array elements as SQL values
                for i in 0..array.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    match ScalarValue::try_from_array(array, i) {
                        Ok(scalar) => write!(f, "{scalar}")?,
                        Err(_) => write!(f, "?")?,
                    }
                }
            }
            InListStorage::Exprs(exprs) => {
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    expr.fmt_sql(f)?;
                }
            }
        }
        write!(f, ")")
    }
}

impl PartialEq for InListExpr {
    fn eq(&self, other: &Self) -> bool {
        if !self.expr.eq(&other.expr) || self.negated != other.negated {
            return false;
        }

        match (&self.list, &other.list) {
            (InListStorage::Array(a1), InListStorage::Array(a2)) => {
                a1.data_type() == a2.data_type()
                    && a1.len() == a2.len()
                    && a1.to_data() == a2.to_data()
            }
            (InListStorage::Exprs(e1), InListStorage::Exprs(e2)) => {
                physical_exprs_bag_equal(e1, e2)
            }
            _ => false,
        }
    }
}

impl Eq for InListExpr {}

impl Hash for InListExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.negated.hash(state);
        match &self.list {
            InListStorage::Array(array) => {
                // Hash discriminant + array metadata
                0_u8.hash(state);
                array.data_type().hash(state);
                array.len().hash(state);
                // Note: We don't hash the actual array data for performance
                // This means hash collisions are possible, but equality will still work correctly
            }
            InListStorage::Exprs(exprs) => {
                1_u8.hash(state);
                exprs.hash(state);
            }
        }
        // Add `self.static_filter` when hash is available
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // check the data type
    let expr_data_type = expr.data_type(schema)?;
    for list_expr in list.iter() {
        let list_expr_data_type = list_expr.data_type(schema)?;
        if !DFSchema::datatype_is_logically_equal(&expr_data_type, &list_expr_data_type) {
            return internal_err!(
                "The data type inlist should be same, the value type is {expr_data_type}, one of list expr type is {list_expr_data_type}"
            );
        }
    }
    let static_filter = try_cast_static_filter_to_set(&list, schema).ok();
    Ok(Arc::new(InListExpr::new(
        expr,
        list,
        *negated,
        static_filter,
    )))
}

/// Create a new static InList expression from an array of values.
///
/// The `array` should contain the list of values for the `IN` clause.
/// The `negated` flag indicates whether this is an `IN` or `NOT IN` expression.
///
/// The generated `InListExpr` will store the array directly for efficient memory usage
/// and use a static filter for O(1) lookups.
///
/// # Errors
/// Returns an error if the array type is not supported for `InList` expressions or if we cannot build a hash based lookup.
#[allow(dead_code)]
pub fn in_list_from_array(
    expr: Arc<dyn PhysicalExpr>,
    array: ArrayRef,
    negated: bool,
) -> Result<Arc<dyn PhysicalExpr>> {
    let static_filter = make_set(array.as_ref(), Ok)?;

    Ok(Arc::new(InListExpr::new_from_array(
        expr,
        array,
        negated,
        Some(static_filter),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions;
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
            let expr = in_list($COL, $LIST, $NEGATED, $SCHEMA).unwrap();
            let result = expr
                .evaluate(&$BATCH)?
                .into_array($BATCH.num_rows())
                .expect("Failed to convert to array");
            let result =
                as_boolean_array(&result).expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in ("a", "b", null)"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in ("a", "b", null)"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
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
    fn in_list_binary() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Binary, true)]);
        let a = BinaryArray::from(vec![
            Some([1, 2, 3].as_slice()),
            Some([1, 2, 2].as_slice()),
            None,
        ]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ([1, 2, 3], [4, 5, 6])"
        let list = vec![lit([1, 2, 3].as_slice()), lit([4, 5, 6].as_slice())];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in ([1, 2, 3], [4, 5, 6])"
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in ([1, 2, 3], [4, 5, 6], null)"
        let list = vec![
            lit([1, 2, 3].as_slice()),
            lit([4, 5, 6].as_slice()),
            lit(ScalarValue::Binary(None)),
        ];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in ([1, 2, 3], [4, 5, 6], null)"
        in_list!(
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
    fn in_list_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
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

    #[test]
    fn in_list_date64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Date64, true)]);
        let a = Date64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
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
    fn in_list_date32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Date32, true)]);
        let a = Date32Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
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
    fn in_list_decimal() -> Result<()> {
        // Now, we can check the NULL type
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);
        let array = vec![Some(100_0000_i128), None, Some(200_5000_i128)]
            .into_iter()
            .collect::<Decimal128Array>();
        let array = array.with_precision_and_scale(13, 4).unwrap();
        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;

        // expression: "a in (100,200), the data type of list is INT32
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(false)],
            Arc::clone(&col_a),
            &schema
        );
        // expression: "a not in (100,200)
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (200,NULL), the data type of list is INT32 AND NULL
        let list = vec![lit(ScalarValue::Int32(Some(100))), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            Arc::clone(&col_a),
            &schema
        );
        // expression: "a not in (200,NULL), the data type of list is INT32 AND NULL
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(100i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        // expression: "a not in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(101i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(true), None, Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        // test the optimization: set
        // expression: "a in (99..300), the data type of list is INT32
        let list = (99i32..300).map(lit).collect::<Vec<_>>();

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, Some(false)],
            Arc::clone(&col_a),
            &schema
        );

        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_cast_static_filter_to_set() -> Result<()> {
        // random schema
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);

        // list of phy expr
        let mut phy_exprs = vec![
            lit(1i64),
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            try_cast(lit(3.13f32), &schema, DataType::Int64)?,
        ];
        let result = try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        let array = Int64Array::from(vec![1, 2, 3, 4]);
        let r = result.contains(&array, false).unwrap();
        assert_eq!(r, BooleanArray::from(vec![true, true, true, false]));

        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();
        // cast(cast(lit())), but the cast to the same data type, one case will be ignored
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int64,
        )?);
        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        phy_exprs.clear();

        // case(cast(lit())), the cast to the diff data type
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int32,
        )?);
        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        // column
        phy_exprs.push(col("a", &schema)?);
        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_err());

        Ok(())
    }

    #[test]
    fn in_list_timestamp() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let a = TimestampMicrosecondArray::from(vec![
            Some(1388588401000000000),
            Some(1288588501000000000),
            None,
        ]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        let list = vec![
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000000),
                None,
            )),
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000001),
                None,
            )),
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000002),
                None,
            )),
        ];

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false), None],
            Arc::clone(&col_a),
            &schema
        );

        in_list!(
            batch,
            list.clone(),
            &true,
            vec![Some(false), Some(true), None],
            Arc::clone(&col_a),
            &schema
        );
        Ok(())
    }

    #[test]
    fn in_expr_with_multiple_element_in_list() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Float64, true),
        ]);
        let a = Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.0),
            Some(f64::NAN),
            Some(-f64::NAN),
        ]);
        let b = Float64Array::from(vec![
            Some(8.0),
            Some(1.0),
            Some(5.0),
            Some(f64::NAN),
            Some(3.0),
        ]);
        let c = Float64Array::from(vec![
            Some(6.0),
            Some(7.0),
            None,
            Some(5.0),
            Some(-f64::NAN),
        ]);
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )?;

        let list = vec![Arc::clone(&col_b), Arc::clone(&col_c)];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(false), Some(true), None, Some(true), Some(true)],
            Arc::clone(&col_a),
            &schema
        );

        in_list!(
            batch,
            list,
            &true,
            vec![Some(true), Some(false), None, Some(false), Some(false)],
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
    fn test_in_list_from_array_deduplication() -> Result<()> {
        // Test that in_list_from_array deduplicates values
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let col_a = col("a", &schema)?;

        // Create array with duplicates: [1, 2, 2, 3, 1, 4]
        let array = Arc::new(Int64Array::from(vec![1, 2, 2, 3, 1, 4])) as ArrayRef;

        // Create InListExpr from array
        let expr = in_list_from_array(Arc::clone(&col_a), array, false)?;

        // Note: in_list_from_array now stores the array directly (InListStorage::Array)
        // so list().len() returns 0. Deduplication happens in the static_filter.
        // We verify correctness by checking the evaluation results below.

        // Create test data
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5), None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // Evaluate the expression
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result)?;

        // Expected: 1,2,3,4 are in list, 5 is not, null stays null
        // This verifies that deduplication in the static_filter works correctly
        let expected = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
        ]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_in_list_from_array_with_nulls() -> Result<()> {
        // Test that in_list_from_array handles nulls correctly
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let col_a = col("a", &schema)?;

        // Create array with a null: [1, 2, NULL, 3]
        let array =
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef;

        // Create InListExpr from array (negated=false)
        let expr = in_list_from_array(Arc::clone(&col_a), array, false)?;

        // Create test data
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // Evaluate the expression
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result)?;

        // When the IN list contains NULL:
        // - Values in the list (1,2,3) -> true
        // - Values not in the list (4) -> NULL (because NULL might match)
        // - NULL input -> NULL
        let expected = BooleanArray::from(vec![
            Some(true), // 1 is in list
            Some(true), // 2 is in list
            Some(true), // 3 is in list
            None,       // 4 not in list, but list has NULL
            None,       // NULL input
        ]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_in_list_from_array_struct() -> Result<()> {
        // Test that in_list_from_array works with struct arrays
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Struct(struct_fields.clone()),
            true,
        )]);

        // Create array of structs with duplicates: [{1,"a"}, {2,"b"}, {1,"a"}]
        let x_array = Arc::new(Int32Array::from(vec![1, 2, 1]));
        let y_array = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let struct_array = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![x_array, y_array],
            None,
        )) as ArrayRef;

        let col_a = col("a", &schema)?;

        // Create InListExpr from struct array
        let expr = in_list_from_array(Arc::clone(&col_a), struct_array, false)?;

        // Note: in_list_from_array now stores the array directly (InListStorage::Array)
        // so list().len() returns 0. Deduplication happens in the static_filter.
        // We verify correctness by checking the evaluation results below.

        // Create test data
        let x_test = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let y_test = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let struct_test = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![x_test, y_test],
            None,
        ));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![struct_test])?;

        // Evaluate the expression
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result)?;

        // {1,"a"} and {2,"b"} are in list, {3,"c"} is not
        let expected = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
        assert_eq!(result, &expected);

        Ok(())
    }

    #[test]
    fn test_in_list_from_array_has_static_filter() -> Result<()> {
        // Test that in_list_from_array creates a static filter
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let col_a = col("a", &schema)?;

        // Create array
        let array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;

        // Create InListExpr from array
        let expr = in_list_from_array(Arc::clone(&col_a), array, false)?;

        // Verify that static filter was created by checking display string contains "(SET)"
        let display_string = expr.to_string();
        assert!(
            display_string.contains("(SET)"),
            "Expected display string to contain '(SET)' but got: {display_string}",
        );

        // Also verify for negated case
        let array2 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let expr_negated = in_list_from_array(Arc::clone(&col_a), array2, true)?;
        let display_string_negated = expr_negated.to_string();
        assert!(
            display_string_negated.contains("(SET)"),
            "Expected negated display string to contain '(SET)' but got: {display_string_negated}",
        );

        Ok(())
    }
}

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

//! Float semantics for DataFusion.
//!
//! Arrow's comparison kernels work on the bit pattern of a float,
//! i.e. IEEE 754 totalOrder. DataFusion deviates from that in one
//! respect: `+0.0` and `-0.0` are equal, hash the same and fall into the same
//! group. See [`normalize_float`], [`normalize_floats`] and
//! [`normalize_float_scalar`]. Ordering still follows totalOrder, so
//! [`compare_floats`] sorts `-0.0` before `+0.0`.
//!
//! Every comparison, hash or grouping boundary should be built from these
//! helpers rather than inspecting float bits itself. Where an Arrow kernel
//! compares, sorts, row-encodes or hashes the values, normalize them first.
//! Where DataFusion compares values itself, use [`compare_floats`],
//! [`float_lt`] or [`float_gt`]. Where it hashes them, hash [`hash_input`].
//!

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, make_array};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type,
};
use half::f16;

use crate::ScalarValue;

/// The floating point types the functions in this module are defined for:
/// [`f16`](struct@f16), [`f32`] and [`f64`].
pub trait FloatSemantics: Copy {
    /// Whether [`normalize_float`] would change this value. This is a cheap
    /// check that lets callers skip rewriting data which is already
    /// normalized.
    fn needs_normalization(self) -> bool;

    /// See [`normalize_float`].
    fn normalize(self) -> Self;

    /// See [`compare_floats`].
    fn compare(self, other: Self) -> Ordering;
}

macro_rules! impl_float_semantics {
    ($(($t:ty, $bits:ty, $zero:expr)),+ $(,)?) => {
        $(impl FloatSemantics for $t {
            #[inline]
            fn needs_normalization(self) -> bool {
                // `-0.0` has only the sign bit set; no other value shares
                // this bit pattern.
                self.to_bits() == 1 << (<$bits>::BITS - 1)
            }

            #[inline]
            fn normalize(self) -> Self {
                if self.needs_normalization() { $zero } else { self }
            }

            #[inline]
            fn compare(self, other: Self) -> Ordering {
                self.total_cmp(&other)
            }
        })+
    };
}
impl_float_semantics!((f16, u16, f16::ZERO), (f32, u32, 0.0), (f64, u64, 0.0));

/// `-0.0` becomes `0.0`, so two values that DataFusion treats as equal also
/// have the same bits. All other values, including NaN payloads, are kept as
/// they are.
#[inline]
pub fn normalize_float<T: FloatSemantics>(v: T) -> T {
    v.normalize()
}

/// Compares two values in the order DataFusion sorts them.
///
/// This is IEEE 754 totalOrder (`total_cmp`), which matches Arrow's sort
/// kernels and row encoding. Note that it orders `-0.0` before `+0.0`.
/// The grouped `MIN`/`MAX` accumulators start from the extremes of this
/// order, so they must change together with it.
#[inline]
pub fn compare_floats<T: FloatSemantics>(left: T, right: T) -> Ordering {
    left.compare(right)
}

/// Whether `left` sorts before `right`, the same as
/// `compare_floats(left, right).is_lt()`.
#[inline]
pub fn float_lt<T: FloatSemantics>(left: T, right: T) -> bool {
    compare_floats(left, right).is_lt()
}

/// Whether `left` sorts after `right`, the same as
/// `compare_floats(left, right).is_gt()`.
#[inline]
pub fn float_gt<T: FloatSemantics>(left: T, right: T) -> bool {
    compare_floats(left, right).is_gt()
}

/// The value that DataFusion hashes in place of a float. Values that compare
/// as equal hash the same, so this is the value [`normalize_float`] returns.
#[inline]
pub fn hash_input<T: FloatSemantics>(v: T) -> T {
    normalize_float(v)
}

/// Applies [`normalize_float`] to every float in `array`, including floats
/// nested inside lists, structs, maps, unions, dictionaries and run-end
/// encoded arrays. Arrays without floats are returned unchanged.
///
/// Callers use this before handing float data to Arrow kernels that compare
/// bit patterns, so that `+0.0 = -0.0` holds.
///
/// The common case - nothing to normalize - is allocation-free: a single
/// read-only scan of the values buffer (auto-vectorizable to an OR-reduction)
/// decides whether to fall through to the rewriting path. Only arrays that
/// contain values to normalize pay for a new buffer.
pub fn normalize_floats(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float16 => normalize_primitive::<Float16Type>(array),
        DataType::Float32 => normalize_primitive::<Float32Type>(array),
        DataType::Float64 => normalize_primitive::<Float64Type>(array),
        dt if has_float_leaf(dt) => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| normalize_floats(&make_array(child.clone())).to_data())
                .collect::<Vec<_>>();
            if children
                .iter()
                .zip(data.child_data())
                .all(|(new, old)| new.ptr_eq(old))
            {
                return Arc::clone(array);
            }
            make_array(
                data.into_builder()
                    .child_data(children)
                    .build()
                    .expect("rewriting float leaves preserves the array layout"),
            )
        }
        _ => Arc::clone(array),
    }
}

fn normalize_primitive<T>(array: &ArrayRef) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: FloatSemantics,
{
    let arr = array.as_primitive::<T>();
    if !arr.values().iter().any(|v| v.needs_normalization()) {
        return Arc::clone(array);
    }
    Arc::new(arr.unary::<_, T>(normalize_float))
}

/// Applies [`normalize_float`] to a `Float16`, `Float32` or `Float64` scalar.
/// Other scalars are returned as is.
pub fn normalize_float_scalar(scalar: ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::Float16(Some(v)) => ScalarValue::Float16(Some(normalize_float(v))),
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(normalize_float(v))),
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(normalize_float(v))),
        other => other,
    }
}

/// Whether `data_type` is a float or contains a float at any depth of its
/// nested children.
pub fn has_float_leaf(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _)
        | DataType::RunEndEncoded(_, f) => has_float_leaf(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| has_float_leaf(f.data_type())),
        DataType::Union(fields, _) => {
            fields.iter().any(|(_, f)| has_float_leaf(f.data_type()))
        }
        DataType::Dictionary(_, values) => has_float_leaf(values),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float16Array, Float32Array, Float64Array, Int32Array, ListArray};
    use arrow::datatypes::Int32Type;

    #[test]
    fn normalize_float_folds_negative_zero() {
        assert_eq!(normalize_float(-0.0_f64).to_bits(), 0.0_f64.to_bits());
        assert_eq!(normalize_float(-0.0_f32).to_bits(), 0.0_f32.to_bits());
        assert_eq!(
            normalize_float(f16::NEG_ZERO).to_bits(),
            f16::ZERO.to_bits()
        );
        assert_eq!(
            hash_input(-0.0_f64).to_bits(),
            hash_input(0.0_f64).to_bits()
        );
        assert!((-0.0_f64).needs_normalization());
        assert!(!0.0_f64.needs_normalization());
    }

    #[test]
    fn normalize_float_keeps_other_values() {
        let neg_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
        let payload_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        for v in [
            1.5,
            -1.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::NAN,
            neg_nan,
            payload_nan,
        ] {
            assert!(!v.needs_normalization());
            assert_eq!(normalize_float(v).to_bits(), v.to_bits());
        }
    }

    #[test]
    fn compare_floats_is_total_order() {
        let neg_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
        assert_eq!(compare_floats(1.0_f64, 2.0), Ordering::Less);
        assert_eq!(compare_floats(-0.0_f64, 0.0), Ordering::Less);
        assert_eq!(compare_floats(f64::NAN, f64::INFINITY), Ordering::Greater);
        assert_eq!(compare_floats(neg_nan, f64::NEG_INFINITY), Ordering::Less);
        assert_eq!(compare_floats(f16::ONE, f16::ZERO), Ordering::Greater);
        assert!(float_lt(1.0_f64, 2.0) && !float_lt(2.0_f64, 1.0));
        assert!(float_gt(f64::NAN, f64::INFINITY) && !float_gt(1.0_f64, 1.0));
    }

    #[test]
    fn normalize_floats_rewrites_negative_zero() {
        let array: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(1.0)]));
        let result = normalize_floats(&array);
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0).to_bits(), 0.0_f64.to_bits());
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 1.0);

        let array: ArrayRef = Arc::new(Float32Array::from(vec![-0.0_f32]));
        let result = normalize_floats(&array);
        assert_eq!(
            result.as_primitive::<Float32Type>().value(0).to_bits(),
            0.0_f32.to_bits()
        );

        let array: ArrayRef = Arc::new(Float16Array::from(vec![f16::NEG_ZERO]));
        let result = normalize_floats(&array);
        assert_eq!(
            result.as_primitive::<Float16Type>().value(0).to_bits(),
            f16::ZERO.to_bits()
        );
    }

    #[test]
    fn normalize_floats_reuses_normalized_input() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 1.0, f64::NAN]));
        assert!(Arc::ptr_eq(&normalize_floats(&array), &array));

        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert!(Arc::ptr_eq(&normalize_floats(&array), &array));
    }

    #[test]
    fn normalize_floats_rewrites_nested_floats() {
        let list = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(-0.0), Some(2.0)]),
            None,
        ]);
        let array: ArrayRef = Arc::new(list);
        let result = normalize_floats(&array);
        assert!(!Arc::ptr_eq(&result, &array));
        let values = result.as_list::<i32>().value(0);
        assert_eq!(
            values.as_primitive::<Float64Type>().value(0).to_bits(),
            0.0_f64.to_bits()
        );
        assert!(result.is_null(1));

        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]);
        let array: ArrayRef = Arc::new(list);
        assert!(Arc::ptr_eq(&normalize_floats(&array), &array));
    }

    #[test]
    fn normalize_float_scalar_folds_negative_zero() {
        assert_eq!(
            normalize_float_scalar(ScalarValue::Float64(Some(-0.0))),
            ScalarValue::Float64(Some(0.0))
        );
        assert_eq!(
            normalize_float_scalar(ScalarValue::Float32(Some(-0.0))),
            ScalarValue::Float32(Some(0.0))
        );
        assert_eq!(
            normalize_float_scalar(ScalarValue::Float16(Some(f16::NEG_ZERO))),
            ScalarValue::Float16(Some(f16::ZERO))
        );
        assert_eq!(
            normalize_float_scalar(ScalarValue::Float64(None)),
            ScalarValue::Float64(None)
        );
        assert_eq!(
            normalize_float_scalar(ScalarValue::Int32(Some(0))),
            ScalarValue::Int32(Some(0))
        );
    }
}

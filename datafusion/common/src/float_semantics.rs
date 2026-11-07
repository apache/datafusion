// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Float semantics for DataFusion.
//!
//! Arrow's comparison kernels work on the bit pattern of a float,
//! i.e. IEEE 754 totalOrder. DataFusion deviates from that in one
//! respect: `+0.0` and `-0.0` are equal, hash the same and fall into the same
//! group. See [`normalize_float`], [`normalize`] and
//! [`ScalarValue::normalize`]. Ordering still follows totalOrder, so
//! [`compare_floats`] sorts `-0.0` before `+0.0`.
//!
//! Every comparison, hash or grouping boundary should be built from these
//! helpers rather than inspecting float bits itself:
//!
//! - Equality and grouping: normalize first ([`normalize_float`],
//!   [`normalize`], [`ScalarValue::normalize`]), then compare bits or
//!   call Arrow's `eq`, `RowConverter` or `make_comparator`. Hash
//!   [`hash_input`].
//! - Ordering: use [`compare_floats`], [`float_lt`] or [`float_gt`]. Sorting
//!   does not normalize yet, so `-0.0` still sorts before `+0.0`.
//!
//! [`ScalarValue::normalize`]: crate::ScalarValue::normalize

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, make_array};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type,
};
use half::f16;

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

/// The value that DataFusion hashes in place of a float. Values that DataFusion
/// treats as equal hash the same, so this is the value [`normalize_float`]
/// returns.
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
/// Optimized for the common case of nothing to normalize, by searching for such
/// values in a vectorized way.
///
/// If the values do require normalization, attempts to rewrite the values in
/// place, if possible.
pub fn normalize(array: ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float16 => normalize_primitive::<Float16Type>(array),
        DataType::Float32 => normalize_primitive::<Float32Type>(array),
        DataType::Float64 => normalize_primitive::<Float64Type>(array),
        dt if has_float_leaf(dt) => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| normalize(make_array(child.clone())).to_data())
                .collect::<Vec<_>>();
            if children
                .iter()
                .zip(data.child_data())
                .all(|(new, old)| new.ptr_eq(old))
            {
                return array;
            }
            make_array(
                data.into_builder()
                    .child_data(children)
                    .build()
                    .expect("rewriting float leaves preserves the array layout"),
            )
        }
        _ => array,
    }
}

/// Like [`normalize`], but borrows the array and so always copies the values
/// buffer when there is something to normalize.
#[deprecated(
    since = "57.0.0",
    note = "use `datafusion_common::float_semantics::normalize` instead"
)]
pub fn normalize_floats(array: &ArrayRef) -> ArrayRef {
    normalize(Arc::clone(array))
}

/// Takes an owned array and tries to reuse its allocation when possible
fn normalize_primitive<T>(array: ArrayRef) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: FloatSemantics,
{
    let arr = array.as_primitive::<T>();
    if !arr.values().iter().any(|v| v.needs_normalization()) {
        return array;
    }
    // Drop the `ArrayRef` so the values buffer is uniquely owned (when nothing
    // else refers to it) and can be rewritten in place.
    let arr = arr.clone();
    drop(array);
    let normalized = match arr.unary_mut(normalize_float) {
        Ok(normalized) => normalized, // updated in place
        Err(arr) => arr.unary::<_, T>(normalize_float), // needed to copy
    };
    Arc::new(normalized)
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
    use crate::{Result, ScalarValue};
    use arrow::array::{
        DictionaryArray, Float16Array, Float32Array, Float64Array, Int8Array, Int32Array,
        ListArray,
    };
    use arrow::datatypes::{Field, Int8Type, Int32Type};

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
        let result = normalize(Arc::clone(&array));
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0).to_bits(), 0.0_f64.to_bits());
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 1.0);

        let array: ArrayRef = Arc::new(Float32Array::from(vec![-0.0_f32]));
        let result = normalize(Arc::clone(&array));
        assert_eq!(
            result.as_primitive::<Float32Type>().value(0).to_bits(),
            0.0_f32.to_bits()
        );

        let array: ArrayRef = Arc::new(Float16Array::from(vec![f16::NEG_ZERO]));
        let result = normalize(Arc::clone(&array));
        assert_eq!(
            result.as_primitive::<Float16Type>().value(0).to_bits(),
            f16::ZERO.to_bits()
        );
    }

    #[test]
    fn normalize_floats_reuses_normalized_input() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 1.0, f64::NAN]));
        assert!(Arc::ptr_eq(&normalize(Arc::clone(&array)), &array));

        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert!(Arc::ptr_eq(&normalize(Arc::clone(&array)), &array));
    }

    #[test]
    fn normalize_floats_rewrites_in_place_when_unique() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 1.0]));
        let values_ptr = array.as_primitive::<Float64Type>().values().as_ptr();
        let result = normalize(array);
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.values().as_ptr(), values_ptr);
        assert_eq!(result.value(0).to_bits(), 0.0_f64.to_bits());

        // A shared buffer is copied and the original is left as it is
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 1.0]));
        let result = normalize(Arc::clone(&array));
        let result = result.as_primitive::<Float64Type>();
        assert_ne!(
            result.values().as_ptr(),
            array.as_primitive::<Float64Type>().values().as_ptr()
        );
        assert_eq!(result.value(0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(
            array.as_primitive::<Float64Type>().value(0).to_bits(),
            (-0.0_f64).to_bits()
        );
    }

    #[test]
    fn normalize_floats_rewrites_nested_floats() {
        let list = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(-0.0), Some(2.0)]),
            None,
        ]);
        let array: ArrayRef = Arc::new(list);
        let result = normalize(Arc::clone(&array));
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
        assert!(Arc::ptr_eq(&normalize(Arc::clone(&array)), &array));
    }

    #[test]
    fn normalize_in_dictionary_arrays_and_scalars() -> Result<()> {
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let array: ArrayRef = Arc::new(DictionaryArray::try_new(
            keys.clone(),
            Arc::new(Float64Array::from(vec![-0.0, nan, 1.0])),
        )?);

        let normalized = normalize(Arc::clone(&array));
        let dictionary = normalized.as_dictionary::<Int8Type>();
        assert_eq!(dictionary.keys(), &keys);
        let values = dictionary.values().as_primitive::<Float64Type>();
        assert_eq!(values.value(0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(values.value(1).to_bits(), nan.to_bits());
        assert_eq!(values.value(2), 1.0);

        assert!(Arc::ptr_eq(
            &normalize(Arc::clone(&normalized)),
            &normalized
        ));

        assert_eq!(
            ScalarValue::try_from_array(&array, 0)?.normalize(),
            ScalarValue::try_from_array(&normalized, 0)?
        );

        Ok(())
    }

    #[test]
    fn normalize_preserves_union_and_run_metadata() -> Result<()> {
        use arrow::datatypes::{UnionFields, UnionMode};

        let run_ends = Arc::new(Field::new("ends", DataType::Int32, false));
        let values = Arc::new(Field::new("samples", DataType::Float64, true));
        let fields = UnionFields::try_new(
            [7, 42],
            [
                Field::new(
                    "runs",
                    DataType::RunEndEncoded(Arc::clone(&run_ends), Arc::clone(&values)),
                    true,
                ),
                Field::new("other", DataType::Int32, true),
            ],
        )?;
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        for mode in [UnionMode::Sparse, UnionMode::Dense] {
            let wrap = |value| {
                ScalarValue::Union(
                    Some((
                        7,
                        Box::new(ScalarValue::RunEndEncoded(
                            Arc::clone(&run_ends),
                            Arc::clone(&values),
                            Box::new(ScalarValue::Float64(value)),
                        )),
                    )),
                    fields.clone(),
                    mode,
                )
            };
            for (value, expected) in [
                (Some(-0.0), Some(0.0)),
                (Some(nan), Some(nan)),
                (None, None),
            ] {
                assert_eq!(wrap(value).normalize(), wrap(expected));
            }
            let null = ScalarValue::Union(None, fields.clone(), mode);
            assert_eq!(null.clone().normalize(), null);
        }
        Ok(())
    }

    #[test]
    fn normalize_preserves_sliced_list() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let list = Arc::new(
            ListArray::from_iter_primitive::<Float64Type, _, _>([
                Some(vec![Some(99.0)]),
                Some(vec![Some(-0.0), Some(nan), None]),
                Some(vec![Some(42.0)]),
            ])
            .slice(1, 1),
        );
        let ScalarValue::List(normalized) = ScalarValue::List(list).normalize() else {
            panic!("normalization must preserve the scalar variant");
        };
        assert_eq!(normalized.len(), 1);
        let child = normalized.value(0);
        let child = child.as_primitive::<Float64Type>();
        assert_eq!(child.len(), 3);
        assert_eq!(child.value(0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(child.value(1).to_bits(), nan.to_bits());
        assert!(child.is_null(2));
    }
}

use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrowPrimitiveType;
use arrow::array::cast::AsArray;
use arrow::array::types::*;
use arrow::compute::SortOptions;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::{array::Array, datatypes::DataType};

use crate::DataFusionError;
use crate::Result;
use crate::ScalarValue;

/// A comparator that compares values at arbitrary indices in two arrays.
///
/// The comparator takes two arrays and two indices, and returns the ordering
/// of the values at those indices. Optional delta values can be added to
/// each side before comparison.
#[derive(Clone)]
pub struct ArrayComparator(
    Arc<dyn Fn(&dyn Array, usize, &dyn Array, usize) -> Ordering + Send + Sync>,
);

impl Debug for ArrayComparator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayComparator").finish_non_exhaustive()
    }
}

impl std::ops::Deref for ArrayComparator {
    type Target = dyn Fn(&dyn Array, usize, &dyn Array, usize) -> Ordering + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Extracted delta with native value and operation.
struct NativeDelta<T> {
    value: T,
    op: Op,
}

/// Extracts a native primitive value and operation from an optional DeltaValue.
///
/// Returns `Ok(None)` if delta is None or contains a null value.
/// Returns an error if the ScalarValue type doesn't match the expected primitive type.
fn extract_native_delta<T: ArrowPrimitiveType>(
    delta: &Option<DeltaValue>,
) -> Result<Option<NativeDelta<T::Native>>> {
    match delta {
        None => Ok(None),
        Some(dv) => {
            let arr = dv.value.to_array()?;
            let prim = arr.as_primitive::<T>();
            if prim.is_null(0) {
                Ok(None)
            } else {
                Ok(Some(NativeDelta {
                    value: prim.value(0),
                    op: dv.op.clone(),
                }))
            }
        }
    }
}

/// Applies the delta operation to a value.
#[inline]
fn apply_delta<T: ArrowNativeTypeOp>(val: T, delta: &NativeDelta<T>) -> T {
    match delta.op {
        Op::AddWrapping => val.add_wrapping(delta.value),
        Op::SubWrapping => val.sub_wrapping(delta.value),
    }
}

/// Creates a comparator for primitive types with optional delta values.
///
/// The comparator applies delta operations to the array values before comparison.
/// Null handling follows the `SortOptions` configuration.
fn compare_primitive<T>(
    left_delta: Option<DeltaValue>,
    right_delta: Option<DeltaValue>,
    opts: SortOptions,
) -> Result<ArrayComparator>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp,
{
    let left_native_delta = extract_native_delta::<T>(&left_delta)?;
    let right_native_delta = extract_native_delta::<T>(&right_delta)?;

    let cmp = match (left_native_delta, right_native_delta) {
        (None, None) => compare(opts, |l, i, r, j| {
            let left_arr: &arrow::array::PrimitiveArray<T> = l.as_primitive::<T>();
            let right_arr = r.as_primitive::<T>();
            let left_val = left_arr.values()[i];
            let right_val = right_arr.values()[j];
            left_val.compare(right_val)
        }),
        (None, Some(right_d)) => compare(opts, move |l, i, r, j| {
            let left_arr = l.as_primitive::<T>();
            let right_arr = r.as_primitive::<T>();
            let left_val = left_arr.values()[i];
            let right_val = apply_delta(right_arr.values()[j], &right_d);
            left_val.compare(right_val)
        }),
        (Some(left_d), None) => compare(opts, move |l, i, r, j| {
            let left_arr = l.as_primitive::<T>();
            let right_arr = r.as_primitive::<T>();
            let left_val = apply_delta(left_arr.values()[i], &left_d);
            let right_val = right_arr.values()[j];
            left_val.compare(right_val)
        }),
        (Some(left_d), Some(right_d)) => compare(opts, move |l, i, r, j| {
            let left_arr = l.as_primitive::<T>();
            let right_arr = r.as_primitive::<T>();
            let left_val = apply_delta(left_arr.values()[i], &left_d);
            let right_val = apply_delta(right_arr.values()[j], &right_d);
            left_val.compare(right_val)
        }),
    };

    Ok(cmp)
}

fn compare<F>(opts: SortOptions, cmp: F) -> ArrayComparator
where
    F: Fn(&dyn Array, usize, &dyn Array, usize) -> Ordering + Send + Sync + 'static,
{
    match (opts.nulls_first, opts.descending) {
        (true, true) => compare_impl::<true, true, _>(cmp),
        (true, false) => compare_impl::<true, false, _>(cmp),
        (false, true) => compare_impl::<false, true, _>(cmp),
        (false, false) => compare_impl::<false, false, _>(cmp),
    }
}

fn compare_impl<const NULLS_FIRST: bool, const DESCENDING: bool, F>(
    cmp: F,
) -> ArrayComparator
where
    F: Fn(&dyn Array, usize, &dyn Array, usize) -> Ordering + Send + Sync + 'static,
{
    let (left_null, right_null) = match NULLS_FIRST {
        true => (Ordering::Less, Ordering::Greater),
        false => (Ordering::Greater, Ordering::Less),
    };

    ArrayComparator(Arc::new(move |left, i, right, j| {
        match (left.is_null(i), right.is_null(j)) {
            (true, true) => Ordering::Equal,
            (true, false) => left_null,
            (false, true) => right_null,
            (false, false) => match DESCENDING {
                true => cmp(left, i, right, j).reverse(),
                false => cmp(left, i, right, j),
            },
        }
    }))
}

#[derive(Clone)]
pub enum Op {
    AddWrapping,
    SubWrapping,
}

#[derive(Clone)]
pub struct DeltaValue {
    pub value: ScalarValue,
    pub op: Op,
}

/// Creates a comparator for comparing array values at arbitrary indices.
///
/// This function creates a comparator closure that can compare values from two arrays
/// at specified indices. Optional delta values can be applied to each side before comparison.
///
/// # Arguments
///
/// * `data_type` - The data type of both arrays being compared
/// * `left_delta` - Optional delta value to apply to left array values before comparison
/// * `right_delta` - Optional delta value to apply to right array values before comparison
/// * `opts` - Sort options controlling null ordering and ascending/descending order
///
/// # Returns
///
/// An `ArrayComparator` that takes two arrays and two indices and returns an `Ordering`.
///
/// # Example
///
/// ```
/// use std::cmp::Ordering;
/// use arrow::array::Int32Array;
/// use arrow::compute::SortOptions;
/// use arrow::datatypes::DataType;
/// use datafusion_common::ScalarValue;
/// use datafusion_common::ord::{make_comparator, DeltaValue, Op};
///
/// // Create a comparator with a delta of +1 on the left side
/// let cmp = make_comparator(
///     DataType::Int32,
///     Some(DeltaValue { value: ScalarValue::Int32(Some(1)), op: Op::AddWrapping }),
///     None,
///     SortOptions::default(),
/// ).unwrap();
///
/// let array = Int32Array::from(vec![1, 2, 3]);
/// // Compares (1 + 1) vs 2, which is Equal
/// assert_eq!(cmp(&array, 0, &array, 1), Ordering::Equal);
/// ```
pub fn make_comparator(
    data_type: DataType,
    left_delta: Option<DeltaValue>,
    right_delta: Option<DeltaValue>,
    opts: SortOptions,
) -> Result<ArrayComparator> {
    use DataType::*;

    // Validate delta types match array type
    if let Some(ref delta) = left_delta {
        let delta_type = delta.value.data_type();
        if delta_type != data_type {
            return Err(DataFusionError::Internal(format!(
                "Left delta type {:?} does not match array type {:?}",
                delta_type, data_type
            )));
        }
    }
    if let Some(ref delta) = right_delta {
        let delta_type = delta.value.data_type();
        if delta_type != data_type {
            return Err(DataFusionError::Internal(format!(
                "Right delta type {:?} does not match array type {:?}",
                delta_type, data_type
            )));
        }
    }

    match data_type {
        Int8 => compare_primitive::<Int8Type>(left_delta, right_delta, opts),
        Int16 => compare_primitive::<Int16Type>(left_delta, right_delta, opts),
        Int32 => compare_primitive::<Int32Type>(left_delta, right_delta, opts),
        Int64 => compare_primitive::<Int64Type>(left_delta, right_delta, opts),
        UInt8 => compare_primitive::<UInt8Type>(left_delta, right_delta, opts),
        UInt16 => compare_primitive::<UInt16Type>(left_delta, right_delta, opts),
        UInt32 => compare_primitive::<UInt32Type>(left_delta, right_delta, opts),
        UInt64 => compare_primitive::<UInt64Type>(left_delta, right_delta, opts),
        Float16 => compare_primitive::<Float16Type>(left_delta, right_delta, opts),
        Float32 => compare_primitive::<Float32Type>(left_delta, right_delta, opts),
        Float64 => compare_primitive::<Float64Type>(left_delta, right_delta, opts),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Comparison for {:?} is not implemented",
            data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;

    use super::*;

    #[test]
    fn test_compare_single_array_no_delta() {
        let cmp =
            make_comparator(DataType::Int32, None, None, SortOptions::default()).unwrap();
        let array = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);

        assert_eq!(Ordering::Equal, cmp(&array, 0, &array, 0));
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 1));
        assert_eq!(Ordering::Equal, cmp(&array, 2, &array, 2));
        assert_eq!(Ordering::Equal, cmp(&array, 3, &array, 3));
        assert_eq!(Ordering::Equal, cmp(&array, 4, &array, 4));
        assert_eq!(Ordering::Less, cmp(&array, 0, &array, 1));
    }

    #[test]
    fn test_compare_single_array_with_add_delta() {
        let cmp = make_comparator(
            DataType::Int32,
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(1)),
                op: Op::AddWrapping,
            }),
            None,
            SortOptions::default(),
        )
        .unwrap();

        let array = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);

        // (1+1=2) vs 1 → Greater
        assert_eq!(Ordering::Greater, cmp(&array, 0, &array, 0));
        // (2+1=3) vs 2 → Greater
        assert_eq!(Ordering::Greater, cmp(&array, 1, &array, 1));
        // (1+1=2) vs 2 → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 0, &array, 1));
    }

    #[test]
    fn test_compare_single_array_with_sub_delta() {
        let cmp = make_comparator(
            DataType::Int32,
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(1)),
                op: Op::SubWrapping,
            }),
            None,
            SortOptions::default(),
        )
        .unwrap();

        let array = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);

        // (1-1=0) vs 1 → Less
        assert_eq!(Ordering::Less, cmp(&array, 0, &array, 0));
        // (2-1=1) vs 2 → Less
        assert_eq!(Ordering::Less, cmp(&array, 1, &array, 1));
        // (2-1=1) vs 1 → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 0));
    }

    #[test]
    fn test_compare_two_arrays_no_delta() {
        let cmp =
            make_comparator(DataType::Int32, None, None, SortOptions::default()).unwrap();
        let left = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);
        let right = Int32Array::new(vec![1, 2, 10, 4, 5].into(), None);

        assert_eq!(Ordering::Equal, cmp(&left, 0, &right, 0));
        assert_eq!(Ordering::Equal, cmp(&left, 1, &right, 1));
        assert_eq!(Ordering::Less, cmp(&left, 2, &right, 2));
        assert_eq!(Ordering::Equal, cmp(&left, 3, &right, 3));
        assert_eq!(Ordering::Equal, cmp(&left, 4, &right, 4));
    }

    #[test]
    fn test_compare_two_arrays_with_add_deltas() {
        // left_delta = +1, right_delta = +2
        // Compares (left[i] + 1) vs (right[j] + 2)
        let cmp = make_comparator(
            DataType::Int32,
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(1)),
                op: Op::AddWrapping,
            }),
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(2)),
                op: Op::AddWrapping,
            }),
            SortOptions::default(),
        )
        .unwrap();
        let left = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);
        let right = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);

        // (1+1=2) vs (1+2=3) → Less
        assert_eq!(Ordering::Less, cmp(&left, 0, &right, 0));
        // (2+1=3) vs (2+2=4) → Less
        assert_eq!(Ordering::Less, cmp(&left, 1, &right, 1));
        // (2+1=3) vs (1+2=3) → Equal
        assert_eq!(Ordering::Equal, cmp(&left, 1, &right, 0));
        // (3+1=4) vs (1+2=3) → Greater
        assert_eq!(Ordering::Greater, cmp(&left, 2, &right, 0));
    }

    #[test]
    fn test_compare_with_mixed_ops() {
        // left_delta = +2, right_delta = -1
        // Compares (left[i] + 2) vs (right[j] - 1)
        let cmp = make_comparator(
            DataType::Int32,
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(2)),
                op: Op::AddWrapping,
            }),
            Some(DeltaValue {
                value: ScalarValue::Int32(Some(1)),
                op: Op::SubWrapping,
            }),
            SortOptions::default(),
        )
        .unwrap();
        let array = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);

        // (1+2=3) vs (1-1=0) → Greater
        assert_eq!(Ordering::Greater, cmp(&array, 0, &array, 0));
        // (1+2=3) vs (4-1=3) → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 0, &array, 3));
        // (2+2=4) vs (5-1=4) → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 4));
    }

    #[test]
    fn test_compare_with_nulls() {
        use arrow::buffer::NullBuffer;

        let cmp = make_comparator(
            DataType::Int32,
            None,
            None,
            SortOptions::default(), // nulls_first = true by default
        )
        .unwrap();

        // Array with nulls: [1, null, 3]
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = Int32Array::new(vec![1, 0, 3].into(), Some(nulls));

        // null vs null → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 1));
        // null vs value → Less (nulls first)
        assert_eq!(Ordering::Less, cmp(&array, 1, &array, 0));
        // value vs null → Greater
        assert_eq!(Ordering::Greater, cmp(&array, 0, &array, 1));
        // value vs value
        assert_eq!(Ordering::Less, cmp(&array, 0, &array, 2));
    }

    #[test]
    fn test_compare_nulls_last() {
        use arrow::buffer::NullBuffer;

        let cmp = make_comparator(
            DataType::Int32,
            None,
            None,
            SortOptions {
                descending: false,
                nulls_first: false, // nulls last
            },
        )
        .unwrap();

        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = Int32Array::new(vec![1, 0, 3].into(), Some(nulls));

        // null vs null → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 1));
        // null vs value → Greater (nulls last)
        assert_eq!(Ordering::Greater, cmp(&array, 1, &array, 0));
        // value vs null → Less
        assert_eq!(Ordering::Less, cmp(&array, 0, &array, 1));
    }

    #[test]
    fn test_compare_descending() {
        let cmp = make_comparator(
            DataType::Int32,
            None,
            None,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )
        .unwrap();

        let array = Int32Array::new(vec![1, 2, 3].into(), None);

        // 1 vs 2 → Greater (reversed from Less)
        assert_eq!(Ordering::Greater, cmp(&array, 0, &array, 1));
        // 3 vs 1 → Less (reversed from Greater)
        assert_eq!(Ordering::Less, cmp(&array, 2, &array, 0));
        // 2 vs 2 → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 1));
    }

    #[test]
    fn test_compare_float64() {
        use arrow::array::Float64Array;

        let cmp = make_comparator(DataType::Float64, None, None, SortOptions::default())
            .unwrap();

        let array = Float64Array::new(vec![1.0, 2.5, 3.0].into(), None);

        assert_eq!(Ordering::Less, cmp(&array, 0, &array, 1));
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 1));
        assert_eq!(Ordering::Greater, cmp(&array, 2, &array, 0));
    }

    #[test]
    fn test_compare_float64_with_delta() {
        use arrow::array::Float64Array;

        let cmp = make_comparator(
            DataType::Float64,
            Some(DeltaValue {
                value: ScalarValue::Float64(Some(0.5)),
                op: Op::AddWrapping,
            }),
            None,
            SortOptions::default(),
        )
        .unwrap();

        let array = Float64Array::new(vec![1.0, 1.5, 2.0].into(), None);

        // (1.0 + 0.5 = 1.5) vs 1.5 → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 0, &array, 1));
        // (1.5 + 0.5 = 2.0) vs 2.0 → Equal
        assert_eq!(Ordering::Equal, cmp(&array, 1, &array, 2));
    }

    #[test]
    fn test_delta_type_mismatch_error() {
        let result = make_comparator(
            DataType::Int32,
            Some(DeltaValue {
                value: ScalarValue::Int64(Some(1)),
                op: Op::AddWrapping,
            }),
            None,
            SortOptions::default(),
        );

        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("Left delta type"));
    }

    #[test]
    fn test_unsupported_type_error() {
        let result = make_comparator(DataType::Utf8, None, None, SortOptions::default());

        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("not implemented"));
    }
}

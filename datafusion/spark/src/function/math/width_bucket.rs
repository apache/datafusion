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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, Float64Array, IntervalMonthDayNanoArray,
    IntervalYearMonthArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Duration, Float64, Int32, Interval};
use arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion_common::cast::{
    as_duration_microsecond_array, as_float64_array, as_int32_array,
    as_interval_mdn_array, as_interval_ym_array,
};
use datafusion_common::types::{
    logical_duration_microsecond, logical_float64, logical_int32, logical_interval_mdn,
    logical_interval_year_month, NativeType,
};
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass,
};
use datafusion_functions::utils::make_scalar_function;

use arrow::array::{Int32Array, Int32Builder};
use arrow::datatypes::TimeUnit::Microsecond;
use datafusion_expr::Coercion;
use datafusion_expr::Volatility::Immutable;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkWidthBucket {
    signature: Signature,
}

impl Default for SparkWidthBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkWidthBucket {
    pub fn new() -> Self {
        let numeric = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        let duration = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_duration_microsecond()),
            vec![TypeSignatureClass::Duration],
            NativeType::Duration(Microsecond),
        );
        let interval_ym = Coercion::new_exact(TypeSignatureClass::Native(
            logical_interval_year_month(),
        ));
        let interval_mdn =
            Coercion::new_exact(TypeSignatureClass::Native(logical_interval_mdn()));
        let bucket = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        let type_signature = Signature::one_of(
            vec![
                TypeSignature::Coercible(vec![
                    numeric.clone(),
                    numeric.clone(),
                    numeric.clone(),
                    bucket.clone(),
                ]),
                TypeSignature::Coercible(vec![
                    duration.clone(),
                    duration.clone(),
                    duration.clone(),
                    bucket.clone(),
                ]),
                TypeSignature::Coercible(vec![
                    interval_ym.clone(),
                    interval_ym.clone(),
                    interval_ym.clone(),
                    bucket.clone(),
                ]),
                TypeSignature::Coercible(vec![
                    interval_mdn.clone(),
                    interval_mdn.clone(),
                    interval_mdn.clone(),
                    bucket.clone(),
                ]),
            ],
            Immutable,
        )
        .with_parameter_names(vec!["expr", "min", "max", "num_buckets"])
        .expect("valid parameter names");
        Self {
            signature: type_signature,
        }
    }
}

impl ScalarUDFImpl for SparkWidthBucket {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "width_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(width_bucket_kern, vec![])(&args.args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        if input.len() == 1 {
            let value = &input[0];
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::default())
        }
    }
}

fn width_bucket_kern(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [v, minv, maxv, nb] = args else {
        return exec_err!(
            "width_bucket expects exactly 4 argument, got {}",
            args.len()
        );
    };

    match v.data_type() {
        Float64 => {
            let v = as_float64_array(v)?;
            let min = as_float64_array(minv)?;
            let max = as_float64_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_float64(v, min, max, n_bucket)))
        }
        Duration(Microsecond) => {
            let v = as_duration_microsecond_array(v)?;
            let min = as_duration_microsecond_array(minv)?;
            let max = as_duration_microsecond_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i64_as_float(v, min, max, n_bucket)))
        }
        Interval(YearMonth) => {
            let v = as_interval_ym_array(v)?;
            let min = as_interval_ym_array(minv)?;
            let max = as_interval_ym_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_i32_as_float(v, min, max, n_bucket)))
        }
        Interval(MonthDayNano) => {
            let v = as_interval_mdn_array(v)?;
            let min = as_interval_mdn_array(minv)?;
            let max = as_interval_mdn_array(maxv)?;
            let n_bucket = as_int32_array(nb)?;
            Ok(Arc::new(width_bucket_interval_mdn_exact(
                v, min, max, n_bucket,
            )))
        }

        other => internal_err!(
            "width_bucket received unexpected data types: {:?}, {:?}, {:?}, {:?}",
            other,
            minv.data_type(),
            maxv.data_type(),
            nb.data_type()
        ),
    }
}

macro_rules! width_bucket_kernel_impl {
    ($name:ident, $arr_ty:ty, $to_f64:expr, $check_nan:expr) => {
        pub(crate) fn $name(
            v: &$arr_ty,
            min: &$arr_ty,
            max: &$arr_ty,
            n_bucket: &Int32Array,
        ) -> Int32Array {
            let len = v.len();
            let mut b = Int32Builder::with_capacity(len);

            for i in 0..len {
                if v.is_null(i) || min.is_null(i) || max.is_null(i) || n_bucket.is_null(i)
                {
                    b.append_null();
                    continue;
                }
                let x = ($to_f64)(v, i);
                let l = ($to_f64)(min, i);
                let h = ($to_f64)(max, i);
                let buckets = n_bucket.value(i);

                if buckets <= 0 {
                    b.append_null();
                    continue;
                }
                if $check_nan {
                    if !x.is_finite() || !l.is_finite() || !h.is_finite() {
                        b.append_null();
                        continue;
                    }
                }

                let ord = match l.partial_cmp(&h) {
                    Some(o) => o,
                    None => {
                        b.append_null();
                        continue;
                    }
                };
                if matches!(ord, std::cmp::Ordering::Equal) {
                    b.append_null();
                    continue;
                }
                let asc = matches!(ord, std::cmp::Ordering::Less);

                if asc {
                    if x < l {
                        b.append_value(0);
                        continue;
                    }
                    if x >= h {
                        b.append_value(buckets + 1);
                        continue;
                    }
                } else {
                    if x > l {
                        b.append_value(0);
                        continue;
                    }
                    if x <= h {
                        b.append_value(buckets + 1);
                        continue;
                    }
                }

                let width = (h - l) / (buckets as f64);
                if width == 0.0 || !width.is_finite() {
                    b.append_null();
                    continue;
                }
                let mut bucket = ((x - l) / width).floor() as i32 + 1;
                if bucket < 1 {
                    bucket = 1;
                }
                if bucket > buckets + 1 {
                    bucket = buckets + 1;
                }

                b.append_value(bucket);
            }

            b.finish()
        }
    };
}

width_bucket_kernel_impl!(
    width_bucket_float64,
    Float64Array,
    |arr: &Float64Array, i: usize| arr.value(i),
    true
);

width_bucket_kernel_impl!(
    width_bucket_i64_as_float,
    DurationMicrosecondArray,
    |arr: &DurationMicrosecondArray, i: usize| arr.value(i) as f64,
    false
);

width_bucket_kernel_impl!(
    width_bucket_i32_as_float,
    IntervalYearMonthArray,
    |arr: &IntervalYearMonthArray, i: usize| arr.value(i) as f64,
    false
);
const NS_PER_DAY_I128: i128 = 86_400_000_000_000;
pub(crate) fn width_bucket_interval_mdn_exact(
    v: &IntervalMonthDayNanoArray,
    lo: &IntervalMonthDayNanoArray,
    hi: &IntervalMonthDayNanoArray,
    n: &Int32Array,
) -> Int32Array {
    let len = v.len();
    let mut b = Int32Builder::with_capacity(len);

    for i in 0..len {
        if v.is_null(i) || lo.is_null(i) || hi.is_null(i) || n.is_null(i) {
            b.append_null();
            continue;
        }
        let buckets = n.value(i);
        if buckets <= 0 {
            b.append_null();
            continue;
        }

        let x = v.value(i);
        let l = lo.value(i);
        let h = hi.value(i);

        // asc/desc
        // Values of IntervalMonthDayNano are compared using their binary representation, which can lead to surprising results.
        let asc = (l.months, l.days, l.nanoseconds) < (h.months, h.days, h.nanoseconds);
        if (l.months, l.days, l.nanoseconds) == (h.months, h.days, h.nanoseconds) {
            b.append_null();
            continue;
        }

        // ------------------- only month -------------------
        if l.days == h.days && l.nanoseconds == h.nanoseconds && l.months != h.months {
            let x_m = x.months as f64;
            let l_m = l.months as f64;
            let h_m = h.months as f64;

            if asc {
                if x_m < l_m {
                    b.append_value(0);
                    continue;
                }
                if x_m >= h_m {
                    b.append_value(buckets + 1);
                    continue;
                }
            } else {
                if x_m > l_m {
                    b.append_value(0);
                    continue;
                }
                if x_m <= h_m {
                    b.append_value(buckets + 1);
                    continue;
                }
            }

            let width = (h_m - l_m) / (buckets as f64);
            if width == 0.0 || !width.is_finite() {
                b.append_null();
                continue;
            }

            let mut bucket = ((x_m - l_m) / width).floor() as i32 + 1;
            if bucket < 1 {
                bucket = 1;
            }
            if bucket > buckets + 1 {
                bucket = buckets + 1;
            }
            b.append_value(bucket);
            continue;
        }

        // ---------------  months equals -------------------
        if l.months == h.months {
            let base_days = l.days as i128;
            let base_ns = l.nanoseconds as i128;

            let xf = (x.days as i128 - base_days) * NS_PER_DAY_I128
                + (x.nanoseconds as i128 - base_ns);
            let hf = (h.days as i128 - base_days) * NS_PER_DAY_I128
                + (h.nanoseconds as i128 - base_ns);

            let x_f = xf as f64;
            let l_f = 0.0;
            let h_f = hf as f64;

            if asc {
                if x_f < l_f {
                    b.append_value(0);
                    continue;
                }
                if x_f >= h_f {
                    b.append_value(buckets + 1);
                    continue;
                }
            } else {
                if x_f > l_f {
                    b.append_value(0);
                    continue;
                }
                if x_f <= h_f {
                    b.append_value(buckets + 1);
                    continue;
                }
            }

            let width = (h_f - l_f) / (buckets as f64);
            if width == 0.0 || !width.is_finite() {
                b.append_null();
                continue;
            }

            let mut bucket = ((x_f - l_f) / width).floor() as i32 + 1;
            if bucket < 1 {
                bucket = 1;
            }
            if bucket > buckets + 1 {
                bucket = buckets + 1;
            }
            b.append_value(bucket);
            continue;
        }

        b.append_null();
    }

    b.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DurationMicrosecondArray, Float64Array, Int32Array,
        IntervalYearMonthArray,
    };
    use arrow::datatypes::IntervalMonthDayNano;

    // --- Helpers -------------------------------------------------------------

    fn i32_array_all(len: usize, val: i32) -> Arc<Int32Array> {
        Arc::new(Int32Array::from(vec![val; len]))
    }

    fn f64_array(vals: &[f64]) -> Arc<Float64Array> {
        Arc::new(Float64Array::from(vals.to_vec()))
    }

    fn f64_array_opt(vals: &[Option<f64>]) -> Arc<Float64Array> {
        Arc::new(Float64Array::from(vals.to_vec()))
    }

    fn dur_us_array(vals: &[i64]) -> Arc<DurationMicrosecondArray> {
        Arc::new(DurationMicrosecondArray::from(vals.to_vec()))
    }

    fn ym_array(vals: &[i32]) -> Arc<IntervalYearMonthArray> {
        Arc::new(IntervalYearMonthArray::from(vals.to_vec()))
    }

    fn downcast_i32(arr: &ArrayRef) -> &Int32Array {
        arr.as_any().downcast_ref::<Int32Array>().unwrap()
    }

    fn mdn_array(vals: &[(i32, i32, i64)]) -> Arc<IntervalMonthDayNanoArray> {
        let data: Vec<IntervalMonthDayNano> = vals
            .iter()
            .map(|(m, d, ns)| IntervalMonthDayNano::new(*m, *d, *ns))
            .collect();
        Arc::new(IntervalMonthDayNanoArray::from(data))
    }

    // --- Float64 -------------------------------------------------------------

    #[test]
    fn test_width_bucket_f64_basic() {
        let v = f64_array(&[0.5, 1.0, 9.9, -1.0, 10.0]);
        let lo = f64_array(&[0.0, 0.0, 0.0, 0.0, 0.0]);
        let hi = f64_array(&[10.0, 10.0, 10.0, 10.0, 10.0]);
        let n = i32_array_all(5, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 2, 10, 0, 11]);
    }

    #[test]
    fn test_width_bucket_f64_descending_range() {
        let v = f64_array(&[9.9, 10.0, 0.0, -0.1, 10.1]);
        let lo = f64_array(&[10.0; 5]);
        let hi = f64_array(&[0.0; 5]);
        let n = i32_array_all(5, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);

        assert_eq!(out.values(), &[1, 1, 11, 11, 0]);
    }
    #[test]
    fn test_width_bucket_f64_bounds_inclusive_exclusive_asc() {
        let v = f64_array(&[0.0, 9.999999999, 10.0]);
        let lo = f64_array(&[0.0; 3]);
        let hi = f64_array(&[10.0; 3]);
        let n = i32_array_all(3, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 10, 11]);
    }

    #[test]
    fn test_width_bucket_f64_bounds_inclusive_exclusive_desc() {
        let v = f64_array(&[10.0, 0.0, -0.000001]);
        let lo = f64_array(&[10.0; 3]);
        let hi = f64_array(&[0.0; 3]);
        let n = i32_array_all(3, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 11, 11]);
    }

    #[test]
    fn test_width_bucket_f64_edge_cases() {
        let v = f64_array(&[1.0, 5.0, 9.0]);
        let lo = f64_array(&[0.0, 0.0, 0.0]);
        let hi = f64_array(&[10.0, 10.0, 10.0]);
        let n = Arc::new(Int32Array::from(vec![0, -1, 10]));
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 10);

        let v = f64_array(&[1.0]);
        let lo = f64_array(&[5.0]);
        let hi = f64_array(&[5.0]);
        let n = i32_array_all(1, 10);
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));

        let v = f64_array_opt(&[Some(f64::NAN)]);
        let lo = f64_array(&[0.0]);
        let hi = f64_array(&[10.0]);
        let n = i32_array_all(1, 10);
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
    }

    #[test]
    fn test_width_bucket_f64_nulls_propagate() {
        let v = f64_array_opt(&[None, Some(1.0), Some(2.0), Some(3.0)]);
        let lo = f64_array(&[0.0; 4]);
        let hi = f64_array(&[10.0; 4]);
        let n = i32_array_all(4, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
        assert_eq!(out.value(1), 2);
        assert_eq!(out.value(2), 3);
        assert_eq!(out.value(3), 4);

        let v = f64_array(&[1.0]);
        let lo = f64_array_opt(&[None]);
        let hi = f64_array(&[10.0]);
        let n = i32_array_all(1, 10);
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
    }

    // --- Duration(Microsecond) ----------------------------------------------

    #[test]
    fn test_width_bucket_duration_us() {
        let v = dur_us_array(&[1_000_000, 0, -1]);
        let lo = dur_us_array(&[0, 0, 0]);
        let hi = dur_us_array(&[2_000_000, 2_000_000, 2_000_000]);
        let n = i32_array_all(3, 2);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[2, 1, 0]);
    }

    #[test]
    fn test_width_bucket_duration_us_equal_bounds() {
        let v = dur_us_array(&[0]);
        let lo = dur_us_array(&[1]);
        let hi = dur_us_array(&[1]);
        let n = i32_array_all(1, 10);
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        assert!(downcast_i32(&out).is_null(0));
    }

    // --- Interval(YearMonth) ------------------------------------------------

    #[test]
    fn test_width_bucket_interval_ym_basic() {
        let v = ym_array(&[0, 5, 11, 12, 13]);
        let lo = ym_array(&[0; 5]);
        let hi = ym_array(&[12; 5]);
        let n = i32_array_all(5, 12);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 6, 12, 13, 13]);
    }

    #[test]
    fn test_width_bucket_interval_ym_desc() {
        let v = ym_array(&[11, 12, 0, -1, 13]);
        let lo = ym_array(&[12; 5]);
        let hi = ym_array(&[0; 5]);
        let n = i32_array_all(5, 12);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[2, 1, 13, 13, 0]);
    }

    // --- Interval(MonthDayNano) --------------------------------------------

    #[test]
    fn test_width_bucket_interval_mdn_months_only_basic() {
        let v = mdn_array(&[(0, 0, 0), (5, 0, 0), (11, 0, 0), (12, 0, 0), (13, 0, 0)]);
        let lo = mdn_array(&[(0, 0, 0); 5]);
        let hi = mdn_array(&[(12, 0, 0); 5]);
        let n = i32_array_all(5, 12);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 6, 12, 13, 13]);
    }

    #[test]
    fn test_width_bucket_interval_mdn_months_only_desc() {
        let v = mdn_array(&[(11, 0, 0), (12, 0, 0), (0, 0, 0), (-1, 0, 0), (13, 0, 0)]);
        let lo = mdn_array(&[(12, 0, 0); 5]);
        let hi = mdn_array(&[(0, 0, 0); 5]);
        let n = i32_array_all(5, 12);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        // Mismo patrón que YM descendente
        assert_eq!(out.values(), &[2, 1, 13, 13, 0]);
    }

    #[test]
    fn test_width_bucket_interval_mdn_day_nano_basic() {
        let v = mdn_array(&[
            (0, 0, 0),
            (0, 5, 0),
            (0, 9, 0),
            (0, 10, 0),
            (0, -1, 0),
            (0, 11, 0),
        ]);
        let lo = mdn_array(&[(0, 0, 0); 6]);
        let hi = mdn_array(&[(0, 10, 0); 6]);
        let n = i32_array_all(6, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        // x==hi -> n+1, x<lo -> 0, x>hi -> n+1
        assert_eq!(out.values(), &[1, 6, 10, 11, 0, 11]);
    }

    #[test]
    fn test_width_bucket_interval_mdn_day_nano_desc() {
        let v = mdn_array(&[(0, 9, 0), (0, 10, 0), (0, 0, 0), (0, -1, 0), (0, 11, 0)]);
        let lo = mdn_array(&[(0, 10, 0); 5]);
        let hi = mdn_array(&[(0, 0, 0); 5]);
        let n = i32_array_all(5, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);

        assert_eq!(out.values(), &[2, 1, 11, 11, 0]);
    }
    #[test]
    fn test_width_bucket_interval_mdn_day_nano_desc_inside() {
        let v = mdn_array(&[(0, 9, 1), (0, 10, 0), (0, 0, 0), (0, -1, 0), (0, 11, 0)]);
        let lo = mdn_array(&[(0, 10, 0); 5]);
        let hi = mdn_array(&[(0, 0, 0); 5]);
        let n = i32_array_all(5, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);

        assert_eq!(out.values(), &[1, 1, 11, 11, 0]);
    }

    #[test]
    fn test_width_bucket_interval_mdn_mixed_months_and_days_is_null() {
        let v = mdn_array(&[(0, 1, 0)]);
        let lo = mdn_array(&[(0, 0, 0)]);
        let hi = mdn_array(&[(1, 1, 0)]);
        let n = i32_array_all(1, 4);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
    }

    #[test]
    fn test_width_bucket_interval_mdn_equal_bounds_is_null() {
        let v = mdn_array(&[(0, 0, 0)]);
        let lo = mdn_array(&[(1, 2, 3)]);
        let hi = mdn_array(&[(1, 2, 3)]); // lo == hi
        let n = i32_array_all(1, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        assert!(downcast_i32(&out).is_null(0));
    }

    #[test]
    fn test_width_bucket_interval_mdn_invalid_n_is_null() {
        let v = mdn_array(&[(0, 0, 0)]);
        let lo = mdn_array(&[(0, 0, 0)]);
        let hi = mdn_array(&[(0, 10, 0)]);
        let n = Arc::new(Int32Array::from(vec![0])); // n <= 0

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        assert!(downcast_i32(&out).is_null(0));
    }

    #[test]
    fn test_width_bucket_interval_mdn_nulls_propagate() {
        let v = Arc::new(IntervalMonthDayNanoArray::from(vec![
            None,
            Some(IntervalMonthDayNano::new(0, 5, 0)),
        ]));
        let lo = mdn_array(&[(0, 0, 0), (0, 0, 0)]);
        let hi = mdn_array(&[(0, 10, 0), (0, 10, 0)]);
        let n = i32_array_all(2, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
        assert_eq!(out.value(1), 6);
    }

    // --- Errores -------------------------------------------------------------

    #[test]
    fn test_width_bucket_wrong_arg_count() {
        let v = f64_array(&[1.0]);
        let lo = f64_array(&[0.0]);
        let hi = f64_array(&[10.0]);
        let err = width_bucket_kern(&[v, lo, hi]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("expects exactly 4"), "unexpected error: {msg}");
    }

    #[test]
    fn test_width_bucket_unsupported_type() {
        let v: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let lo = f64_array(&[0.0, 0.0, 0.0]);
        let hi = f64_array(&[10.0, 10.0, 10.0]);
        let n = i32_array_all(3, 10);

        let err = width_bucket_kern(&[v, lo, hi, n]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("width_bucket received unexpected data types"),
            "unexpected error: {msg}"
        );
    }
}

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

use crate::function::error_utils::unsupported_data_types_exec_err;
use arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, Float64Array, IntervalYearMonthArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{
    Duration, Float64, Int32, Interval,
};
use arrow::datatypes::IntervalUnit::YearMonth;
use datafusion_common::cast::{
    as_duration_microsecond_array, as_float64_array, as_int32_array, as_interval_ym_array,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_functions::utils::make_scalar_function;

use arrow::array::{Int32Array, Int32Builder};
use arrow::datatypes::TimeUnit::Microsecond;
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
        Self {
            signature: Signature::user_defined(Immutable),
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

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;

        let (v, lo, hi, n) = (&types[0], &types[1], &types[2], &types[3]);

        let is_num = |t: &DataType| {
            matches!(
                t,
                Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Decimal128(_, _)
            )
        };
        let is_int = |t: &DataType| matches!(t, Int8 | Int16 | Int32 | Int64);

        if is_num(v) && is_num(lo) && is_num(hi) && is_int(n) {
            return Ok(vec![Float64, Float64, Float64, Int32]);
        }

        let all_duration = matches!(v, Duration(_))
            && matches!(lo, Duration(_))
            && matches!(hi, Duration(_));
        if all_duration && is_int(n) {
            return Ok(vec![
                Duration(Microsecond),
                Duration(Microsecond),
                Duration(Microsecond),
                Int32,
            ]);
        }

        if matches!(v, Interval(YearMonth))
            && matches!(lo, Interval(YearMonth))
            && matches!(hi, Interval(YearMonth))
            && is_int(n)
        {
            return Ok(vec![
                Interval(YearMonth),
                Interval(YearMonth),
                Interval(YearMonth),
                Int32,
            ]);
        }

        exec_err!("rint expects a numeric argument, got {}", types[0])
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // FIXME copy
        // round preserves the order of the first argument
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
        return exec_err!("rint expects exactly 4 argument, got {}", args.len());
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

        other => Err(unsupported_data_types_exec_err(
            "width_bucket",
            "Float/Decimal OR Duration OR Interval(YearMonth) for first 3 args; Int for 4th",
            &[
                other.clone(),
                minv.data_type().clone(),
                maxv.data_type().clone(),
                nb.data_type().clone(),
            ],
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DurationMicrosecondArray, Float64Array, Int32Array,
        IntervalYearMonthArray,
    };
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

    // --- Float64 -------------------------------------------------------------

    #[test]
    fn test_width_bucket_f64_basic() {
        // v in [0.5, 1.0, 9.9, -1.0, 10.0], [lo, hi] = [0, 10], n=10
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
        // rango descendente: lo=10, hi=0, n=10
        let v = f64_array(&[9.9, 10.0, 0.0, -0.1, 10.1]);
        let lo = f64_array(&[10.0; 5]);
        let hi = f64_array(&[0.0; 5]);
        let n = i32_array_all(5, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);

        // Inclusión en el límite inferior: x == lo -> bucket 1
        assert_eq!(out.values(), &[1, 1, 11, 11, 0]);
    }
    #[test]
    fn test_width_bucket_f64_bounds_inclusive_exclusive_asc() {
        // ascendente: lo=0, hi=10, n=10
        let v = f64_array(&[0.0, 9.999999999, 10.0]);
        let lo = f64_array(&[0.0; 3]);
        let hi = f64_array(&[10.0; 3]);
        let n = i32_array_all(3, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 10, 11]); // lo inclusivo, hi exclusivo (>=hi -> n+1)
    }

    #[test]
    fn test_width_bucket_f64_bounds_inclusive_exclusive_desc() {
        // descendente: lo=10, hi=0, n=10
        let v = f64_array(&[10.0, 0.0, -0.000001]);
        let lo = f64_array(&[10.0; 3]);
        let hi = f64_array(&[0.0; 3]);
        let n = i32_array_all(3, 10);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[1, 11, 11]); // lo inclusivo, hi exclusivo (<=hi -> n+1)
    }


    #[test]
    fn test_width_bucket_f64_edge_cases() {
        // n <= 0  => null
        let v = f64_array(&[1.0, 5.0, 9.0]);
        let lo = f64_array(&[0.0, 0.0, 0.0]);
        let hi = f64_array(&[10.0, 10.0, 10.0]);
        let n = Arc::new(Int32Array::from(vec![0, -1, 10]));
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 10);

        // bounds iguales => null
        let v = f64_array(&[1.0]);
        let lo = f64_array(&[5.0]);
        let hi = f64_array(&[5.0]);
        let n = i32_array_all(1, 10);
        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert!(out.is_null(0));

        // NaN en cualquiera => null
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

        // null en lo / hi / n => null
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
        // v: [1s, 0, -1], lo=0, hi=2s, n=2
        let v = dur_us_array(&[1_000_000, 0, -1]);
        let lo = dur_us_array(&[0, 0, 0]);
        let hi = dur_us_array(&[2_000_000, 2_000_000, 2_000_000]);
        let n = i32_array_all(3, 2);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        // width = 1s; x<lo => 0; x>=hi => 3
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
        // meses: lo=0, hi=12, n=12
        // v: [0,5,11,12,13] -> [1,6,12,13,13]
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
        // descendente: lo=12, hi=0, n=12
        let v = ym_array(&[11, 12, 0, -1, 13]);
        let lo = ym_array(&[12; 5]);
        let hi = ym_array(&[0; 5]);
        let n = i32_array_all(5, 12);

        let out = width_bucket_kern(&[v, lo, hi, n]).unwrap();
        let out = downcast_i32(&out);
        assert_eq!(out.values(), &[2, 1, 13, 13, 0]);
    }


    // --- Errores -------------------------------------------------------------

    #[test]
    fn test_width_bucket_wrong_arg_count() {
        // 3 argumentos -> error
        let v = f64_array(&[1.0]);
        let lo = f64_array(&[0.0]);
        let hi = f64_array(&[10.0]);
        let err = width_bucket_kern(&[v, lo, hi]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("expects exactly 4"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_width_bucket_unsupported_type() {
        // Int32 en v -> debe caer en unsupported types (porque sólo soportas f64/duration/ym)
        let v: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let lo = f64_array(&[0.0, 0.0, 0.0]);
        let hi = f64_array(&[10.0, 10.0, 10.0]);
        let n = i32_array_all(3, 10);

        let err = width_bucket_kern(&[v, lo, hi, n]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported data types") || msg.contains("Float/Decimal OR Duration OR Interval(YearMonth)"),
            "unexpected error: {msg}"
        );
    }
}

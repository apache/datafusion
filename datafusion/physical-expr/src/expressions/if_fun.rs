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

use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::zip::zip;
use datafusion_common::{cast::as_boolean_array, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Implements IF(expr1, expr2, expr3)
/// If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.
/// Args: 0 - boolean array
///       1 - if expr1 is true, then the result is expr2
///       2 - if expr1 is true, then the result is expr3.
///
pub fn if_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but IF takes exactly 3 args",
            args.len(),
        )));
    }
    let mut array_lens = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });

    let (predicates, true_vals, false_vals) = (&args[0], &args[1], &args[2]);

    if let Some(size) = array_lens.next() {
        match predicates {
            ColumnarValue::Scalar(_) => {
                handle_scalar_predicates(predicates, true_vals, false_vals)
            }
            ColumnarValue::Array(predicates) => {
                handle_array_predicates(predicates, size, true_vals, false_vals)
            }
        }
    } else {
        handle_scalar_predicates(predicates, true_vals, false_vals)
    }
}

fn handle_scalar_predicates(
    predicates: &ColumnarValue,
    true_vals: &ColumnarValue,
    false_vals: &ColumnarValue,
) -> Result<ColumnarValue> {
    match predicates {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(predicates))) => {
            if *predicates {
                Ok(true_vals.clone())
            } else {
                Ok(false_vals.clone())
            }
        }
        ColumnarValue::Scalar(val) if val.is_null() => Ok(false_vals.clone()),
        _ => Err(DataFusionError::Internal(
            "the predicate expr must be boolean type".to_string(),
        )),
    }
}

fn handle_array_predicates(
    predicates: &ArrayRef,
    size: usize,
    true_vals: &ColumnarValue,
    false_vals: &ColumnarValue,
) -> Result<ColumnarValue> {
    let predicates = as_boolean_array(&predicates)?;

    let result = match (true_vals, false_vals) {
        (ColumnarValue::Array(true_arr), ColumnarValue::Array(false_arr)) => {
            zip(predicates, true_arr.as_ref(), false_arr.as_ref())?
        }
        (ColumnarValue::Array(true_arr), ColumnarValue::Scalar(false_scalar)) => {
            let false_arr = false_scalar.to_array_of_size(size);
            zip(predicates, true_arr.as_ref(), false_arr.as_ref())?
        }
        (ColumnarValue::Scalar(true_scalar), ColumnarValue::Array(false_arr)) => {
            let true_arr = true_scalar.to_array_of_size(size);
            zip(predicates, true_arr.as_ref(), false_arr.as_ref())?
        }
        (ColumnarValue::Scalar(true_scalar), ColumnarValue::Scalar(false_scalar)) => {
            let true_arr = true_scalar.to_array_of_size(size);
            let false_arr = false_scalar.to_array_of_size(size);
            zip(predicates, true_arr.as_ref(), false_arr.as_ref())?
        }
    };
    Ok(ColumnarValue::Array(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;

    use datafusion_common::{Result, ScalarValue};

    use super::*;

    #[test]
    fn if_scalar() -> Result<()> {
        let predicates = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
        let a = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let a = ColumnarValue::Array(Arc::new(a));
        let b = Int32Array::from(vec![Some(11), Some(22), Some(33), Some(44)]);
        let b = ColumnarValue::Array(Arc::new(b));

        let result = if_func(&[predicates, a, b])?;
        let expected =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.into_array(0).as_ref());
        Ok(())
    }

    #[test]
    fn if_with_null() -> Result<()> {
        let predicates =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);
        let predicates = ColumnarValue::Array(Arc::new(predicates));
        let a = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let a = ColumnarValue::Array(Arc::new(a));
        let b = Int32Array::from(vec![Some(11), Some(22), Some(33), Some(44)]);
        let b = ColumnarValue::Array(Arc::new(b));

        let result = if_func(&[predicates, a, b])?;
        let result = result.into_array(0);
        let expected =
            Arc::new(Int32Array::from(vec![Some(11), Some(2), Some(33), Some(3)]))
                as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn if_invalid_data_types() -> Result<()> {
        let predicates = Int32Array::from(vec![Some(1), Some(2), None, None]);
        let predicates = ColumnarValue::Array(Arc::new(predicates));
        let a = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let a = ColumnarValue::Array(Arc::new(a));
        let b = Int32Array::from(vec![Some(11), Some(22), Some(33), Some(44)]);
        let b = ColumnarValue::Array(Arc::new(b));

        let result = if_func(&[predicates, a, b]);
        assert!(result.is_err());

        let predicates =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);
        let predicates = ColumnarValue::Array(Arc::new(predicates));
        let a = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let a = ColumnarValue::Array(Arc::new(a));
        let b =
            Int64Array::from(vec![Some(11i64), Some(22i64), Some(33i64), Some(44i64)]);
        let b = ColumnarValue::Array(Arc::new(b));

        let result = if_func(&[predicates, a, b]);
        assert!(result.is_err());
        Ok(())
    }
}

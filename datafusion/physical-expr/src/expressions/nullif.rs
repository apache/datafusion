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

use arrow::array::Array;
use arrow::compute::eq_dyn;
use arrow::compute::nullif::nullif;
use datafusion_common::{cast::as_boolean_array, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

use super::binary::array_eq_scalar;

/// Implements NULLIF(expr1, expr2)
/// Args: 0 - left expr is any array
///       1 - if the left is equal to this expr2, then the result is NULL, otherwise left value is passed.
///
pub fn nullif_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but NULLIF takes exactly two args",
            args.len(),
        )));
    }

    let (lhs, rhs) = (&args[0], &args[1]);

    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let cond_array = array_eq_scalar(lhs, rhs)?;

            let array = nullif(lhs, as_boolean_array(&cond_array)?)?;

            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            // Get args0 == args1 evaluated and produce a boolean array
            let cond_array = eq_dyn(lhs, rhs)?;

            // Now, invoke nullif on the result
            let array = nullif(lhs, as_boolean_array(&cond_array)?)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)) => {
            // Similar to Array-Array case, except of ScalarValue -> Array cast
            let lhs = lhs.to_array_of_size(rhs.len());
            let cond_array = eq_dyn(&lhs, rhs)?;

            let array = nullif(&lhs, as_boolean_array(&cond_array)?)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)) => {
            let val: ScalarValue = match lhs.eq(rhs) {
                true => lhs.get_datatype().try_into()?,
                false => lhs.clone(),
            };

            Ok(ColumnarValue::Scalar(val))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;

    use super::*;
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn nullif_int32() -> Result<()> {
        let a = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke NULLIF() correctly
    fn nullif_int32_nonulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected = Arc::new(Int32Array::from(vec![
            None,
            Some(3),
            Some(10),
            Some(7),
            Some(8),
            None,
            Some(2),
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_boolean() -> Result<()> {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected =
            Arc::new(BooleanArray::from(vec![Some(true), None, None])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_string() -> Result<()> {
        let a = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Utf8(Some("bar".to_string())));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected = Arc::new(StringArray::from(vec![
            Some("foo"),
            None,
            None,
            Some("baz"),
        ])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_literal_first() -> Result<()> {
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nullif_func(&[lit_array, a])?;
        let result = result.into_array(0);

        let expected = Arc::new(Int32Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(2),
            Some(2),
            Some(2),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_scalar() -> Result<()> {
        let a_eq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_eq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result_eq = nullif_func(&[a_eq, b_eq])?;
        let result_eq = result_eq.into_array(1);

        let expected_eq = Arc::new(Int32Array::from(vec![None])) as ArrayRef;

        assert_eq!(expected_eq.as_ref(), result_eq.as_ref());

        let a_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result_neq = nullif_func(&[a_neq, b_neq])?;
        let result_neq = result_neq.into_array(1);

        let expected_neq = Arc::new(Int32Array::from(vec![Some(2i32)])) as ArrayRef;
        assert_eq!(expected_neq.as_ref(), result_neq.as_ref());

        Ok(())
    }
}

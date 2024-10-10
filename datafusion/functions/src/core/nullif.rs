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

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, Documentation};

use arrow::compute::kernels::cmp::eq;
use arrow::compute::kernels::nullif::nullif;
use datafusion_common::ScalarValue;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_CONDITIONAL;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct NullIfFunc {
    signature: Signature,
}

/// Currently supported types by the nullif function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
static SUPPORTED_NULLIF_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];

impl Default for NullIfFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NullIfFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                SUPPORTED_NULLIF_TYPES.to_vec(),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NullIfFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "nullif"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // NULLIF has two args and they might get coerced, get a preview of this
        let coerced_types = datafusion_expr::type_coercion::functions::data_types(
            arg_types,
            &self.signature,
        );
        coerced_types
            .map(|typs| typs[0].clone())
            .map_err(|e| e.context("Failed to coerce arguments for NULLIF"))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        nullif_func(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_nullif_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_nullif_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_CONDITIONAL)
            .with_description("Returns _null_ if _expression1_ equals _expression2_; otherwise it returns _expression1_.
This can be used to perform the inverse operation of [`coalesce`](#coalesce).")
            .with_syntax_example("nullif(expression1, expression2)")
            .with_sql_example(r#"```sql
> select nullif('datafusion', 'data');
+-----------------------------------------+
| nullif(Utf8("datafusion"),Utf8("data")) |
+-----------------------------------------+
| datafusion                              |
+-----------------------------------------+
> select nullif('datafusion', 'datafusion');
+-----------------------------------------------+
| nullif(Utf8("datafusion"),Utf8("datafusion")) |
+-----------------------------------------------+
|                                               |
+-----------------------------------------------+
```
"#)
            .with_argument(
                "expression1",
                "Expression to compare and return if equal to expression2. Can be a constant, column, or function, and any combination of operators."
            )
            .with_argument(
                "expression2",
                "Expression to compare to expression1. Can be a constant, column, or function, and any combination of operators."
            )
            .build()
            .unwrap()
    })
}

/// Implements NULLIF(expr1, expr2)
/// Args: 0 - left expr is any array
///       1 - if the left is equal to this expr2, then the result is NULL, otherwise left value is passed.
///
fn nullif_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "{:?} args were supplied but NULLIF takes exactly two args",
            args.len()
        );
    }

    let (lhs, rhs) = (&args[0], &args[1]);

    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let rhs = rhs.to_scalar()?;
            let array = nullif(lhs, &eq(&lhs, &rhs)?)?;

            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            let array = nullif(lhs, &eq(&lhs, &rhs)?)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)) => {
            let lhs_s = lhs.to_scalar()?;
            let lhs_a = lhs.to_array_of_size(rhs.len())?;
            let array = nullif(
                // nullif in arrow-select does not support Datum, so we need to convert to array
                lhs_a.as_ref(),
                &eq(&lhs_s, &rhs)?,
            )?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)) => {
            let val: ScalarValue = match lhs.eq(rhs) {
                true => lhs.data_type().try_into()?,
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
        let result = result.into_array(0).expect("Failed to convert to array");

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
        let result = result.into_array(0).expect("Failed to convert to array");

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
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected =
            Arc::new(BooleanArray::from(vec![Some(true), None, None])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_string() -> Result<()> {
        let a = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::from("bar"));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

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
        let result = result.into_array(0).expect("Failed to convert to array");

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
        let result_eq = result_eq.into_array(1).expect("Failed to convert to array");

        let expected_eq = Arc::new(Int32Array::from(vec![None])) as ArrayRef;

        assert_eq!(expected_eq.as_ref(), result_eq.as_ref());

        let a_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result_neq = nullif_func(&[a_neq, b_neq])?;
        let result_neq = result_neq
            .into_array(1)
            .expect("Failed to convert to array");

        let expected_neq = Arc::new(Int32Array::from(vec![Some(2i32)])) as ArrayRef;
        assert_eq!(expected_neq.as_ref(), result_neq.as_ref());

        Ok(())
    }
}

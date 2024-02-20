use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null};
use arrow::array::{Array, BooleanArray};

#[derive(Debug)]
pub(super) struct NVLFunc {
    signature: Signature,
    aliases: Vec<String>,
}

/// Currently supported types by the nvl/ifnull function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_NVL_TYPES: &[DataType] = &[
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

impl NVLFunc {
    pub fn new() -> Self {
        Self {
            signature:
            Signature::uniform(2, SUPPORTED_NVL_TYPES.to_vec(),
                Volatility::Immutable,
            ),
            aliases: vec![String::from("ifnull")],
        }
    }
}

impl ScalarUDFImpl for NVLFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // NVL has two args and they might get coerced, get a preview of this
        let coerced_types = datafusion_expr::type_coercion::functions::data_types(arg_types, &self.signature);
        coerced_types.map(|typs| typs[0].clone())
            .map_err(|e| e.context("Failed to coerce arguments for NVL")
        )
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        nvl_func(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn nvl_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return internal_err!(
            "{:?} args were supplied but NVL/IFNULL takes exactly two args",
            args.len()
        );
    }
    let (lhs, rhs) = (&args[0], &args[1]);
    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let size = lhs.len();
            let mut current_value = rhs.to_array_of_size(size)?;
            let remainder = BooleanArray::from(vec![true; size]);
            let to_apply = and(&remainder, &is_not_null(lhs.as_ref())?)?;
            current_value = zip(&to_apply, lhs, &current_value)?;
            Ok(ColumnarValue::Array(current_value))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            let size = lhs.len();
            // start with nulls as default output
            let remainder = BooleanArray::from(vec![true; size]);
            let to_apply = and(&remainder, &is_not_null(lhs.as_ref())?)?;
            let current_value = zip(&to_apply, lhs, &rhs)?;
            Ok(ColumnarValue::Array(current_value))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)) => {
            let size = rhs.len();
            let mut current_value = lhs.to_array_of_size(size)?;
            let remainder = BooleanArray::from(vec![true; size]);
            let to_apply = and(&remainder, &is_not_null(current_value.as_ref())?)?;
            current_value = zip(&to_apply, &current_value, &rhs)?;
            Ok(ColumnarValue::Array(current_value))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)) => {
            let mut current_value = lhs;
            if lhs.is_null() {
                current_value = rhs;
            }
            Ok(ColumnarValue::Scalar(current_value.clone()))
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
    fn nvl_int32() -> Result<()> {
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

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(6i32)));

        let result = nvl_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(6),
            Some(6),
            Some(3),
            Some(6),
            Some(6),
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke nvl() correctly
    fn nvl_int32_nonulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(20i32)));

        let result = nvl_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(3),
            Some(10),
            Some(7),
            Some(8),
            Some(1),
            Some(2),
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl_boolean() -> Result<()> {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));

        let result = nvl_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected =
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(false)])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl_string() -> Result<()> {
        let a = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::from("bax"));

        let result = nvl_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            Some("bax"),
            Some("baz"),
        ])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl_literal_first() -> Result<()> {
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nvl_func(&[lit_array, a])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl_scalar() -> Result<()> {
        let a_null = ColumnarValue::Scalar(ScalarValue::Int32(None));
        let b_null = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result_null = nvl_func(&[a_null, b_null])?;
        let result_null = result_null.into_array(1).expect("Failed to convert to array");

        let expected_null = Arc::new(Int32Array::from(vec![Some(2i32)])) as ArrayRef;

        assert_eq!(expected_null.as_ref(), result_null.as_ref());

        let a_nnull = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_nnull = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result_nnull = nvl_func(&[a_nnull, b_nnull])?;
        let result_nnull = result_nnull
            .into_array(1)
            .expect("Failed to convert to array");

        let expected_nnull = Arc::new(Int32Array::from(vec![Some(2i32)])) as ArrayRef;
        assert_eq!(expected_nnull.as_ref(), result_nnull.as_ref());

        Ok(())
    }
}

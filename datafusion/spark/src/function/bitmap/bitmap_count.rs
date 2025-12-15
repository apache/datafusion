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
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int64Array,
    LargeBinaryArray, as_dictionary_array,
};
use arrow::datatypes::DataType::{
    Binary, BinaryView, Dictionary, FixedSizeBinary, LargeBinary,
};
use arrow::datatypes::{DataType, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitmapCount {
    signature: Signature,
}

impl Default for BitmapCount {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapCount {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Binary)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitmapCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        use arrow::datatypes::Field;
        // bitmap_count returns Int64 with the same nullability as the input
        Ok(Arc::new(Field::new(
            args.arg_fields[0].name(),
            DataType::Int64,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(bitmap_count_inner, vec![])(&args.args)
    }
}

fn binary_count_ones(opt: Option<&[u8]>) -> Option<i64> {
    opt.map(|value| value.iter().map(|b| b.count_ones() as i64).sum())
}

macro_rules! downcast_and_count_ones {
    ($input_array:expr, $array_type:ident) => {{
        let arr = downcast_arg!($input_array, $array_type);
        Ok(arr.iter().map(binary_count_ones).collect::<Int64Array>())
    }};
}

macro_rules! downcast_dict_and_count_ones {
    ($input_dict:expr, $key_array_type:ident) => {{
        let dict_array = as_dictionary_array::<$key_array_type>($input_dict);
        let array = dict_array.downcast_dict::<BinaryArray>().unwrap();
        Ok(array
            .into_iter()
            .map(binary_count_ones)
            .collect::<Int64Array>())
    }};
}

pub fn bitmap_count_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("bitmap_count", arg)?;

    let res: Result<Int64Array> = match &input_array.data_type() {
        Binary => downcast_and_count_ones!(input_array, BinaryArray),
        BinaryView => downcast_and_count_ones!(input_array, BinaryViewArray),
        LargeBinary => downcast_and_count_ones!(input_array, LargeBinaryArray),
        FixedSizeBinary(_size) => {
            downcast_and_count_ones!(input_array, FixedSizeBinaryArray)
        }
        Dictionary(k, v) if v.as_ref() == &Binary => match k.as_ref() {
            DataType::Int8 => downcast_dict_and_count_ones!(input_array, Int8Type),
            DataType::Int16 => downcast_dict_and_count_ones!(input_array, Int16Type),
            DataType::Int32 => downcast_dict_and_count_ones!(input_array, Int32Type),
            DataType::Int64 => downcast_dict_and_count_ones!(input_array, Int64Type),
            data_type => {
                internal_err!(
                    "bitmap_count does not support Dictionary({data_type}, Binary)"
                )
            }
        },
        data_type => {
            internal_err!("bitmap_count does not support {data_type}")
        }
    };

    Ok(Arc::new(res?))
}

#[cfg(test)]
mod tests {
    use crate::function::bitmap::bitmap_count::BitmapCount;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType::Int64;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue::Scalar;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    macro_rules! test_bitmap_count_binary_invoke {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Binary($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeBinary($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::BinaryView($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
                    $INPUT.map(|a| a.len()).unwrap_or(0) as i32,
                    $INPUT
                ))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );
        };
    }

    #[test]
    fn test_bitmap_count_invoke() -> Result<()> {
        test_bitmap_count_binary_invoke!(None::<Vec<u8>>, Ok(None));
        test_bitmap_count_binary_invoke!(Some(vec![0x0Au8]), Ok(Some(2)));
        test_bitmap_count_binary_invoke!(Some(vec![0xFFu8, 0xFFu8]), Ok(Some(16)));
        test_bitmap_count_binary_invoke!(
            Some(vec![0x0Au8, 0xB0u8, 0xCDu8]),
            Ok(Some(10))
        );
        Ok(())
    }

    #[test]
    fn test_dictionary_encoded_bitmap_count_invoke() -> Result<()> {
        let dict = Scalar(ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Binary(Some(vec![0xFFu8, 0xFFu8]))),
        ));

        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Binary),
                ),
                true,
            )
            .into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![dict.clone()],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", Int64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let udf = BitmapCount::new();
        let actual = udf.invoke_with_args(args)?;
        let expect = Scalar(ScalarValue::Int64(Some(16)));
        assert_eq!(*actual.into_array(1)?, *expect.into_array(1)?);
        Ok(())
    }

    #[test]
    fn test_bitmap_count_nullability() -> Result<()> {
        use datafusion_expr::ReturnFieldArgs;

        let bitmap_count = BitmapCount::new();

        // Test with non-nullable binary field
        let non_nullable_field = Arc::new(Field::new("bin", DataType::Binary, false));

        let result = bitmap_count.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&non_nullable_field)],
            scalar_arguments: &[None],
        })?;

        // The result should not be nullable (same as input)
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), &Int64);

        // Test with nullable binary field
        let nullable_field = Arc::new(Field::new("bin", DataType::Binary, true));

        let result = bitmap_count.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&nullable_field)],
            scalar_arguments: &[None],
        })?;

        // The result should be nullable (same as input)
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &Int64);

        Ok(())
    }
}

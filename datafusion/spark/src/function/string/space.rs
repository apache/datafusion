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

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StringBuilder,
    as_dictionary_array,
};
use arrow::datatypes::{DataType, Int32Type};
use datafusion_common::cast::as_int32_array;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `space` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#space>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSpace {
    signature: Signature,
}

impl Default for SparkSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSpace {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int32,
                    DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Int32),
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSpace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "space"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let return_type = match &args[0] {
            DataType::Dictionary(key_type, _) => {
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8))
            }
            _ => DataType::Utf8,
        };
        Ok(return_type)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_space(&args.args)
    }
}

pub fn spark_space(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return exec_err!("space function takes exactly one argument");
    }
    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = spark_space_array(array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = spark_space_scalar(scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn spark_space_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int32 => {
            let array = as_int32_array(array)?;
            Ok(Arc::new(spark_space_array_inner(array)))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = spark_space_array(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `space`")
        }
    }
}

fn spark_space_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Int32(value) => {
            let result = value.map(|v| {
                if v <= 0 {
                    String::new()
                } else {
                    " ".repeat(v as usize)
                }
            });
            Ok(ScalarValue::Utf8(result))
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `space`")
        }
    }
}

fn spark_space_array_inner(array: &Int32Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut space_buf = String::new();
    for value in array.iter() {
        match value {
            None => builder.append_null(),
            Some(l) if l > 0 => {
                let l = l as usize;
                if space_buf.len() < l {
                    space_buf = " ".repeat(l);
                }
                builder.append_value(&space_buf[..l]);
            }
            Some(_) => builder.append_value(""),
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use crate::function::string::space::spark_space;
    use arrow::array::{Array, Int32Array, Int32DictionaryArray};
    use arrow::datatypes::Int32Type;
    use datafusion_common::cast::{as_dictionary_array, as_string_array};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_spark_space_int32_array() -> Result<()> {
        let int32_array = ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(1),
            Some(-3),
            Some(0),
            Some(5),
            None,
        ])));
        let ColumnarValue::Array(result) = spark_space(&[int32_array])? else {
            unreachable!()
        };
        let result = as_string_array(&result)?;

        assert_eq!(result.value(0), " ");
        assert_eq!(result.value(1), "");
        assert_eq!(result.value(2), "");
        assert_eq!(result.value(3), "     ");
        assert!(result.is_null(4));
        Ok(())
    }

    #[test]
    fn test_spark_space_dictionary() -> Result<()> {
        let dictionary = ColumnarValue::Array(Arc::new(Int32DictionaryArray::new(
            Int32Array::from(vec![0, 1, 2, 3, 4]),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(-3),
                Some(0),
                Some(5),
                None,
            ])),
        )));
        let ColumnarValue::Array(result) = spark_space(&[dictionary])? else {
            unreachable!()
        };
        let result =
            as_string_array(as_dictionary_array::<Int32Type>(&result)?.values())?;
        assert_eq!(result.value(0), " ");
        assert_eq!(result.value(1), "");
        assert_eq!(result.value(2), "");
        assert_eq!(result.value(3), "     ");
        assert!(result.is_null(4));
        Ok(())
    }

    #[test]
    fn test_spark_space_scalar() -> Result<()> {
        let scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(-5)));
        let ColumnarValue::Scalar(result) = spark_space(&[scalar])? else {
            unreachable!()
        };
        match result {
            ScalarValue::Utf8(Some(result)) => {
                assert_eq!(result, "");
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

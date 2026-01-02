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

use arrow::array::{ArrayRef, DictionaryArray, StringArray};
use arrow::datatypes::{DataType, Int32Type};
use datafusion_common::cast::{as_dictionary_array, as_int32_array};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
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
        if args.args.len() != 1 {
            return plan_err!("space expects exactly 1 argument");
        }
        make_scalar_function(spark_space, vec![])(&args.args)
    }
}

fn spark_space(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = &args[0];
    match array.data_type() {
        DataType::Int32 => {
            let result = space(array);
            Ok(Arc::new(result))
        }
        DataType::Dictionary(_, v) if v.as_ref() == &DataType::Int32 => {
            let dictionary_array = as_dictionary_array::<Int32Type>(array)?;
            let values = Arc::new(space(dictionary_array.values()));
            let result =
                DictionaryArray::try_new(dictionary_array.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `space`")
        }
    }
}

fn space(array: &ArrayRef) -> StringArray {
    as_int32_array(array)
        .unwrap()
        .iter()
        .map(|n| {
            n.map(|m| {
                if m < 0 {
                    String::new()
                } else {
                    " ".repeat(m as usize)
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::function::string::space::spark_space;
    use arrow::array::{Array, Int32Array, Int32DictionaryArray};
    use arrow::datatypes::Int32Type;
    use datafusion_common::Result;
    use datafusion_common::cast::{as_dictionary_array, as_string_array};
    use std::sync::Arc;

    #[test]
    fn test_spark_space_int32_array() -> Result<()> {
        let int32_array = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(-3),
            Some(0),
            Some(5),
            None,
        ]));
        let result = spark_space(&[int32_array])?;
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
        let dictionary = Arc::new(Int32DictionaryArray::new(
            Int32Array::from(vec![0, 1, 2, 3, 4]),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(-3),
                Some(0),
                Some(5),
                None,
            ])),
        ));
        let result = spark_space(&[dictionary])?;
        let result =
            as_string_array(as_dictionary_array::<Int32Type>(&result)?.values())?;
        assert_eq!(result.value(0), " ");
        assert_eq!(result.value(1), "");
        assert_eq!(result.value(2), "");
        assert_eq!(result.value(3), "     ");
        assert!(result.is_null(4));
        Ok(())
    }
}

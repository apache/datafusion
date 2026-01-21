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

use arrow::array::{ArrayRef, Scalar};
use arrow::compute::kernels::comparison::ends_with as arrow_ends_with;
use arrow::datatypes::DataType;

use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::binary::{binary_to_string_coercion, string_coercion};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Tests if a string ends with a substring.",
    syntax_example = "ends_with(str, substr)",
    sql_example = r#"```sql
>  select ends_with('datafusion', 'soin');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("soin")) |
+--------------------------------------------+
| false                                      |
+--------------------------------------------+
> select ends_with('datafusion', 'sion');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("sion")) |
+--------------------------------------------+
| true                                       |
+--------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring to test for.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EndsWithFunc {
    signature: Signature,
}

impl Default for EndsWithFunc {
    fn default() -> Self {
        EndsWithFunc::new()
    }
}

impl EndsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EndsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ends_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [str_arg, suffix_arg] = take_function_args(self.name(), &args.args)?;

        // Determine the common type for coercion
        let coercion_type = string_coercion(
            &str_arg.data_type(),
            &suffix_arg.data_type(),
        )
        .or_else(|| {
            binary_to_string_coercion(&str_arg.data_type(), &suffix_arg.data_type())
        });

        let Some(coercion_type) = coercion_type else {
            return exec_err!(
                "Unsupported data types {:?}, {:?} for function `ends_with`.",
                str_arg.data_type(),
                suffix_arg.data_type()
            );
        };

        // Helper to cast an array if needed
        let maybe_cast = |arr: &ArrayRef, target: &DataType| -> Result<ArrayRef> {
            if arr.data_type() == target {
                Ok(Arc::clone(arr))
            } else {
                Ok(arrow::compute::kernels::cast::cast(arr, target)?)
            }
        };

        match (str_arg, suffix_arg) {
            // Both scalars - just compute directly
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Scalar(suffix_scalar)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let suffix_arr = suffix_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let suffix_arr = maybe_cast(&suffix_arr, &coercion_type)?;
                let result = arrow_ends_with(&str_arr, &suffix_arr)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
            // String is array, suffix is scalar - use Scalar wrapper for optimization
            (ColumnarValue::Array(str_arr), ColumnarValue::Scalar(suffix_scalar)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let suffix_arr = suffix_scalar.to_array_of_size(1)?;
                let suffix_arr = maybe_cast(&suffix_arr, &coercion_type)?;
                let suffix_scalar = Scalar::new(suffix_arr);
                let result = arrow_ends_with(&str_arr, &suffix_scalar)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // String is scalar, suffix is array - use Scalar wrapper for string
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Array(suffix_arr)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let str_scalar = Scalar::new(str_arr);
                let suffix_arr = maybe_cast(suffix_arr, &coercion_type)?;
                let result = arrow_ends_with(&str_scalar, &suffix_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // Both arrays - pass directly
            (ColumnarValue::Array(str_arr), ColumnarValue::Array(suffix_arr)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let suffix_arr = maybe_cast(suffix_arr, &coercion_type)?;
                let result = arrow_ends_with(&str_arr, &suffix_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, StringArray};
    use arrow::datatypes::DataType::Boolean;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use crate::string::ends_with::EndsWithFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_scalar_scalar() -> Result<()> {
        // Test Scalar + Scalar combinations
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("bet")),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );

        // Test with LargeUtf8
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
                    "alphabet".to_string()
                ))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("bet".to_string()))),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );

        // Test with Utf8View
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    "alphabet".to_string()
                ))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("bet".to_string()))),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }

    #[test]
    fn test_array_scalar() -> Result<()> {
        // Test Array + Scalar (the optimized path)
        let array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("alphabet"),
            Some("beta"),
            None,
        ])));
        let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("bet".to_string())));

        let args = vec![array, scalar];
        test_function!(
            EndsWithFunc::new(),
            args,
            Ok(Some(true)), // First element result: "alphabet" ends with "bet"
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }

    #[test]
    fn test_array_scalar_full_result() {
        // Test Array + Scalar and verify all results
        let func = EndsWithFunc::new();
        let array = Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("alphabet"),
            Some("beta"),
            None,
        ]));
        let args = vec![
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("bet".to_string()))),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" ends with "bet"
        assert!(bool_array.value(1)); // "alphabet" ends with "bet"
        assert!(!bool_array.value(2)); // "beta" does not end with "bet"
        assert!(bool_array.is_null(3)); // null input -> null output
    }

    #[test]
    fn test_scalar_array() {
        // Test Scalar + Array
        let func = EndsWithFunc::new();
        let suffixes = Arc::new(StringArray::from(vec![
            Some("bet"),
            Some("alph"),
            Some("phabet"),
            None,
        ]));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("alphabet".to_string()))),
            ColumnarValue::Array(suffixes),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" ends with "bet"
        assert!(!bool_array.value(1)); // "alphabet" does not end with "alph"
        assert!(bool_array.value(2)); // "alphabet" ends with "phabet"
        assert!(bool_array.is_null(3)); // null suffix -> null output
    }

    #[test]
    fn test_array_array() {
        // Test Array + Array
        let func = EndsWithFunc::new();
        let strings = Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("rust"),
            Some("datafusion"),
            None,
        ]));
        let suffixes = Arc::new(StringArray::from(vec![
            Some("bet"),
            Some("st"),
            Some("hello"),
            Some("test"),
        ]));
        let args = vec![
            ColumnarValue::Array(strings),
            ColumnarValue::Array(suffixes),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" ends with "bet"
        assert!(bool_array.value(1)); // "rust" ends with "st"
        assert!(!bool_array.value(2)); // "datafusion" does not end with "hello"
        assert!(bool_array.is_null(3)); // null string -> null output
    }
}

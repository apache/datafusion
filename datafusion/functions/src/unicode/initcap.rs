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
    Array, ArrayRef, GenericStringBuilder, OffsetSizeTrait, StringViewBuilder,
};
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignatureClass,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Capitalizes the first character in each word in the input string. \
            Words are delimited by non-alphanumeric characters.",
    syntax_example = "initcap(str)",
    sql_example = r#"```sql
> select initcap('apache datafusion');
+------------------------------------+
| initcap(Utf8("apache datafusion")) |
+------------------------------------+
| Apache Datafusion                  |
+------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "lower"),
    related_udf(name = "upper")
)]
#[derive(Debug)]
pub struct InitcapFunc {
    signature: Signature,
}

impl Default for InitcapFunc {
    fn default() -> Self {
        InitcapFunc::new()
    }
}

impl InitcapFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for InitcapFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "initcap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let DataType::Utf8View = arg_types[0] {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "initcap")
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(initcap::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(initcap::<i64>, vec![])(args),
            DataType::Utf8View => make_scalar_function(initcap_utf8view, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function `initcap`")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Converts the first letter of each word to upper case and the rest to lower
/// case. Words are sequences of alphanumeric characters separated by
/// non-alphanumeric characters.
///
/// Example:
/// ```sql
/// initcap('hi THOMAS') = 'Hi Thomas'
/// ```
fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    let mut builder = GenericStringBuilder::<T>::with_capacity(
        string_array.len(),
        string_array.value_data().len(),
    );

    let mut container = String::new();
    string_array.iter().for_each(|str| match str {
        Some(s) => {
            initcap_string(s, &mut container);
            builder.append_value(&container);
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn initcap_utf8view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;

    let mut builder = StringViewBuilder::with_capacity(string_view_array.len());

    let mut container = String::new();
    string_view_array.iter().for_each(|str| match str {
        Some(s) => {
            initcap_string(s, &mut container);
            builder.append_value(&container);
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn initcap_string(input: &str, container: &mut String) {
    container.clear();
    let mut prev_is_alphanumeric = false;

    if input.is_ascii() {
        for c in input.chars() {
            if prev_is_alphanumeric {
                container.push(c.to_ascii_lowercase());
            } else {
                container.push(c.to_ascii_uppercase());
            };
            prev_is_alphanumeric = c.is_ascii_alphanumeric();
        }
    } else {
        for c in input.chars() {
            if prev_is_alphanumeric {
                container.extend(c.to_lowercase());
            } else {
                container.extend(c.to_uppercase());
            }
            prev_is_alphanumeric = c.is_alphanumeric();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::unicode::initcap::InitcapFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from("hi THOMAS"))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "êM ả ñAnDÚ ÁrBOL ОлЕГ ИвАНОВИч ÍslENsku ÞjóðaRiNNaR εΛλΗΝΙκΉ"
                    .to_string()
            )))],
            Ok(Some(
                "Êm Ả Ñandú Árbol Олег Иванович Íslensku Þjóðarinnar Ελληνική"
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS".to_string()
            )))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS wIth M0re ThAN 12 ChaRs".to_string()
            )))],
            Ok(Some("Hi Thomas With M0re Than 12 Chars")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "đẸp đẼ êM ả ñAnDÚ ÁrBOL ОлЕГ ИвАНОВИч ÍslENsku ÞjóðaRiNNaR εΛλΗΝΙκΉ"
                    .to_string()
            )))],
            Ok(Some(
                "Đẹp Đẽ Êm Ả Ñandú Árbol Олег Иванович Íslensku Þjóðarinnar Ελληνική"
            )),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "".to_string()
            )))],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(None))],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );

        Ok(())
    }
}

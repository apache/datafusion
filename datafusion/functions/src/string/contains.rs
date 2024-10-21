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

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::contains as arrow_contains;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Boolean, LargeUtf8, Utf8, Utf8View};
use datafusion_common::exec_err;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct ContainsFunc {
    signature: Signature,
}

impl Default for ContainsFunc {
    fn default() -> Self {
        ContainsFunc::new()
    }
}

impl ContainsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ContainsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(contains, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_contains_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_contains_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description(
                "Return true if search_str is found within string (case-sensitive).",
            )
            .with_syntax_example("contains(str, search_str)")
            .with_sql_example(
                r#"```sql
> select contains('the quick brown fox', 'row');
+---------------------------------------------------+
| contains(Utf8("the quick brown fox"),Utf8("row")) |
+---------------------------------------------------+
| true                                              |
+---------------------------------------------------+
```"#,
            )
            .with_standard_argument("str", Some("String"))
            .with_argument("search_str", "The string to search for in str.")
            .build()
            .unwrap()
    })
}

/// use `arrow::compute::contains` to do the calculation for contains
pub fn contains(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    match (args[0].data_type(), args[1].data_type()) {
        (Utf8View, Utf8View) => {
            let mod_str = args[0].as_string_view();
            let match_str = args[1].as_string_view();
            let res = arrow_contains(mod_str, match_str)?;
            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8, Utf8) => {
            let mod_str = args[0].as_string::<i32>();
            let match_str = args[1].as_string::<i32>();
            let res = arrow_contains(mod_str, match_str)?;
            Ok(Arc::new(res) as ArrayRef)
        }
        (LargeUtf8, LargeUtf8) => {
            let mod_str = args[0].as_string::<i64>();
            let match_str = args[1].as_string::<i64>();
            let res = arrow_contains(mod_str, match_str)?;
            Ok(Arc::new(res) as ArrayRef)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `contains`.")
        }
    }
}

#[cfg(test)]
mod test {
    use super::ContainsFunc;
    use arrow::array::{BooleanArray, StringArray};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_contains_udf() {
        let udf = ContainsFunc::new();
        let array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("xxx?()"),
            Some("yyy?()"),
        ])));
        let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("x?(".to_string())));
        let actual = udf.invoke(&[array, scalar]).unwrap();
        let expect = ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
        ])));
        assert_eq!(
            *actual.into_array(2).unwrap(),
            *expect.into_array(2).unwrap()
        );
    }
}

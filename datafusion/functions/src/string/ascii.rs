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
use arrow::array::{ArrayAccessor, ArrayIter, ArrayRef, AsArray, Int32Array};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::types::logical_string;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignatureClass;
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the Unicode character code of the first character in a string.",
    syntax_example = "ascii(str)",
    sql_example = r#"```sql
> select ascii('abc');
+--------------------+
| ascii(Utf8("abc")) |
+--------------------+
| 97                 |
+--------------------+
> select ascii('🚀');
+-------------------+
| ascii(Utf8("🚀")) |
+-------------------+
| 128640            |
+-------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "chr")
)]
#[derive(Debug)]
pub struct AsciiFunc {
    signature: Signature,
}

impl Default for AsciiFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsciiFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![TypeSignatureClass::Native(logical_string())],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AsciiFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ascii"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        Ok(Int32)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(ascii, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn calculate_ascii<'a, V>(array: V) -> Result<ArrayRef, ArrowError>
where
    V: ArrayAccessor<Item = &'a str>,
{
    let iter = ArrayIter::new(array);
    let result = iter
        .map(|string| {
            string.map(|s| {
                let mut chars = s.chars();
                chars.next().map_or(0, |v| v as i32)
            })
        })
        .collect::<Int32Array>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the numeric code of the first character of the argument.
pub fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            Ok(calculate_ascii(string_array)?)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            Ok(calculate_ascii(string_array)?)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            Ok(calculate_ascii(string_array)?)
        }
        other => internal_err!("Unsupported data type for ascii: {:?}", other),
    }
}

#[cfg(test)]
mod tests {
    use crate::expr_fn::ascii;
    use crate::string::ascii::AsciiFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::DataType::Int32;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr::{col, lit, ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    macro_rules! test_ascii_invoke {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    fn ascii_array(input: ArrayRef) -> Result<ArrayRef> {
        let batch = RecordBatch::try_from_iter([("c0", input)])?;
        let df_schema = DFSchema::try_from(batch.schema())?;
        let expr = ascii(col("c0"));
        let physical_expr =
            SessionContext::new().create_physical_expr(expr, &df_schema)?;
        let result = match physical_expr.evaluate(&batch)? {
            ColumnarValue::Array(result) => Ok(result),
            _ => datafusion_common::internal_err!("ascii"),
        }?;
        Ok(result)
    }

    fn ascii_scalar(input: ScalarValue) -> Result<ScalarValue> {
        let df_schema = DFSchema::empty();
        let expr = ascii(lit(input));
        let physical_expr =
            SessionContext::new().create_physical_expr(expr, &df_schema)?;
        let result = match physical_expr
            .evaluate(&RecordBatch::new_empty(Arc::clone(df_schema.inner())))?
        {
            ColumnarValue::Scalar(result) => Ok(result),
            _ => datafusion_common::internal_err!("ascii"),
        }?;
        Ok(result)
    }

    #[test]
    fn test_ascii_invoke() -> Result<()> {
        test_ascii_invoke!(Some(String::from("x")), Ok(Some(120)));
        test_ascii_invoke!(Some(String::from("a")), Ok(Some(97)));
        test_ascii_invoke!(Some(String::from("")), Ok(Some(0)));
        test_ascii_invoke!(None, Ok(None));
        Ok(())
    }

    #[test]
    fn test_ascii_expr() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef;
        let expected = Arc::new(Int32Array::from(vec![Some(120)])) as ArrayRef;
        let result = ascii_array(input)?;
        assert_eq!(&expected, &result);

        let input = ScalarValue::Utf8(Some(String::from("x")));
        let expected = ScalarValue::Int32(Some(120));
        let result = ascii_scalar(input)?;
        assert_eq!(&expected, &result);

        let input = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        let expected = Arc::new(Int32Array::from(vec![Some(50)])) as ArrayRef;
        let result = ascii_array(input)?;
        assert_eq!(&expected, &result);

        let input = ScalarValue::Int32(Some(2));
        let expected = ScalarValue::Int32(Some(50));
        let result = ascii_scalar(input)?;
        assert_eq!(&expected, &result);

        Ok(())
    }
}

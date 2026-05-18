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

use crate::string::common::to_lower;
use datafusion_common::Result;
use datafusion_common::types::logical_string;
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Converts a string to lower-case.",
    syntax_example = "lower(str)",
    sql_example = r#"```sql
> select lower('Ångström');
+-------------------------+
| lower(Utf8("Ångström")) |
+-------------------------+
| ångström                |
+-------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "initcap"),
    related_udf(name = "upper")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LowerFunc {
    signature: Signature,
}

impl Default for LowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LowerFunc {
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

impl ScalarUDFImpl for LowerFunc {
    fn name(&self) -> &str {
        "lower"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        to_lower(&args.args, "lower")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, StringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

    fn invoke_lower(input: ArrayRef) -> Result<ArrayRef> {
        let func = LowerFunc::new();
        let data_type = input.data_type().clone();
        let args = ScalarFunctionArgs {
            number_rows: input.len(),
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Field::new("a", data_type.clone(), true).into()],
            return_field: Field::new("f", data_type, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        match func.invoke_with_args(args)? {
            ColumnarValue::Array(r) => Ok(r),
            _ => unreachable!("lower"),
        }
    }

    fn to_lower(input: ArrayRef, expected: ArrayRef) -> Result<()> {
        let result = invoke_lower(input)?;
        assert_eq!(&expected, &result);
        Ok(())
    }

    #[test]
    fn lower_maybe_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_full_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_partial_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὈΔΥΣΣΕΎΣ"),
            Some("TSCHÜSS"),
            Some("Ⱦ"), // ⱦ: length change
            Some("农历新年"),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὀδυσσεύς"),
            Some("tschüss"),
            Some("ⱦ"),
            Some("农历新年"),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_utf8view() -> Result<()> {
        let input = Arc::new(StringViewArray::from(vec![
            Some("ARROW"),
            None,
            Some("TSCHÜSS"),
        ])) as ArrayRef;

        let expected = Arc::new(StringViewArray::from(vec![
            Some("arrow"),
            None,
            Some("tschüss"),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_ascii_utf8view() -> Result<()> {
        // Mix of inlined (≤12 bytes) and referenced (>12 bytes) strings, plus
        // a null and an empty, to exercise the all-ASCII Utf8View fast path.
        let input = Arc::new(StringViewArray::from(vec![
            Some("ARROW"), // inlined short
            None,
            Some("HELLO WORLD 123"), // referenced (15 bytes)
            Some(""),
            Some("0123456789"),         // inlined, no case change
            Some("DATAFUSION IS COOL"), // referenced
        ])) as ArrayRef;

        let expected = Arc::new(StringViewArray::from(vec![
            Some("arrow"),
            None,
            Some("hello world 123"),
            Some(""),
            Some("0123456789"),
            Some("datafusion is cool"),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_sliced_ascii_utf8view() -> Result<()> {
        // Slice of a parent that contains a non-ASCII string outside the
        // slice. The slice is all-ASCII, so the fast path must run and produce
        // correct output while the parent's unaddressed non-ASCII bytes are
        // irrelevant to the result.
        let parent = Arc::new(StringViewArray::from(vec![
            Some("农历新年LONG ENOUGH FOR BUFFER"),
            Some("HELLO WORLD 123"),
            Some("DATAFUSION ROCKS!"),
            Some("ZZZZZZZZZZZZZZZZ"),
        ])) as ArrayRef;
        let sliced = parent.slice(1, 2);
        let result = invoke_lower(sliced)?;
        let result_sv = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        let expected = StringViewArray::from(vec![
            Some("hello world 123"),
            Some("datafusion rocks!"),
        ]);
        assert_eq!(result_sv, &expected);
        // The slice's two long views address 15 + 17 = 32 bytes; the ASCII
        // fast path must produce a single packed buffer of exactly that
        // size, not one scaled to the parent's data buffer.
        assert_eq!(result_sv.data_buffers().len(), 1);
        assert_eq!(result_sv.data_buffers()[0].len(), 32);
        Ok(())
    }

    #[test]
    fn lower_utf8view_inline_only_no_buffers() -> Result<()> {
        // An array whose values are all ≤ 12 bytes is fully inline; the ASCII
        // fast path should produce no data buffers at all.
        let input = Arc::new(StringViewArray::from(vec![
            Some("HELLO"),
            None,
            Some(""),
            Some("0123456789ab"), // 12 bytes — inline boundary
        ])) as ArrayRef;
        let result = invoke_lower(input)?;
        let result_sv = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        let expected = StringViewArray::from(vec![
            Some("hello"),
            None,
            Some(""),
            Some("0123456789ab"),
        ]);
        assert_eq!(result_sv, &expected);
        assert_eq!(
            result_sv.data_buffers().len(),
            0,
            "inline-only Utf8View should produce no data buffers"
        );
        Ok(())
    }

    #[test]
    fn lower_utf8view_long_packs_tight() -> Result<()> {
        // Mix of long and inline values; the long values should be packed into
        // a single tight output buffer whose size is exactly the sum of their
        // lengths (inline values do not contribute).
        let input = Arc::new(StringViewArray::from(vec![
            Some("HELLO WORLD 123"), // 15 bytes (long)
            Some("ABC"),             // inline
            None,
            Some("DATAFUSION ROCKS!"),   // 17 bytes (long)
            Some("ANOTHER LONG STRING"), // 19 bytes (long)
        ])) as ArrayRef;
        let result = invoke_lower(input)?;
        let result_sv = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        let expected = StringViewArray::from(vec![
            Some("hello world 123"),
            Some("abc"),
            None,
            Some("datafusion rocks!"),
            Some("another long string"),
        ]);
        assert_eq!(result_sv, &expected);
        assert_eq!(result_sv.data_buffers().len(), 1);
        assert_eq!(result_sv.data_buffers()[0].len(), 15 + 17 + 19);
        Ok(())
    }

    #[test]
    fn lower_utf8view_splits_into_multiple_buffers() -> Result<()> {
        // Produce enough long-string output to overflow the first data block
        // (≈16 KiB after the initial doubling) and confirm the fast path
        // splits across buffers rather than packing everything into one and
        // risking the i32::MAX offset limit.
        const STR_LEN: usize = 500;
        const N: usize = 40; // 40 × 500 B = 20 KiB total — crosses the first block.
        let value = "X".repeat(STR_LEN);
        let inputs: Vec<Option<String>> = (0..N).map(|_| Some(value.clone())).collect();
        let input = Arc::new(StringViewArray::from(inputs.clone())) as ArrayRef;
        let result = invoke_lower(input)?;
        let result_sv = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        let expected_value = "x".repeat(STR_LEN);
        let expected: Vec<Option<&str>> =
            (0..N).map(|_| Some(expected_value.as_str())).collect();
        assert_eq!(result_sv, &StringViewArray::from(expected));
        assert!(
            result_sv.data_buffers().len() >= 2,
            "expected the output to span more than one data buffer, got {}",
            result_sv.data_buffers().len()
        );
        // Total bytes across buffers must equal total long-value bytes
        // (no row was inlined since each value is > 12 bytes).
        let total: usize = result_sv.data_buffers().iter().map(|b| b.len()).sum();
        assert_eq!(total, N * STR_LEN);
        Ok(())
    }

    #[test]
    fn lower_sliced_utf8() -> Result<()> {
        let parent = Arc::new(StringArray::from(vec![
            Some("AAAAAAAA"),
            Some("HELLO"),
            Some("WORLD"),
            Some(""),
            Some("ZZZZZZZZ"),
        ])) as ArrayRef;
        let sliced = parent.slice(1, 3);
        let result = invoke_lower(sliced)?;
        let result_sa = result.as_any().downcast_ref::<StringArray>().unwrap();

        let expected = StringArray::from(vec![Some("hello"), Some("world"), Some("")]);
        assert_eq!(result_sa, &expected);
        // The slice's addressed bytes are "HELLO" + "WORLD" = 10; the ASCII
        // fast path must produce a tight output buffer (not the parent's).
        assert_eq!(result_sa.value_data().len(), 10);
        Ok(())
    }
}

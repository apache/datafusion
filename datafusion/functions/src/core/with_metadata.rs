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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Attaches Arrow field metadata (key/value pairs) to the input expression. Keys must be non-empty constant strings and values must be constant strings (empty values are allowed). Existing metadata on the input field is preserved; new keys overwrite on collision. This is the inverse of `arrow_metadata`.",
    syntax_example = "with_metadata(expression, key1, value1[, key2, value2, ...])",
    sql_example = r#"```sql
> select arrow_metadata(with_metadata(column1, 'unit', 'ms'), 'unit') from (values (1));
+---------------------------------------------------------------+
| arrow_metadata(with_metadata(column1,Utf8("unit"),Utf8("ms")),Utf8("unit")) |
+---------------------------------------------------------------+
| ms                                                            |
+---------------------------------------------------------------+
> select arrow_metadata(with_metadata(column1, 'unit', 'ms', 'source', 'sensor')) from (values (1));
+--------------------------+
| {source: sensor, unit: ms} |
+--------------------------+
```"#,
    argument(
        name = "expression",
        description = "The expression whose output Arrow field should be annotated. Values flow through unchanged."
    ),
    argument(
        name = "key",
        description = "Metadata key. Must be a non-empty constant string literal."
    ),
    argument(
        name = "value",
        description = "Metadata value. Must be a constant string literal (may be empty)."
    )
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WithMetadataFunc {
    signature: Signature,
}

impl Default for WithMetadataFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl WithMetadataFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for WithMetadataFunc {
    fn name(&self) -> &str {
        "with_metadata"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "with_metadata: return_type called instead of return_field_from_args"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Require at least the value expression plus one (key, value) pair,
        // and an odd total (1 + 2*N).
        if args.arg_fields.len() < 3 {
            return exec_err!(
                "with_metadata requires the input expression plus at least one (key, value) pair (minimum 3 arguments), got {}",
                args.arg_fields.len()
            );
        }
        if args.arg_fields.len().is_multiple_of(2) {
            return exec_err!(
                "with_metadata requires an odd number of arguments (expression followed by key/value pairs), got {}",
                args.arg_fields.len()
            );
        }

        let input_field = &args.arg_fields[0];
        let mut metadata = input_field.metadata().clone();

        // Keys are at indices 1, 3, 5, ...; values at 2, 4, 6, ...
        for pair_idx in 0..((args.scalar_arguments.len() - 1) / 2) {
            let key_idx = 1 + pair_idx * 2;
            let value_idx = key_idx + 1;

            let key = args.scalar_arguments[key_idx]
                .and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "with_metadata requires argument {key_idx} (key) to be a non-empty constant string"
                    ))
                })?;

            let value = args.scalar_arguments[value_idx]
                .and_then(|sv| sv.try_as_str().flatten())
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "with_metadata requires argument {value_idx} (value) to be a constant string"
                    ))
                })?;

            metadata.insert(key.to_string(), value.to_string());
        }

        // Preserve the input field's name, data type, and nullability; only the
        // metadata changes. This makes `with_metadata(col, ...)` a true
        // pass-through annotation from a schema perspective.
        let field = Field::new(
            input_field.name(),
            input_field.data_type().clone(),
            input_field.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field.into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Pure value pass-through. The metadata was attached to the return
        // field during planning and flows through record batch schemas; the
        // physical operator does not need to rebuild arrays.
        Ok(args.args[0].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    fn field(name: &str, dt: DataType, nullable: bool) -> FieldRef {
        Arc::new(Field::new(name, dt, nullable))
    }

    fn str_lit(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.to_string()))
    }

    #[test]
    fn attaches_single_key() {
        let udf = WithMetadataFunc::new();
        let input = field("my_col", DataType::Int32, true);
        let k = str_lit("unit");
        let v = str_lit("ms");
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, Some(&k), Some(&v)];
        let ret = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap();
        assert_eq!(ret.name(), "my_col");
        assert_eq!(ret.data_type(), &DataType::Int32);
        assert!(ret.is_nullable());
        assert_eq!(ret.metadata().get("unit").map(String::as_str), Some("ms"));
    }

    #[test]
    fn merges_existing_metadata_and_overwrites_on_collision() {
        let udf = WithMetadataFunc::new();
        let mut existing = Field::new("x", DataType::Float64, false);
        existing.set_metadata(
            [
                ("keep".to_string(), "yes".to_string()),
                ("unit".to_string(), "old".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        let input: FieldRef = Arc::new(existing);
        let k = str_lit("unit");
        let v = str_lit("new");
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, Some(&k), Some(&v)];
        let ret = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap();
        assert_eq!(ret.name(), "x");
        assert!(!ret.is_nullable());
        assert_eq!(ret.metadata().get("keep").map(String::as_str), Some("yes"));
        assert_eq!(ret.metadata().get("unit").map(String::as_str), Some("new"));
    }

    #[test]
    fn multiple_pairs() {
        let udf = WithMetadataFunc::new();
        let input = field("c", DataType::Utf8, true);
        let k1 = str_lit("a");
        let v1 = str_lit("1");
        let k2 = str_lit("b");
        let v2 = str_lit("2");
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, Some(&k1), Some(&v1), Some(&k2), Some(&v2)];
        let ret = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap();
        assert_eq!(ret.metadata().get("a").map(String::as_str), Some("1"));
        assert_eq!(ret.metadata().get("b").map(String::as_str), Some("2"));
    }

    #[test]
    fn rejects_even_arity() {
        let udf = WithMetadataFunc::new();
        let input = field("c", DataType::Int32, true);
        let a = str_lit("a");
        let b = str_lit("b");
        let c = str_lit("c");
        // 4 args total: input + 3 literals (odd key count)
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, Some(&a), Some(&b), Some(&c)];
        let err = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap_err();
        assert!(err.to_string().contains("odd number"));
    }

    #[test]
    fn rejects_too_few_args() {
        let udf = WithMetadataFunc::new();
        let input = field("c", DataType::Int32, true);
        let k = str_lit("a");
        let fields = [Arc::clone(&input), field("", DataType::Utf8, false)];
        let scalars = [None, Some(&k)];
        let err = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap_err();
        assert!(err.to_string().contains("at least one"));
    }

    #[test]
    fn allows_empty_value() {
        let udf = WithMetadataFunc::new();
        let input = field("c", DataType::Int32, true);
        let k = str_lit("unit");
        let v = str_lit("");
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, false),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, Some(&k), Some(&v)];
        let ret = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap();
        assert_eq!(ret.metadata().get("unit").map(String::as_str), Some(""));
    }

    #[test]
    fn rejects_non_literal_key() {
        let udf = WithMetadataFunc::new();
        let input = field("c", DataType::Int32, true);
        let v = str_lit("v");
        let fields = [
            Arc::clone(&input),
            field("", DataType::Utf8, true),
            field("", DataType::Utf8, false),
        ];
        let scalars = [None, None, Some(&v)];
        let err = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            })
            .unwrap_err();
        assert!(err.to_string().contains("non-empty constant string"));
    }
}

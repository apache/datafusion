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

use std::{collections::HashMap, sync::Arc};

use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{logical_int64, logical_string};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use itertools::izip;
use regex::Regex;

use crate::regex::{compile_and_cache_regex, compile_regex};
use crate::utils::make_scalar_function;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extract the string that matches the regular expression in the specified group.",
    syntax_example = "regexp_extract(str, regexp[, idx])",
    sql_example = r#"```sql
> SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
+---------------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\d+)-(\d+)"),Int64(1)) |
+---------------------------------------------------------------+
| 100                                                           |
+---------------------------------------------------------------+
> SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
+---------------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\d+)-(\d+)"),Int64(2)) |
+---------------------------------------------------------------+
| 200                                                           |
+---------------------------------------------------------------+
```
"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    argument(
        name = "idx",
        description = "- **idx**: Optional index of the regular expression group to extract (index 0 is the implicit group. Can be a constant, column, or function. Defaults to 1."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let return_type = match args.arg_fields[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => DataType::Utf8,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            other => return exec_err!("unsupported value type {other:?}"),
        };
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be implemented")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use datafusion_expr::function::Hint::*;
        let hints = vec![Pad, AcceptsSingular, AcceptsSingular];
        make_scalar_function(regexp_extract, hints)(&args.args)
    }
}

fn regexp_extract(args: &[ArrayRef]) -> Result<ArrayRef> {
    let idx_opts = match args.get(2) {
        Some(idxs) => as_int64_array(idxs)?,
        None => &Int64Array::from_value(1, 1),
    };
    let value_str_type = args[0].data_type();
    let pattern_str_type = args[1].data_type();
    match (value_str_type, pattern_str_type) {
        (DataType::Utf8, DataType::Utf8) => {
            let value_opts = args[0].as_string::<i32>();
            let pattern_opts = args[1].as_string::<i32>();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::Utf8, DataType::Utf8View) => {
            let value_opts = args[0].as_string::<i32>();
            let pattern_opts = args[1].as_string_view();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            let value_opts = args[0].as_string::<i32>();
            let pattern_opts = args[1].as_string::<i64>();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            let value_opts = args[0].as_string::<i64>();
            let pattern_opts = args[1].as_string::<i32>();
            regexp_extract_impl::<i64>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::LargeUtf8, DataType::Utf8View) => {
            let value_opts = args[0].as_string::<i64>();
            let pattern_opts = args[1].as_string_view();
            regexp_extract_impl::<i64>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let value_opts = args[0].as_string::<i64>();
            let pattern_opts = args[1].as_string::<i64>();
            regexp_extract_impl::<i64>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::Utf8View, DataType::Utf8View) => {
            let value_opts = args[0].as_string_view();
            let pattern_opts = args[1].as_string_view();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::Utf8View, DataType::Utf8) => {
            let value_opts = args[0].as_string_view();
            let pattern_opts = args[1].as_string::<i32>();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        (DataType::Utf8View, DataType::LargeUtf8) => {
            let value_opts = args[0].as_string_view();
            let pattern_opts = args[1].as_string::<i64>();
            regexp_extract_impl::<i32>(&value_opts, &pattern_opts, idx_opts)
        }
        other => {
            exec_err!("unsupported data types {other:?} for regexp_extract")
        }
    }
}

fn regexp_extract_impl<'a, 'b, OffsetSize: OffsetSizeTrait>(
    value_opts: &impl StringArrayType<'a>,
    pattern_opts: &impl StringArrayType<'b>,
    idx_opts: &Int64Array,
) -> Result<ArrayRef> {
    let mut results = GenericStringBuilder::<OffsetSize>::new();
    if pattern_opts.len() == 1 {
        // Scalar pattern.
        if pattern_opts.is_null(0) {
            return Ok(Arc::new(GenericStringArray::<OffsetSize>::new_null(
                value_opts.len(),
            )));
        }
        let compiled_pattern = compile_regex(pattern_opts.value(0), None)?;
        for (value_opt, idx_opt) in izip!(value_opts.iter(), idx_opts.iter().cycle()) {
            if let (Some(value), Some(idx)) = (value_opt, idx_opt) {
                let result = do_regexp_extract(value, &compiled_pattern, idx)?;
                results.append_value(result);
            } else {
                results.append_null();
            }
        }
    } else {
        // Non-scalar pattern.
        let mut regex_cache = HashMap::new();
        for (value_opt, pattern_opt, idx_opt) in izip!(
            value_opts.iter(),
            pattern_opts.iter(),
            idx_opts.iter().cycle()
        ) {
            if let (Some(value), Some(pattern), Some(idx)) =
                (value_opt, pattern_opt, idx_opt)
            {
                let compiled_pattern =
                    compile_and_cache_regex(pattern, None, &mut regex_cache)?;
                let result = do_regexp_extract(value, compiled_pattern, idx)?;
                results.append_value(result);
            } else {
                results.append_null();
            }
        }
    }
    Ok(Arc::new(results.finish()))
}

fn do_regexp_extract<'a>(value: &'a str, pattern: &Regex, idx: i64) -> Result<&'a str> {
    let Ok(idx) = usize::try_from(idx) else {
        return exec_err!("regexp_extract idx {idx} cannot be converted to usize");
    };
    if idx >= pattern.captures_len() {
        return exec_err!(
            "regexp_extract idx is {idx} but there are only {} capture groups",
            pattern.captures_len()
        );
    }
    let match_opt = match idx {
        0 => pattern.find(value), // Fast path when implicit group is used.
        _ => pattern
            .captures(value)
            .and_then(|captures| captures.get(idx)),
    };
    Ok(match_opt.map_or("", |m| m.as_str()))
}

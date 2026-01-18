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
    Array, ArrayBuilder, ArrayRef, AsArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::{Field, FieldRef};
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{
    NativeType, logical_int32, logical_int64, logical_string,
};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{Coercion, ReturnFieldArgs, TypeSignatureClass};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::unicode::substr::{enable_ascii_fast_path, get_true_start_end};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `substring` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#substring>
///
/// Returns the substring from string starting at position pos with length len.
/// Position is 1-indexed. If pos is negative, it counts from the end of the string.
/// Returns NULL if any input is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSubstring {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkSubstring {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSubstring {
    pub fn new() -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        let int64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Native(logical_int32())],
            NativeType::Int64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![string.clone(), int64.clone()]),
                    TypeSignature::Coercible(vec![
                        string.clone(),
                        int64.clone(),
                        int64.clone(),
                    ]),
                ],
                Volatility::Immutable,
            )
            .with_parameter_names(vec![
                "str".to_string(),
                "pos".to_string(),
                "length".to_string(),
            ])
            .expect("valid parameter names"),
            aliases: vec![String::from("substr")],
        }
    }
}

impl ScalarUDFImpl for SparkSubstring {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_substring, vec![])(&args.args)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called for Spark substring"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        // Spark semantics: substring returns NULL if ANY input is NULL
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            "substring",
            args.arg_fields[0].data_type().clone(),
            nullable,
        )))
    }
}

fn spark_substring(args: &[ArrayRef]) -> Result<ArrayRef> {
    let start_array = as_int64_array(&args[1])?;
    let length_array = if args.len() > 2 {
        Some(as_int64_array(&args[2])?)
    } else {
        None
    };

    match args[0].data_type() {
        DataType::Utf8 => spark_substring_impl(
            &args[0].as_string::<i32>(),
            start_array,
            length_array,
            GenericStringBuilder::<i32>::new(),
        ),
        DataType::LargeUtf8 => spark_substring_impl(
            &args[0].as_string::<i64>(),
            start_array,
            length_array,
            GenericStringBuilder::<i64>::new(),
        ),
        DataType::Utf8View => spark_substring_impl(
            &args[0].as_string_view(),
            start_array,
            length_array,
            StringViewBuilder::new(),
        ),
        other => exec_err!(
            "Unsupported data type {other:?} for function spark_substring, expected Utf8View, Utf8 or LargeUtf8."
        ),
    }
}

/// Convert Spark's start position to DataFusion's 1-based start position.
///
/// Spark semantics:
/// - Positive start: 1-based index from beginning
/// - Zero start: treated as 1
/// - Negative start: counts from end of string
///
/// Returns the converted 1-based start position for use with `get_true_start_end`.
#[inline]
fn spark_start_to_datafusion_start(start: i64, len: usize) -> i64 {
    if start >= 0 {
        start.max(1)
    } else {
        let len_i64 = i64::try_from(len).unwrap_or(i64::MAX);
        let start = start.saturating_add(len_i64).saturating_add(1);
        start.max(1)
    }
}

trait StringArrayBuilder: ArrayBuilder {
    fn append_value(&mut self, val: &str);
    fn append_null(&mut self);
}

impl<O: OffsetSizeTrait> StringArrayBuilder for GenericStringBuilder<O> {
    fn append_value(&mut self, val: &str) {
        GenericStringBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        GenericStringBuilder::append_null(self);
    }
}

impl StringArrayBuilder for StringViewBuilder {
    fn append_value(&mut self, val: &str) {
        StringViewBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        StringViewBuilder::append_null(self);
    }
}

fn spark_substring_impl<'a, V, B>(
    string_array: &V,
    start_array: &Int64Array,
    length_array: Option<&Int64Array>,
    mut builder: B,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    B: StringArrayBuilder,
{
    let is_ascii = enable_ascii_fast_path(string_array, start_array, length_array);

    for i in 0..string_array.len() {
        if string_array.is_null(i) || start_array.is_null(i) {
            builder.append_null();
            continue;
        }

        if let Some(len_arr) = length_array
            && len_arr.is_null(i)
        {
            builder.append_null();
            continue;
        }

        let string = string_array.value(i);
        let start = start_array.value(i);
        let len_opt = length_array.map(|arr| arr.value(i));

        // Spark: negative length returns empty string
        if let Some(len) = len_opt
            && len < 0
        {
            builder.append_value("");
            continue;
        }

        let string_len = if is_ascii {
            string.len()
        } else {
            string.chars().count()
        };

        let adjusted_start = spark_start_to_datafusion_start(start, string_len);

        let (byte_start, byte_end) = get_true_start_end(
            string,
            adjusted_start,
            len_opt.map(|l| l as u64),
            is_ascii,
        );
        let substr = &string[byte_start..byte_end];
        builder.append_value(substr);
    }

    Ok(builder.finish())
}

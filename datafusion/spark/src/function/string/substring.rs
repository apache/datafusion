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
    Array, ArrayAccessor, ArrayBuilder, ArrayRef, AsArray, BinaryViewBuilder,
    GenericBinaryBuilder, GenericStringBuilder, Int64Array, OffsetSizeTrait,
    StringViewBuilder,
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
        let binary = Coercion::new_exact(TypeSignatureClass::Binary);
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
                    TypeSignature::Coercible(vec![binary.clone(), int64.clone()]),
                    TypeSignature::Coercible(vec![
                        binary.clone(),
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
        DataType::Utf8 => {
            let array = args[0].as_string::<i32>();
            let is_ascii = enable_ascii_fast_path(&array, start_array, length_array);
            spark_substring_generic(
                &array,
                start_array,
                length_array,
                GenericStringBuilder::<i32>::new(),
                is_ascii,
            )
        }
        DataType::LargeUtf8 => {
            let array = args[0].as_string::<i64>();
            let is_ascii = enable_ascii_fast_path(&array, start_array, length_array);
            spark_substring_generic(
                &array,
                start_array,
                length_array,
                GenericStringBuilder::<i64>::new(),
                is_ascii,
            )
        }
        DataType::Utf8View => {
            let array = args[0].as_string_view();
            let is_ascii = enable_ascii_fast_path(&array, start_array, length_array);
            spark_substring_generic(
                &array,
                start_array,
                length_array,
                StringViewBuilder::new(),
                is_ascii,
            )
        }
        // Binary paths always use byte-level indexing, so `is_ascii` is irrelevant
        // and set to `true` (its value is ignored by the `[u8]` impl of
        // `SubstringItem`).
        DataType::Binary => spark_substring_generic(
            &args[0].as_binary::<i32>(),
            start_array,
            length_array,
            GenericBinaryBuilder::<i32>::new(),
            true,
        ),
        DataType::LargeBinary => spark_substring_generic(
            &args[0].as_binary::<i64>(),
            start_array,
            length_array,
            GenericBinaryBuilder::<i64>::new(),
            true,
        ),
        DataType::BinaryView => spark_substring_generic(
            &args[0].as_binary_view(),
            start_array,
            length_array,
            BinaryViewBuilder::new(),
            true,
        ),
        other => exec_err!(
            "Unsupported data type {other:?} for function spark_substring, expected Utf8View, Utf8, LargeUtf8, Binary, LargeBinary or BinaryView."
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
/// The result may be `<= 0` when a negative start lands before the string
/// (e.g. `start=-10` on a 3-char string gives `-6`). Such values are passed
/// through to `get_true_start_end`, which clamps them and yields an empty
/// slice — matching Spark's behavior for out-of-range negative positions.
#[inline]
fn spark_start_to_datafusion_start(start: i64, len: usize) -> i64 {
    if start >= 0 {
        start.max(1)
    } else {
        let len_i64 = i64::try_from(len).unwrap_or(i64::MAX);
        start + len_i64 + 1
    }
}

trait SubstringItem {
    /// Length used for Spark's negative-position adjustment.
    /// For `str` this is characters (or bytes in ASCII mode); for `[u8]` it is
    /// always byte count.
    fn positional_len(&self, is_ascii: bool) -> usize;

    /// Converts Spark's 1-indexed adjusted start + optional length into a
    /// byte range clamped to `[0, byte_len]`.
    fn byte_range(
        &self,
        adjusted_start: i64,
        len: Option<i64>,
        is_ascii: bool,
    ) -> Result<(usize, usize)>;

    fn byte_slice(&self, start: usize, end: usize) -> &Self;
}

impl SubstringItem for str {
    fn positional_len(&self, is_ascii: bool) -> usize {
        if is_ascii {
            self.len()
        } else {
            self.chars().count()
        }
    }

    fn byte_range(
        &self,
        adjusted_start: i64,
        len: Option<i64>,
        is_ascii: bool,
    ) -> Result<(usize, usize)> {
        get_true_start_end(self, adjusted_start, len, is_ascii)
    }

    fn byte_slice(&self, start: usize, end: usize) -> &Self {
        &self[start..end]
    }
}

impl SubstringItem for [u8] {
    fn positional_len(&self, _is_ascii: bool) -> usize {
        self.len()
    }

    fn byte_range(
        &self,
        adjusted_start: i64,
        len: Option<i64>,
        _is_ascii: bool,
    ) -> Result<(usize, usize)> {
        let byte_len = self.len();
        let start0 = adjusted_start.saturating_sub(1);
        let end0 = match len {
            Some(l) => start0.saturating_add(l),
            None => byte_len as i64,
        };
        let byte_len_i64 = byte_len as i64;
        Ok((
            start0.clamp(0, byte_len_i64) as usize,
            end0.clamp(0, byte_len_i64) as usize,
        ))
    }

    fn byte_slice(&self, start: usize, end: usize) -> &Self {
        &self[start..end]
    }
}

trait SubstringBuilder: ArrayBuilder {
    type Item: SubstringItem + ?Sized;
    fn append_value(&mut self, val: &Self::Item);
    fn append_null(&mut self);
    /// Spark's semantic "empty" for this builder's item type, used for the
    /// negative-length short-circuit.
    fn append_empty(&mut self);
}

impl<O: OffsetSizeTrait> SubstringBuilder for GenericStringBuilder<O> {
    type Item = str;
    fn append_value(&mut self, val: &str) {
        GenericStringBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        GenericStringBuilder::append_null(self);
    }
    fn append_empty(&mut self) {
        GenericStringBuilder::append_value(self, "");
    }
}

impl SubstringBuilder for StringViewBuilder {
    type Item = str;
    fn append_value(&mut self, val: &str) {
        StringViewBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        StringViewBuilder::append_null(self);
    }
    fn append_empty(&mut self) {
        StringViewBuilder::append_value(self, "");
    }
}

impl<O: OffsetSizeTrait> SubstringBuilder for GenericBinaryBuilder<O> {
    type Item = [u8];
    fn append_value(&mut self, val: &[u8]) {
        GenericBinaryBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        GenericBinaryBuilder::append_null(self);
    }
    fn append_empty(&mut self) {
        GenericBinaryBuilder::append_value(self, &[]);
    }
}

impl SubstringBuilder for BinaryViewBuilder {
    type Item = [u8];
    fn append_value(&mut self, val: &[u8]) {
        BinaryViewBuilder::append_value(self, val);
    }
    fn append_null(&mut self) {
        BinaryViewBuilder::append_null(self);
    }
    fn append_empty(&mut self) {
        BinaryViewBuilder::append_value(self, []);
    }
}

/// Unified implementation of Spark's `substring`, generic over the source
/// array (`StringArrayType`/`BinaryArrayType` via `ArrayAccessor`) and its
/// corresponding builder. Per-row indexing semantics are delegated to
/// [`SubstringItem`], which differs between `str` (char-aware when
/// `is_ascii` is false) and `[u8]` (always byte-level).
fn spark_substring_generic<'a, Source, Item, Builder>(
    array: &Source,
    start_array: &Int64Array,
    length_array: Option<&Int64Array>,
    mut builder: Builder,
    is_ascii: bool,
) -> Result<ArrayRef>
where
    Source: ArrayAccessor<Item = &'a Item>,
    Item: SubstringItem + ?Sized + 'a,
    Builder: SubstringBuilder<Item = Item>,
{
    for i in 0..array.len() {
        if array.is_null(i) || start_array.is_null(i) {
            builder.append_null();
            continue;
        }

        if let Some(len_arr) = length_array
            && len_arr.is_null(i)
        {
            builder.append_null();
            continue;
        }

        let value = array.value(i);
        let start = start_array.value(i);
        let len_opt = length_array.map(|arr| arr.value(i));

        // Spark: negative length yields an empty value
        if let Some(len) = len_opt
            && len < 0
        {
            builder.append_empty();
            continue;
        }

        let positional_len = value.positional_len(is_ascii);
        let adjusted_start = spark_start_to_datafusion_start(start, positional_len);
        let (byte_start, byte_end) =
            value.byte_range(adjusted_start, len_opt, is_ascii)?;
        builder.append_value(value.byte_slice(byte_start, byte_end));
    }

    Ok(builder.finish())
}

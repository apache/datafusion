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

//! [`ScalarUDFImpl`] definitions for array_to_string and string_to_array functions.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericListArray,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, ListBuilder,
    OffsetSizeTrait, StringArray, StringBuilder, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field};

use datafusion_common::utils::ListCoercion;
use datafusion_common::{DataFusionError, Result, ScalarValue, not_impl_err};

use std::fmt::{self, Write};

use crate::utils::make_scalar_function;
use arrow::array::{
    StringArrayType, StringViewArray,
    builder::{ArrayBuilder, LargeStringBuilder, StringViewBuilder},
    cast::AsArray,
};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::DataType::{
    Dictionary, FixedSizeList, LargeList, LargeUtf8, List, Null, Utf8, Utf8View,
};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, Coercion, ColumnarValue,
    Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_macros::user_doc;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(
    ArrayToString,
    array_to_string,
    array delimiter, // arg name
    "converts each element to its text representation.", // doc
    array_to_string_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Converts each element to its text representation.",
    syntax_example = "array_to_string(array, delimiter[, null_string])",
    sql_example = r#"```sql
> select array_to_string([[1, 2, 3, 4], [5, 6, 7, 8]], ',');
+----------------------------------------------------+
| array_to_string(List([1,2,3,4,5,6,7,8]),Utf8(",")) |
+----------------------------------------------------+
| 1,2,3,4,5,6,7,8                                    |
+----------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "delimiter", description = "Array element separator."),
    argument(
        name = "null_string",
        description = "Optional. String to use for null values in the output. If not provided, nulls will be omitted."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayToString {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayToString {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayToString {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::String,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("list_to_string"),
                String::from("array_join"),
                String::from("list_join"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayToString {
    fn name(&self) -> &str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_to_string_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    StringToArray,
    string_to_array,
    string delimiter null_string, // arg name
    "splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`", // doc
    string_to_array_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Splits a string into an array of substrings based on a delimiter. Any substrings matching the optional `null_str` argument are replaced with NULL.",
    syntax_example = "string_to_array(str, delimiter[, null_str])",
    sql_example = r#"```sql
> select string_to_array('abc##def', '##');
+-----------------------------------+
| string_to_array(Utf8('abc##def'))  |
+-----------------------------------+
| ['abc', 'def']                    |
+-----------------------------------+
> select string_to_array('abc def', ' ', 'def');
+---------------------------------------------+
| string_to_array(Utf8('abc def'), Utf8(' '), Utf8('def')) |
+---------------------------------------------+
| ['abc', NULL]                               |
+---------------------------------------------+
```"#,
    argument(name = "str", description = "String expression to split."),
    argument(name = "delimiter", description = "Delimiter string to split on."),
    argument(
        name = "null_str",
        description = "Substring values to be replaced with `NULL`."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StringToArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for StringToArray {
    fn default() -> Self {
        Self::new()
    }
}

impl StringToArray {
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
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("string_to_list")],
        }
    }
}

impl ScalarUDFImpl for StringToArray {
    fn name(&self) -> &str {
        "string_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let delimiter_is_scalar = matches!(&args[1], ColumnarValue::Scalar(_));
        let null_str_is_scalar = args
            .get(2)
            .is_none_or(|a| matches!(a, ColumnarValue::Scalar(_)));

        if !(delimiter_is_scalar && null_str_is_scalar) {
            return make_scalar_function(string_to_array_fallback)(&args);
        }

        // Delimiter and null_str (if given) are scalar, so use the fast path
        let delimiter = match &args[1] {
            ColumnarValue::Scalar(s) => s.try_as_str().ok_or_else(|| {
                exec_datafusion_err!(
                    "unsupported type for string_to_array delimiter: {:?}",
                    args[1].data_type()
                )
            })?,
            _ => unreachable!("delimiter must be scalar in this branch"),
        };
        let null_value = match args.get(2) {
            Some(ColumnarValue::Scalar(s)) => s.try_as_str().ok_or_else(|| {
                exec_datafusion_err!(
                    "unsupported type for string_to_array null_str: {:?}",
                    args[2].data_type()
                )
            })?,
            _ => None,
        };

        let (all_scalar, string_array) = match &args[0] {
            ColumnarValue::Array(a) => (false, Arc::clone(a)),
            ColumnarValue::Scalar(s) => (true, s.to_array_of_size(1)?),
        };

        let result = match string_array.data_type() {
            Utf8 => {
                let arr = string_array.as_string::<i32>();
                let builder =
                    StringBuilder::with_capacity(arr.len(), arr.get_buffer_memory_size());
                string_to_array_scalar_args(&arr, delimiter, null_value, builder)
            }
            Utf8View => {
                let arr = string_array.as_string_view();
                let builder = StringViewBuilder::with_capacity(arr.len());
                string_to_array_scalar_args(&arr, delimiter, null_value, builder)
            }
            LargeUtf8 => {
                let arr = string_array.as_string::<i64>();
                let builder = LargeStringBuilder::with_capacity(
                    arr.len(),
                    arr.get_buffer_memory_size(),
                );
                string_to_array_scalar_args(&arr, delimiter, null_value, builder)
            }
            other => {
                exec_err!("unsupported type for string_to_array function as {other:?}")
            }
        }?;

        if all_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Appends `value` to the string builder, or NULL if it matches `null_value`.
#[inline(always)]
fn append_part(
    builder: &mut impl StringArrayBuilderType,
    value: &str,
    null_value: Option<&str>,
) {
    if null_value == Some(value) {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

/// Optimized `string_to_array` implementation for the common case where
/// delimiter and null_value are scalar values.
fn string_to_array_scalar_args<'a, StringArrType, StringBuilderType>(
    string_array: &StringArrType,
    delimiter: Option<&str>,
    null_value: Option<&str>,
    string_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    StringBuilderType: StringArrayBuilderType,
{
    let mut list_builder = ListBuilder::new(string_builder);

    match delimiter {
        Some("") => {
            // Empty delimiter: each non-empty string becomes a single-element list.
            // Empty strings produce an empty array (PostgreSQL compat).
            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    list_builder.append(false);
                    continue;
                }
                let string = string_array.value(i);
                if !string.is_empty() {
                    append_part(list_builder.values(), string, null_value);
                }
                list_builder.append(true);
            }
        }
        Some(delimiter) => {
            // Rather than using `str::split`, do the split ourselves using
            // `memmem::Finder`. This allows pre-compiling the delimiter search
            // pattern once and reusing it for all rows.
            let finder = memchr::memmem::Finder::new(delimiter.as_bytes());
            let delim_len = delimiter.len();

            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    list_builder.append(false);
                    continue;
                }
                let string = string_array.value(i);
                if !string.is_empty() {
                    let bytes = string.as_bytes();
                    let mut start = 0;
                    for pos in finder.find_iter(bytes) {
                        append_part(
                            list_builder.values(),
                            &string[start..pos],
                            null_value,
                        );
                        start = pos + delim_len;
                    }
                    // Trailing part after last delimiter (or entire string if no
                    // delimiter was found).
                    append_part(list_builder.values(), &string[start..], null_value);
                }
                list_builder.append(true);
            }
        }
        None => {
            // NULL delimiter: split into individual characters.
            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    list_builder.append(false);
                    continue;
                }
                let string = string_array.value(i);
                for (pos, c) in string.char_indices() {
                    append_part(
                        list_builder.values(),
                        &string[pos..pos + c.len_utf8()],
                        null_value,
                    );
                }
                list_builder.append(true);
            }
        }
    }

    Ok(Arc::new(list_builder.finish()) as ArrayRef)
}

/// Fallback path for `string_to_array` when delimiter and/or null_value
/// are array columns rather than scalars.
fn string_to_array_fallback(args: &[ArrayRef]) -> Result<ArrayRef> {
    let null_value_array = args.get(2);

    match args[0].data_type() {
        Utf8 => {
            let arr = args[0].as_string::<i32>();
            let builder =
                StringBuilder::with_capacity(arr.len(), arr.get_buffer_memory_size());
            string_to_array_column_args(&arr, &args[1], null_value_array, builder)
        }
        Utf8View => {
            let arr = args[0].as_string_view();
            let builder = StringViewBuilder::with_capacity(arr.len());
            string_to_array_column_args(&arr, &args[1], null_value_array, builder)
        }
        LargeUtf8 => {
            let arr = args[0].as_string::<i64>();
            let builder = LargeStringBuilder::with_capacity(
                arr.len(),
                arr.get_buffer_memory_size(),
            );
            string_to_array_column_args(&arr, &args[1], null_value_array, builder)
        }
        other => exec_err!("unsupported type for string_to_array function as {other:?}"),
    }
}

fn string_to_array_column_args<'a, StringArrType, StringBuilderType>(
    string_array: &StringArrType,
    delimiter_array: &ArrayRef,
    null_value_array: Option<&ArrayRef>,
    string_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    StringBuilderType: StringArrayBuilderType,
{
    let mut list_builder = ListBuilder::new(string_builder);

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            list_builder.append(false);
            continue;
        }

        let string = string_array.value(i);
        let delimiter = get_str_value(delimiter_array, i);
        let null_value = null_value_array.and_then(|arr| get_str_value(arr, i));

        match delimiter {
            Some("") => {
                if !string.is_empty() {
                    append_part(list_builder.values(), string, null_value);
                }
            }
            Some(delimiter) => {
                if !string.is_empty() {
                    for part in string.split(delimiter) {
                        append_part(list_builder.values(), part, null_value);
                    }
                }
            }
            None => {
                for (pos, c) in string.char_indices() {
                    append_part(
                        list_builder.values(),
                        &string[pos..pos + c.len_utf8()],
                        null_value,
                    );
                }
            }
        }

        list_builder.append(true);
    }

    Ok(Arc::new(list_builder.finish()) as ArrayRef)
}

/// Returns the string value at index `i` from a string array of any type.
fn get_str_value(array: &ArrayRef, i: usize) -> Option<&str> {
    if array.is_null(i) {
        return None;
    }
    match array.data_type() {
        Utf8 => Some(array.as_string::<i32>().value(i)),
        LargeUtf8 => Some(array.as_string::<i64>().value(i)),
        Utf8View => Some(array.as_string_view().value(i)),
        other => {
            debug_assert!(false, "unexpected type in get_str_value: {other:?}");
            None
        }
    }
}

fn array_to_string_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters: Vec<Option<&str>> = match args[1].data_type() {
        Utf8 => args[1].as_string::<i32>().iter().collect(),
        Utf8View => args[1].as_string_view().iter().collect(),
        LargeUtf8 => args[1].as_string::<i64>().iter().collect(),
        other => {
            return exec_err!(
                "unsupported type for second argument to array_to_string function as {other:?}"
            );
        }
    };

    let null_strings: Vec<Option<&str>> = if args.len() == 3 {
        match args[2].data_type() {
            Utf8 => args[2].as_string::<i32>().iter().collect(),
            Utf8View => args[2].as_string_view().iter().collect(),
            LargeUtf8 => args[2].as_string::<i64>().iter().collect(),
            other => {
                return exec_err!(
                    "unsupported type for third argument to array_to_string function as {other:?}"
                );
            }
        }
    } else {
        // If `null_strings` is not specified, we treat it as equivalent to
        // explicitly passing a NULL value for `null_strings` in every row.
        vec![None; args[0].len()]
    };

    let string_arr = match arr.data_type() {
        List(_) => {
            let list_array = as_list_array(&arr)?;
            generate_string_array::<i32>(list_array, &delimiters, &null_strings)?
        }
        LargeList(_) => {
            let list_array = as_large_list_array(&arr)?;
            generate_string_array::<i64>(list_array, &delimiters, &null_strings)?
        }
        // Signature guards against this arm
        _ => return exec_err!("array_to_string expects list as first argument"),
    };

    Ok(Arc::new(string_arr))
}

fn generate_string_array<O: OffsetSizeTrait>(
    list_arr: &GenericListArray<O>,
    delimiters: &[Option<&str>],
    null_strings: &[Option<&str>],
) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(list_arr.len(), 0);

    for ((arr, &delimiter), &null_string) in list_arr
        .iter()
        .zip(delimiters.iter())
        .zip(null_strings.iter())
    {
        let (Some(arr), Some(delimiter)) = (arr, delimiter) else {
            builder.append_null();
            continue;
        };

        let mut first = true;
        compute_array_to_string(&mut builder, &arr, delimiter, null_string, &mut first)?;
        builder.append_value("");
    }

    Ok(builder.finish())
}

fn compute_array_to_string(
    w: &mut impl Write,
    arr: &ArrayRef,
    delimiter: &str,
    null_string: Option<&str>,
    first: &mut bool,
) -> Result<()> {
    // Handle lists by recursing on each list element.
    macro_rules! handle_list {
        ($list_array:expr) => {
            for i in 0..$list_array.len() {
                if !$list_array.is_null(i) {
                    compute_array_to_string(
                        w,
                        &$list_array.value(i),
                        delimiter,
                        null_string,
                        first,
                    )?;
                } else if let Some(ns) = null_string {
                    if *first {
                        *first = false;
                    } else {
                        w.write_str(delimiter)?;
                    }
                    w.write_str(ns)?;
                }
            }
        };
    }

    match arr.data_type() {
        List(..) => {
            let list_array = as_list_array(arr)?;
            handle_list!(list_array);
            Ok(())
        }
        FixedSizeList(..) => {
            let list_array = as_fixed_size_list_array(arr)?;
            handle_list!(list_array);
            Ok(())
        }
        LargeList(..) => {
            let list_array = as_large_list_array(arr)?;
            handle_list!(list_array);
            Ok(())
        }
        Dictionary(_key_type, value_type) => {
            // Call cast to unwrap the dictionary. This could be optimized if we wanted
            // to accept the overhead of extra code
            let values = cast(arr, value_type.as_ref()).map_err(|e| {
                DataFusionError::from(e)
                    .context("Casting dictionary to values in compute_array_to_string")
            })?;
            compute_array_to_string(w, &values, delimiter, null_string, first)
        }
        Null => Ok(()),
        data_type => {
            macro_rules! str_leaf {
                ($ARRAY_TYPE:ident) => {
                    write_leaf_to_string(
                        w,
                        downcast_arg!(arr, $ARRAY_TYPE),
                        delimiter,
                        null_string,
                        first,
                        |w, x: &str| w.write_str(x),
                    )?
                };
            }
            macro_rules! bool_leaf {
                ($ARRAY_TYPE:ident) => {
                    write_leaf_to_string(
                        w,
                        downcast_arg!(arr, $ARRAY_TYPE),
                        delimiter,
                        null_string,
                        first,
                        |w, x: bool| {
                            if x {
                                w.write_str("true")
                            } else {
                                w.write_str("false")
                            }
                        },
                    )?
                };
            }
            macro_rules! int_leaf {
                ($ARRAY_TYPE:ident) => {
                    write_leaf_to_string(
                        w,
                        downcast_arg!(arr, $ARRAY_TYPE),
                        delimiter,
                        null_string,
                        first,
                        |w, x| {
                            let mut itoa_buf = itoa::Buffer::new();
                            w.write_str(itoa_buf.format(x))
                        },
                    )?
                };
            }
            macro_rules! float_leaf {
                ($ARRAY_TYPE:ident) => {
                    write_leaf_to_string(
                        w,
                        downcast_arg!(arr, $ARRAY_TYPE),
                        delimiter,
                        null_string,
                        first,
                        // TODO: Consider switching to a more efficient
                        // floating point display library (e.g., ryu). This
                        // might result in some differences in the output
                        // format, however.
                        |w, x| write!(w, "{}", x),
                    )?
                };
            }
            match data_type {
                Utf8 => str_leaf!(StringArray),
                Utf8View => str_leaf!(StringViewArray),
                LargeUtf8 => str_leaf!(LargeStringArray),
                DataType::Boolean => bool_leaf!(BooleanArray),
                DataType::Float32 => float_leaf!(Float32Array),
                DataType::Float64 => float_leaf!(Float64Array),
                DataType::Int8 => int_leaf!(Int8Array),
                DataType::Int16 => int_leaf!(Int16Array),
                DataType::Int32 => int_leaf!(Int32Array),
                DataType::Int64 => int_leaf!(Int64Array),
                DataType::UInt8 => int_leaf!(UInt8Array),
                DataType::UInt16 => int_leaf!(UInt16Array),
                DataType::UInt32 => int_leaf!(UInt32Array),
                DataType::UInt64 => int_leaf!(UInt64Array),
                data_type if can_cast_types(data_type, &Utf8) => {
                    let str_arr = cast(arr, &Utf8).map_err(|e| {
                        DataFusionError::from(e)
                            .context("Casting to string in array_to_string")
                    })?;
                    return compute_array_to_string(
                        w,
                        &str_arr,
                        delimiter,
                        null_string,
                        first,
                    );
                }
                data_type => {
                    return not_impl_err!(
                        "Unsupported data type in array_to_string: {data_type}"
                    );
                }
            }
            Ok(())
        }
    }
}

/// Appends the string representation of each element in a leaf (non-list)
/// array to `w`, separated by `delimiter`. Null elements are rendered
/// using `null_string` if provided, or skipped otherwise. The `append`
/// closure controls how each non-null element is written.
fn write_leaf_to_string<'a, W: Write, A, T>(
    w: &mut W,
    arr: &'a A,
    delimiter: &str,
    null_string: Option<&str>,
    first: &mut bool,
    append: impl Fn(&mut W, T) -> fmt::Result,
) -> Result<()>
where
    &'a A: IntoIterator<Item = Option<T>>,
{
    for x in arr {
        // Skip nulls when no null_string is provided
        if x.is_none() && null_string.is_none() {
            continue;
        }

        if *first {
            *first = false;
        } else {
            w.write_str(delimiter)?;
        }

        match x {
            Some(x) => append(w, x)?,
            None => w.write_str(null_string.unwrap())?,
        }
    }
    Ok(())
}

trait StringArrayBuilderType: ArrayBuilder {
    fn append_value(&mut self, val: &str);

    fn append_null(&mut self);
}

impl StringArrayBuilderType for StringBuilder {
    fn append_value(&mut self, val: &str) {
        StringBuilder::append_value(self, val);
    }

    fn append_null(&mut self) {
        StringBuilder::append_null(self);
    }
}

impl StringArrayBuilderType for StringViewBuilder {
    fn append_value(&mut self, val: &str) {
        StringViewBuilder::append_value(self, val)
    }

    fn append_null(&mut self) {
        StringViewBuilder::append_null(self)
    }
}

impl StringArrayBuilderType for LargeStringBuilder {
    fn append_value(&mut self, val: &str) {
        LargeStringBuilder::append_value(self, val);
    }

    fn append_null(&mut self) {
        LargeStringBuilder::append_null(self);
    }
}

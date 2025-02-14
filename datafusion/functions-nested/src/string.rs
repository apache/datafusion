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
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, ListBuilder,
    OffsetSizeTrait, StringArray, StringBuilder, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field};
use datafusion_expr::TypeSignature;

use datafusion_common::{
    internal_datafusion_err, not_impl_err, plan_err, DataFusionError, Result,
};

use std::any::Any;

use crate::utils::make_scalar_function;
use arrow::array::{
    builder::{ArrayBuilder, LargeStringBuilder, StringViewBuilder},
    cast::AsArray,
    GenericStringArray, StringArrayType, StringViewArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType::{
    Dictionary, FixedSizeList, LargeList, LargeUtf8, List, Null, Utf8, Utf8View,
};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::exec_err;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::{downcast_arg, downcast_named_arg};
use datafusion_macros::user_doc;
use std::sync::Arc;

macro_rules! call_array_function {
    ($DATATYPE:expr, false) => {
        match $DATATYPE {
            DataType::Utf8 => array_function!(StringArray),
            DataType::Utf8View => array_function!(StringViewArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            dt => not_impl_err!("Unsupported data type in array_to_string: {dt}"),
        }
    };
    ($DATATYPE:expr, $INCLUDE_LIST:expr) => {{
        match $DATATYPE {
            DataType::List(_) => array_function!(ListArray),
            DataType::Utf8 => array_function!(StringArray),
            DataType::Utf8View => array_function!(StringViewArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            dt => not_impl_err!("Unsupported data type in array_to_string: {dt}"),
        }
    }};
}

macro_rules! to_string {
    ($ARG:expr, $ARRAY:expr, $DELIMITER:expr, $NULL_STRING:expr, $WITH_NULL_STRING:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        for x in arr {
            match x {
                Some(x) => {
                    $ARG.push_str(&x.to_string());
                    $ARG.push_str($DELIMITER);
                }
                None => {
                    if $WITH_NULL_STRING {
                        $ARG.push_str($NULL_STRING);
                        $ARG.push_str($DELIMITER);
                    }
                }
            }
        }
        Ok($ARG)
    }};
}

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
        description = "Optional. String to replace null values in the array. If not provided, nulls will be handled by default behavior."
    )
)]
#[derive(Debug)]
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
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("list_to_string"),
                String::from("array_join"),
                String::from("list_join"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayToString {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_to_string_inner)(args)
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
#[derive(Debug)]
pub(super) struct StringToArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl StringToArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("string_to_list")],
        }
    }
}

impl ScalarUDFImpl for StringToArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            Utf8 | Utf8View | LargeUtf8 => {
                List(Arc::new(Field::new_list_field(arg_types[0].clone(), true)))
            }
            _ => {
                return plan_err!(
                    "The string_to_array function can only accept Utf8, Utf8View or LargeUtf8."
                );
            }
        })
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        match args[0].data_type() {
            Utf8 | Utf8View => make_scalar_function(string_to_array_inner::<i32>)(args),
            LargeUtf8 => make_scalar_function(string_to_array_inner::<i64>)(args),
            other => {
                exec_err!("unsupported type for string_to_array function as {other:?}")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Array_to_string SQL function
pub(super) fn array_to_string_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters: Vec<Option<&str>> = match args[1].data_type() {
        Utf8 => args[1].as_string::<i32>().iter().collect(),
        Utf8View => args[1].as_string_view().iter().collect(),
        LargeUtf8 => args[1].as_string::<i64>().iter().collect(),
        other => return exec_err!("unsupported type for second argument to array_to_string function as {other:?}")
    };

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = match args[2].data_type() {
            Utf8 => args[2].as_string::<i32>().value(0).to_string(),
            Utf8View => args[2].as_string_view().value(0).to_string(),
            LargeUtf8 => args[2].as_string::<i64>().value(0).to_string(),
            other => return exec_err!("unsupported type for second argument to array_to_string function as {other:?}")
        };
        with_null_string = true;
    }

    /// Creates a single string from single element of a ListArray (which is
    /// itself another Array)
    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimiter: String,
        null_string: String,
        with_null_string: bool,
    ) -> Result<&mut String> {
        match arr.data_type() {
            List(..) => {
                let list_array = as_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            LargeList(..) => {
                let list_array = as_large_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            Dictionary(_key_type, value_type) => {
                // Call cast to unwrap the dictionary. This could be optimized if we wanted
                // to accept the overhead of extra code
                let values = cast(&arr, value_type.as_ref()).map_err(|e| {
                    DataFusionError::from(e).context(
                        "Casting dictionary to values in compute_array_to_string",
                    )
                })?;
                compute_array_to_string(
                    arg,
                    values,
                    delimiter,
                    null_string,
                    with_null_string,
                )
            }
            Null => Ok(arg),
            data_type => {
                macro_rules! array_function {
                    ($ARRAY_TYPE:ident) => {
                        to_string!(
                            arg,
                            arr,
                            &delimiter,
                            &null_string,
                            with_null_string,
                            $ARRAY_TYPE
                        )
                    };
                }
                call_array_function!(data_type, false)
            }
        }
    }

    fn generate_string_array<O: OffsetSizeTrait>(
        list_arr: &GenericListArray<O>,
        delimiters: Vec<Option<&str>>,
        null_string: String,
        with_null_string: bool,
    ) -> Result<StringArray> {
        let mut res: Vec<Option<String>> = Vec::new();
        for (arr, &delimiter) in list_arr.iter().zip(delimiters.iter()) {
            if let (Some(arr), Some(delimiter)) = (arr, delimiter) {
                let mut arg = String::from("");
                let s = compute_array_to_string(
                    &mut arg,
                    arr,
                    delimiter.to_string(),
                    null_string.clone(),
                    with_null_string,
                )?
                .clone();

                if let Some(s) = s.strip_suffix(delimiter) {
                    res.push(Some(s.to_string()));
                } else {
                    res.push(Some(s));
                }
            } else {
                res.push(None);
            }
        }

        Ok(StringArray::from(res))
    }

    let arr_type = arr.data_type();
    let string_arr = match arr_type {
        List(_) | FixedSizeList(_, _) => {
            let list_array = as_list_array(&arr)?;
            generate_string_array::<i32>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        LargeList(_) => {
            let list_array = as_large_list_array(&arr)?;
            generate_string_array::<i64>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        _ => {
            let mut arg = String::from("");
            let mut res: Vec<Option<String>> = Vec::new();
            // delimiter length is 1
            assert_eq!(delimiters.len(), 1);
            let delimiter = delimiters[0].unwrap();
            let s = compute_array_to_string(
                &mut arg,
                Arc::clone(arr),
                delimiter.to_string(),
                null_string,
                with_null_string,
            )?
            .clone();

            if !s.is_empty() {
                let s = s.strip_suffix(delimiter).unwrap().to_string();
                res.push(Some(s));
            } else {
                res.push(Some(s));
            }
            StringArray::from(res)
        }
    };

    Ok(Arc::new(string_arr))
}

/// String_to_array SQL function
/// Splits string at occurrences of delimiter and returns an array of parts
/// string_to_array('abc~@~def~@~ghi', '~@~') = '["abc", "def", "ghi"]'
fn string_to_array_inner<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("string_to_array expects two or three arguments");
    }

    match args[0].data_type() {
        Utf8 => {
            let string_array = args[0].as_string::<T>();
            let builder = StringBuilder::with_capacity(string_array.len(), string_array.get_buffer_memory_size());
            string_to_array_inner_2::<&GenericStringArray<T>, StringBuilder>(args, string_array, builder)
        }
        Utf8View => {
            let string_array = args[0].as_string_view();
            let builder = StringViewBuilder::with_capacity(string_array.len());
            string_to_array_inner_2::<&StringViewArray, StringViewBuilder>(args, string_array, builder)
        }
        LargeUtf8 => {
            let string_array = args[0].as_string::<T>();
            let builder = LargeStringBuilder::with_capacity(string_array.len(), string_array.get_buffer_memory_size());
            string_to_array_inner_2::<&GenericStringArray<T>, LargeStringBuilder>(args, string_array, builder)
        }
        other =>  exec_err!("unsupported type for first argument to string_to_array function as {other:?}")
    }
}

fn string_to_array_inner_2<'a, StringArrType, StringBuilderType>(
    args: &'a [ArrayRef],
    string_array: StringArrType,
    string_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    StringBuilderType: StringArrayBuilderType,
{
    match args[1].data_type() {
        Utf8 => {
            let delimiter_array = args[1].as_string::<i32>();
            if args.len() == 2 {
                string_to_array_impl::<
                    StringArrType,
                    &GenericStringArray<i32>,
                    &StringViewArray,
                    StringBuilderType,
                >(string_array, delimiter_array, None, string_builder)
            } else {
                string_to_array_inner_3::<StringArrType,
                    &GenericStringArray<i32>,
                    StringBuilderType>(args, string_array, delimiter_array, string_builder)
            }
        }
        Utf8View => {
            let delimiter_array = args[1].as_string_view();

            if args.len() == 2 {
                string_to_array_impl::<
                    StringArrType,
                    &StringViewArray,
                    &StringViewArray,
                    StringBuilderType,
                >(string_array, delimiter_array, None, string_builder)
            } else {
                string_to_array_inner_3::<StringArrType,
                    &StringViewArray,
                    StringBuilderType>(args, string_array, delimiter_array, string_builder)
            }
        }
        LargeUtf8 => {
            let delimiter_array = args[1].as_string::<i64>();
            if args.len() == 2 {
                string_to_array_impl::<
                    StringArrType,
                    &GenericStringArray<i64>,
                    &StringViewArray,
                    StringBuilderType,
                >(string_array, delimiter_array, None, string_builder)
            } else {
                string_to_array_inner_3::<StringArrType,
                    &GenericStringArray<i64>,
                    StringBuilderType>(args, string_array, delimiter_array, string_builder)
            }
        }
        other =>  exec_err!("unsupported type for second argument to string_to_array function as {other:?}")
    }
}

fn string_to_array_inner_3<'a, StringArrType, DelimiterArrType, StringBuilderType>(
    args: &'a [ArrayRef],
    string_array: StringArrType,
    delimiter_array: DelimiterArrType,
    string_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    StringBuilderType: StringArrayBuilderType,
{
    match args[2].data_type() {
        Utf8 => {
            let null_type_array = Some(args[2].as_string::<i32>());
            string_to_array_impl::<
                StringArrType,
                DelimiterArrType,
                &GenericStringArray<i32>,
                StringBuilderType,
            >(
                string_array,
                delimiter_array,
                null_type_array,
                string_builder,
            )
        }
        Utf8View => {
            let null_type_array = Some(args[2].as_string_view());
            string_to_array_impl::<
                StringArrType,
                DelimiterArrType,
                &StringViewArray,
                StringBuilderType,
            >(
                string_array,
                delimiter_array,
                null_type_array,
                string_builder,
            )
        }
        LargeUtf8 => {
            let null_type_array = Some(args[2].as_string::<i64>());
            string_to_array_impl::<
                StringArrType,
                DelimiterArrType,
                &GenericStringArray<i64>,
                StringBuilderType,
            >(
                string_array,
                delimiter_array,
                null_type_array,
                string_builder,
            )
        }
        other => {
            exec_err!("unsupported type for string_to_array function as {other:?}")
        }
    }
}

fn string_to_array_impl<
    'a,
    StringArrType,
    DelimiterArrType,
    NullValueArrType,
    StringBuilderType,
>(
    string_array: StringArrType,
    delimiter_array: DelimiterArrType,
    null_value_array: Option<NullValueArrType>,
    string_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    NullValueArrType: StringArrayType<'a>,
    StringBuilderType: StringArrayBuilderType,
{
    let mut list_builder = ListBuilder::new(string_builder);

    match null_value_array {
        None => {
            string_array.iter().zip(delimiter_array.iter()).for_each(
                |(string, delimiter)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            list_builder.values().append_value(string);
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                list_builder.values().append_value(s);
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                list_builder.values().append_value(c.as_str());
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                },
            )
        }
        Some(null_value_array) => string_array
            .iter()
            .zip(delimiter_array.iter())
            .zip(null_value_array.iter())
            .for_each(|((string, delimiter), null_value)| {
                match (string, delimiter) {
                    (Some(string), Some("")) => {
                        if Some(string) == null_value {
                            list_builder.values().append_null();
                        } else {
                            list_builder.values().append_value(string);
                        }
                        list_builder.append(true);
                    }
                    (Some(string), Some(delimiter)) => {
                        string.split(delimiter).for_each(|s| {
                            if Some(s) == null_value {
                                list_builder.values().append_null();
                            } else {
                                list_builder.values().append_value(s);
                            }
                        });
                        list_builder.append(true);
                    }
                    (Some(string), None) => {
                        string.chars().map(|c| c.to_string()).for_each(|c| {
                            if Some(c.as_str()) == null_value {
                                list_builder.values().append_null();
                            } else {
                                list_builder.values().append_value(c.as_str());
                            }
                        });
                        list_builder.append(true);
                    }
                    _ => list_builder.append(false), // null value
                }
            }),
    };

    let list_array = list_builder.finish();
    Ok(Arc::new(list_array) as ArrayRef)
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

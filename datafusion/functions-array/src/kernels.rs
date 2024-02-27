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

//! implementation kernels for array functions

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericListArray,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, OffsetSizeTrait,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_list_array, as_list_array, as_string_array};
use datafusion_common::{exec_err, DataFusionError};
use std::any::type_name;
use std::sync::Arc;

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
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

macro_rules! call_array_function {
    ($DATATYPE:expr, false) => {
        match $DATATYPE {
            DataType::Utf8 => array_function!(StringArray),
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
            _ => unreachable!(),
        }
    };
    ($DATATYPE:expr, $INCLUDE_LIST:expr) => {{
        match $DATATYPE {
            DataType::List(_) => array_function!(ListArray),
            DataType::Utf8 => array_function!(StringArray),
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
            _ => unreachable!(),
        }
    }};
}

/// Array_to_string SQL function
pub(super) fn array_to_string(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters = as_string_array(&args[1])?;
    let delimiters: Vec<Option<&str>> = delimiters.iter().collect();

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = as_string_array(&args[2])?.value(0).to_string();
        with_null_string = true;
    }

    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimiter: String,
        null_string: String,
        with_null_string: bool,
    ) -> datafusion_common::Result<&mut String> {
        match arr.data_type() {
            DataType::List(..) => {
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
            DataType::LargeList(..) => {
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
            DataType::Null => Ok(arg),
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
    ) -> datafusion_common::Result<StringArray> {
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
        DataType::List(_) | DataType::FixedSizeList(_, _) => {
            let list_array = as_list_array(&arr)?;
            generate_string_array::<i32>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        DataType::LargeList(_) => {
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
                arr.clone(),
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

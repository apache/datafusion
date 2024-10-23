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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::strings::StringArrayType;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, Datum, ListArray, ListBuilder, StringArray,
    StringBuilder,
};
use arrow::datatypes::DataType::{LargeUtf8, Utf8};
use arrow::datatypes::{DataType, Field, GenericStringType};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::TypeSignature::{Exact, Uniform};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

use itertools::{izip, Itertools};
use regex::Regex;

#[derive(Debug)]
pub struct RegexpSplitToArrayFunc {
    signature: Signature,
}

impl Default for RegexpSplitToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSplitToArrayFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Uniform(2, vec![Utf8View, LargeUtf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                    Exact(vec![Utf8, Utf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSplitToArrayFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_split_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        use DataType::*;

        Ok(match &arg_types[0] {
            Null => Null,
            other => List(Arc::new(Field::new("item", other.clone(), true))),
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let result = regexp_split_to_array_func(&args)?;
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(result))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_regexp_split_to_array_doc())
    }
}

pub fn regexp_split_to_array_func(
    args: &[ArrayRef],
) -> Result<ArrayRef, DataFusionError> {
    let args_len = args.len();
    if !(2..=3).contains(&args_len) {
        return exec_err!("regexp_count was called with {args_len} arguments. It requires 2 or 3 arguments.");
    }

    let values = &args[0];

    regex_split_to_array(
        values,
        &args[1],
        if args_len > 2 { Some(&args[2]) } else { None },
    )
    .map_err(|e| e.into())
}

pub fn regex_split_to_array(
    values: &dyn Array,
    regex_array: &dyn Datum,
    flags_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (regex_array, is_regex_scalar) = regex_array.get();
    let (flags_array, is_flags_scalar) = flags_array.map_or((None, true), |flags| {
        let (flags, is_flags_scalar) = flags.get();
        (Some(flags), is_flags_scalar)
    });

    match (values.data_type(), regex_array.data_type(), flags_array) {
        (Utf8, Utf8, None) => regexp_split_to_array_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            None,
            is_flags_scalar,
        ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 => regexp_split_to_array_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            Some(flags_array.as_string::<i32>()),
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, None) => regexp_split_to_array_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            None,
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 => regexp_split_to_array_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            Some(flags_array.as_string::<i64>()),
            is_flags_scalar,
        ),
        _ => Err(ArrowError::ComputeError(
            "regexp_count() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the values, regex_array, and flags_array to match".to_string(),
        )),
    }
}

pub fn regexp_split_to_array_inner<'a, S>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    flags_array: Option<S>,
    is_flags_scalar: bool,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a>,
{
    let (regex_scalar, is_regex_scalar) = if is_regex_scalar || regex_array.len() == 1 {
        (Some(regex_array.value(0)), true)
    } else {
        (None, false)
    };

    let (flags_array, flags_scalar, is_flags_scalar) =
        if let Some(flags_array) = flags_array {
            if is_flags_scalar || flags_array.len() == 1 {
                (None, Some(flags_array.value(0)), true)
            } else {
                (Some(flags_array), None, false)
            }
        } else {
            (None, None, true)
        };

    let mut regex_cache = HashMap::new();

    let result = match (is_regex_scalar, is_flags_scalar) {
        (true, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    let iter = values
                        .iter()
                        .map(|value| {
                            Some(
                                get_splitted_array_no_regex(value)
                                    .into_iter()
                                    .map(Some)
                                    .collect_vec(),
                            )
                        })
                        .collect_vec();
                    return Ok(
                        Arc::new(ListArray::from_iter_primitive(iter)) as Arc<dyn Array>
                    );
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;
            let iter = values
                .iter()
                .map(|value| {
                    Some(get_splitted_array(value, &pattern).into_iter().map(Some))
                })
                .collect_vec();
            Ok(Arc::new(ListArray::from_iter_primitive(iter)))
        }
        (true, false) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(
                        values
                            .iter()
                            .map(|value| get_splitted_array_no_regex(value))
                            .collect(),
                    ))
                }
                Some(regex) => regex,
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            let iter = values
                .iter()
                .zip(flags_array.iter())
                .map(|(value, flags)| {
                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache).ok();
                    pattern.map(|p| Some(get_splitted_array(value, &p)))
                })
                .collect(); // TODO: there must be no need to collect
            Ok(Arc::new(ListArray::from_iter_primitive(iter)))
        }
        (false, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let iter = values
                .iter()
                .zip(regex_array.iter())
                .map(|(value, regex)| {
                    let regex = match regex {
                        None | Some("") => {
                            return Err(ArrowError::ComputeError(
                                "regex_array must not contain null or empty strings"
                                    .to_string(),
                            ))
                        }
                        Some(regex) => regex,
                    };
                    let pattern =
                        compile_and_cache_regex(regex, flags_scalar, &mut regex_cache)
                            .ok();
                    Ok(pattern
                        .map(|p| Some(get_splitted_array(value, &p)))
                        .unwrap()
                        .unwrap())
                })
                .collect();
            Ok(Arc::new(ListArray::from_iter_primitive(iter)))
        }
        (false, false) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            let iter = izip!(values.iter(), regex_array.iter(), flags_array.iter())
                .map(|(value, regex, flags)| {
                    let regex = match regex {
                        None | Some("") => return Some(vec![]),
                        Some(regex) => regex,
                    };

                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache).ok();
                    pattern.map(|p| {
                        get_splitted_array(value, &p)
                            .into_iter()
                            .map(Some)
                            .collect()
                    })
                })
                .collect_vec();
            Ok(Arc::new(ListArray::from_iter_primitive(iter)))
        }
    };
    result.map(|r| r as Arc<dyn Array>)
}

fn compile_and_cache_regex(
    regex: &str,
    flags: Option<&str>,
    regex_cache: &mut HashMap<String, Regex>,
) -> Result<Regex, ArrowError> {
    match regex_cache.entry(regex.to_string()) {
        Entry::Vacant(entry) => {
            let compiled = compile_regex(regex, flags)?;
            entry.insert(compiled.clone());
            Ok(compiled)
        }
        Entry::Occupied(entry) => Ok(entry.get().to_owned()),
    }
}

fn compile_regex(regex: &str, flags: Option<&str>) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") {
                return Err(ArrowError::ComputeError(
                    "regexp_count() does not support global flag".to_string(),
                ));
            }
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
    })
}

fn get_splitted_array_no_regex(value: Option<&str>) -> Vec<String> {
    match value {
        None | Some("") => vec![],
        Some(value) => vec![value.split("").collect()],
    }
}

fn get_splitted_array(value: Option<&str>, pattern: &Regex) -> Vec<String> {
    let value = match value {
        None | Some("") => return vec![],
        Some(value) => value,
    };
    pattern.split(value).map(|s| s.to_string()).collect_vec()
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_regexp_split_to_array_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| Documentation::builder().build().unwrap())
}

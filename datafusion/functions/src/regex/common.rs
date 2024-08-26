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

//! Common utilities for implementing regex functions

use std::collections::HashMap;

use arrow::array::{Array, ArrayDataBuilder, BooleanArray};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use datafusion_common::DataFusionError;
use regex::Regex;

use crate::string::common::StringArrayType;

/// Perform SQL `array ~ regex_array` operation on
/// [`StringArray`] / [`LargeStringArray`] / [`StringViewArray`].
/// If `regex_array` element has an empty value, the corresponding result value is always true.
///
/// `flags_array` are optional [`StringArray`] / [`LargeStringArray`] / [`StringViewArray`] flag,
/// which allow special search modes, such as case-insensitive and multi-line mode.
/// See the documentation [here](https://docs.rs/regex/1.5.4/regex/#grouping-and-flags)
/// for more information.
///
/// It is inspired / copied from `regexp_is_match_utf8` [arrow-rs].
///
/// [arrow-rs]: https://github.com/apache/arrow-rs/blob/8c956a9f9ab26c14072740cce64c2b99cb039b13/arrow-string/src/regexp.rs#L31-L37
pub fn regexp_is_match<'a, ArrayType1, ArrayType2, ArrayType3>(
    array: &'a ArrayType1,
    regex_array: &'a ArrayType2,
    flags_array: Option<&'a ArrayType3>,
) -> datafusion_common::Result<BooleanArray, DataFusionError>
where
    &'a ArrayType1: StringArrayType<'a>,
    &'a ArrayType2: StringArrayType<'a>,
    &'a ArrayType3: StringArrayType<'a>,
{
    if array.len() != regex_array.len() {
        return Err(DataFusionError::Execution(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let nulls = NullBuffer::union(array.nulls(), regex_array.nulls());

    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let mut result = BooleanBufferBuilder::new(array.len());

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(regex_array.iter().zip(flags.iter()).map(
            |(pattern, flags)| {
                pattern.map(|pattern| match flags {
                    Some(flag) => format!("(?{flag}){pattern}"),
                    None => pattern.to_string(),
                })
            },
        )) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };

    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                (Some(_), Some(pattern)) if pattern == *"" => {
                    result.append(true);
                }
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re,
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Regular expression did not compile: {e:?}"
                                ))
                            })?;
                            patterns.entry(pattern).or_insert(re)
                        }
                    };
                    result.append(re.is_match(value));
                }
                _ => result.append(false),
            }
            Ok(())
        })
        .collect::<datafusion_common::Result<Vec<()>, ArrowError>>()?;

    let data = unsafe {
        ArrayDataBuilder::new(DataType::Boolean)
            .len(array.len())
            .buffers(vec![result.into()])
            .nulls(nulls)
            .build_unchecked()
    };

    Ok(BooleanArray::from(data))
}

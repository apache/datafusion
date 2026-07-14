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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_int_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the [`Levenshtein distance`](https://en.wikipedia.org/wiki/Levenshtein_distance) between the two given strings.",
    syntax_example = "levenshtein(str1, str2)",
    sql_example = r#"```sql
> select levenshtein('kitten', 'sitting');
+---------------------------------------------+
| levenshtein(Utf8("kitten"),Utf8("sitting")) |
+---------------------------------------------+
| 3                                           |
+---------------------------------------------+
```"#,
    argument(
        name = "str1",
        description = "String expression to compute Levenshtein distance with str2."
    ),
    argument(
        name = "str2",
        description = "String expression to compute Levenshtein distance with str1."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LevenshteinFunc {
    signature: Signature,
}

impl Default for LevenshteinFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LevenshteinFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LevenshteinFunc {
    fn name(&self) -> &str {
        "levenshtein"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .or_else(|| binary_to_string_coercion(&arg_types[0], &arg_types[1]))
        {
            utf8_to_int_type(&coercion_data_type, "levenshtein")
        } else {
            exec_err!(
                "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(levenshtein::<i32>, vec![])(&args.args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(levenshtein::<i64>, vec![])(&args.args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Longest pattern, in bytes, that the bit-parallel path can encode in a single
/// machine word. That path is ASCII-only, so this is a character count too.
const MYERS_MAX_PATTERN_LEN: usize = u64::BITS as usize;

/// Pattern length up to which resetting the match table character by character
/// beats zeroing the whole of it. Above this, a vectorized fill is cheaper than
/// the scattered stores.
const MYERS_SPARSE_RESET_LEN: usize = 32;

/// Scratch space reused across rows so that no single row has to allocate.
struct LevenshteinScratch {
    /// Dynamic-programming row for the character-wise fallback.
    cache: Vec<usize>,
    /// Bitmask of the positions at which each ASCII character occurs in the
    /// Myers pattern. Characters absent from the pattern match nowhere, hence 0.
    /// Left all-zero between rows so that each row can fill it in directly.
    peq: [u64; 128],
}

impl LevenshteinScratch {
    fn new() -> Self {
        Self {
            cache: Vec::new(),
            peq: [0; 128],
        }
    }
}

/// Levenshtein distance between `a` and `b`.
///
/// Uses Myers' bit-parallel algorithm when both inputs are ASCII and the shorter
/// one fits in a single 64-bit word, which computes a whole column of the
/// dynamic-programming matrix per text character instead of one cell at a time.
/// Longer or non-ASCII inputs fall back to the character-wise implementation and
/// stay quadratic.
#[inline]
fn levenshtein_distance(a: &str, b: &str, scratch: &mut LevenshteinScratch) -> usize {
    // The distance is symmetric, so the shorter side can always be the pattern.
    let (pattern, text) = if a.len() <= b.len() { (a, b) } else { (b, a) };

    if pattern.len() <= MYERS_MAX_PATTERN_LEN && pattern.is_ascii() && text.is_ascii() {
        myers_distance(pattern.as_bytes(), text.as_bytes(), &mut scratch.peq)
    } else {
        // The fallback sizes its buffer from the second argument, so give it the
        // shorter side.
        datafusion_strsim::levenshtein_with_buffer(text, pattern, &mut scratch.cache)
    }
}

/// Myers' bit-parallel Levenshtein distance. Both inputs must be ASCII and
/// `pattern` must be at most [`MYERS_MAX_PATTERN_LEN`] bytes long.
///
/// `vp`/`vn` hold the vertical deltas (+1 / -1) of the current matrix column as
/// bitmasks, one bit per pattern character, and `score` tracks the value of that
/// column's last cell.
///
/// `peq` must be all-zero on entry and is left all-zero on return.
fn myers_distance(pattern: &[u8], text: &[u8], peq: &mut [u64; 128]) -> usize {
    debug_assert!(pattern.is_ascii() && text.is_ascii());
    debug_assert!(pattern.len() <= MYERS_MAX_PATTERN_LEN);
    debug_assert!(peq.iter().all(|&mask| mask == 0));

    let m = pattern.len();
    if m == 0 {
        return text.len();
    }

    for (i, &c) in pattern.iter().enumerate() {
        peq[c as usize] |= 1 << i;
    }

    let last_bit = 1u64 << (m - 1);
    let mut vp = u64::MAX;
    let mut vn = 0u64;
    let mut score = m;

    for &c in text {
        let eq = peq[c as usize];
        let x = eq | vn;
        let d0 = (vp.wrapping_add(x & vp) ^ vp) | x;
        let hn = vp & d0;
        let hp = vn | !(vp | d0);

        if hp & last_bit != 0 {
            score += 1;
        } else if hn & last_bit != 0 {
            score -= 1;
        }

        let hp = (hp << 1) | 1;
        let hn = hn << 1;
        vp = hn | !(d0 | hp);
        vn = hp & d0;
    }

    // Restore the all-zero invariant for the next row.
    if m <= MYERS_SPARSE_RESET_LEN {
        for &c in pattern {
            peq[c as usize] = 0;
        }
    } else {
        peq.fill(0);
    }

    score
}

///Returns the Levenshtein distance between the two given strings.
/// LEVENSHTEIN('kitten', 'sitting') = 3
fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [str1, str2] = take_function_args("levenshtein", args)?;

    if let Some(coercion_data_type) =
        string_coercion(args[0].data_type(), args[1].data_type()).or_else(|| {
            binary_to_string_coercion(args[0].data_type(), args[1].data_type())
        })
    {
        let str1 = if str1.data_type() == &coercion_data_type {
            Arc::clone(str1)
        } else {
            arrow::compute::kernels::cast::cast(&str1, &coercion_data_type)?
        };
        let str2 = if str2.data_type() == &coercion_data_type {
            Arc::clone(str2)
        } else {
            arrow::compute::kernels::cast::cast(&str2, &coercion_data_type)?
        };

        match coercion_data_type {
            DataType::Utf8View => {
                let str1_array = as_string_view_array(&str1)?;
                let str2_array = as_string_view_array(&str2)?;

                // Reusable scratch space to avoid allocating for each row
                let mut scratch = LevenshteinScratch::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(levenshtein_distance(string1, string2, &mut scratch)
                                as i32)
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::Utf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;

                // Reusable scratch space to avoid allocating for each row
                let mut scratch = LevenshteinScratch::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(levenshtein_distance(string1, string2, &mut scratch)
                                as i32)
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::LargeUtf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;

                // Reusable scratch space to avoid allocating for each row
                let mut scratch = LevenshteinScratch::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(levenshtein_distance(string1, string2, &mut scratch)
                                as i64)
                        }
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            other => {
                exec_err!(
                    "levenshtein was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
                )
            }
        }
    } else {
        exec_err!(
            "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use datafusion_common::cast::as_int32_array;

    use super::*;

    #[test]
    fn to_levenshtein() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array, string2_array]).unwrap();
        let result =
            as_int32_array(&res).expect("failed to initialized function levenshtein");
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        Ok(())
    }

    /// The bit-parallel path must agree with the reference implementation for
    /// every input it accepts, including empty strings and pattern lengths at
    /// the 64-character word boundary.
    #[test]
    fn myers_matches_reference() {
        let alphabets: [&[u8]; 3] = [b"ab", b"abcdefg", b"kitensg -0"];
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut scratch = LevenshteinScratch::new();

        for alphabet in alphabets {
            for len1 in [0usize, 1, 2, 5, 16, 63, 64, 65, 100] {
                for len2 in [0usize, 1, 3, 8, 32, 64, 65, 120] {
                    for _ in 0..8 {
                        let make = |n: usize, next: &mut dyn FnMut() -> u64| {
                            (0..n)
                                .map(|_| {
                                    alphabet[(next() % alphabet.len() as u64) as usize]
                                        as char
                                })
                                .collect::<String>()
                        };
                        let a = make(len1, &mut next);
                        let b = make(len2, &mut next);
                        assert_eq!(
                            levenshtein_distance(&a, &b, &mut scratch),
                            datafusion_strsim::levenshtein(&a, &b),
                            "levenshtein({a:?}, {b:?})"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn myers_known_distances() {
        let mut scratch = LevenshteinScratch::new();
        for (a, b, expected) in [
            ("kitten", "sitting", 3),
            ("", "", 0),
            ("", "abc", 3),
            ("abc", "", 3),
            ("abc", "abc", 0),
            ("flaw", "lawn", 2),
            // Non-ASCII falls back to the character-wise implementation.
            ("café", "cafe", 1),
            ("😀abc", "abc", 1),
        ] {
            assert_eq!(
                levenshtein_distance(a, b, &mut scratch),
                expected,
                "{a} {b}"
            );
        }
    }
}

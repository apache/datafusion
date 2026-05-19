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

//! Regex expressions
use memchr::memchr;

use arrow::array::ArrayDataBuilder;
use arrow::array::BufferBuilder;
use arrow::array::GenericStringArray;
use arrow::array::StringViewBuilder;
use arrow::array::{Array, ArrayRef, OffsetSizeTrait};
use arrow::array::{ArrayAccessor, StringViewArray};
use arrow::array::{ArrayIter, AsArray, new_null_array};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::exec_err;
use datafusion_common::plan_err;
use datafusion_common::{
    DataFusionError, Result, cast::as_generic_string_array, internal_err,
};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature;
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use regex::{CaptureLocations, Regex};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Replaces substrings in a string that match a [regular expression](https://docs.rs/regex/latest/regex/#syntax).",
    syntax_example = "regexp_replace(str, regexp, replacement[, flags])",
    sql_example = r#"```sql
> select regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g');
+------------------------------------------------------------------------+
| regexp_replace(Utf8("foobarbaz"),Utf8("b(..)"),Utf8("X\1Y"),Utf8("g")) |
+------------------------------------------------------------------------+
| fooXarYXazY                                                            |
+------------------------------------------------------------------------+
SELECT regexp_replace('aBc', '(b|d)', 'Ab\\1a', 'i');
+-------------------------------------------------------------------+
| regexp_replace(Utf8("aBc"),Utf8("(b|d)"),Utf8("Ab\1a"),Utf8("i")) |
+-------------------------------------------------------------------+
| aAbBac                                                            |
+-------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/regexp.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against.
  Can be a constant, column, or function."
    ),
    argument(
        name = "replacement",
        description = "Replacement string expression to operate on. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
- **g**: (global) Search globally and don't return after the first match
- **i**: case-insensitive: letters match both upper and lower case
- **m**: multi-line mode: ^ and $ match begin/end of line
- **s**: allow . to match \n
- **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
- **U**: swap the meaning of x* and x*?"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpReplaceFunc {
    signature: Signature,
}
impl Default for RegexpReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpReplaceFunc {
    pub fn new() -> Self {
        use DataType::*;
        use TypeSignature::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Uniform(3, vec![Utf8View, LargeUtf8, Utf8]),
                    Uniform(4, vec![Utf8View, LargeUtf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpReplaceFunc {
    fn name(&self) -> &str {
        "regexp_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => LargeUtf8,
            Utf8 | Binary => Utf8,
            Utf8View | BinaryView => Utf8View,
            Null => Null,
            Dictionary(_, t) => match **t {
                LargeUtf8 | LargeBinary => LargeUtf8,
                Utf8 | Binary => Utf8,
                Null => Null,
                _ => {
                    return plan_err!(
                        "the regexp_replace can only accept strings but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The regexp_replace function can only accept strings. Got {other}"
                );
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;

        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let result = regexp_replace_func(args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn regexp_replace_func(args: &[ColumnarValue]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => specialize_regexp_replace::<i32>(args),
        DataType::LargeUtf8 => specialize_regexp_replace::<i64>(args),
        DataType::Utf8View => specialize_regexp_replace::<i32>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_replace")
        }
    }
}

/// replace POSIX capture groups (like \1 or \\1) with Rust Regex group (like ${1})
/// used by regexp_replace
/// Handles both single backslash (\1) and double backslash (\\1) which can occur
/// when SQL strings with escaped backslashes are passed through
///
/// Note: \0 is converted to ${0}, which in Rust's regex replacement syntax
/// substitutes the entire match. This is consistent with POSIX behavior where
/// \0 (or &) refers to the entire matched string.
fn regex_replace_posix_groups(replacement: &str) -> String {
    static CAPTURE_GROUPS_RE_LOCK: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\\{1,2}(\d+)").unwrap());
    CAPTURE_GROUPS_RE_LOCK
        .replace_all(replacement, "$${$1}")
        .into_owned()
}

/// Fast-path state used in place of [`Regex::replacen`] for patterns matching
/// [`OptimizedRegex`]'s direct-extraction shape.
enum ShortRegex {
    /// Generic shortened-regex extractor for patterns of the form
    /// `^...(capture)....*$` with replacement `${1}`. The trailing `.*$` is
    /// stripped and `captures_read` is used to find capture group 1 directly,
    /// avoiding `expand()` and any `String` allocation.
    ShortenedRegex {
        re: Regex,
        locs: CaptureLocations,
    },
    /// Specialized memchr-based extractor for the common subset of the above
    /// where the regex up to the capture is a finite set of literal byte
    /// prefixes, the capture is `[^X]+` for a single ASCII byte `X`, and the
    /// capture is followed by `X.*$`. See
    /// [`try_recognize_literal_prefix_capture`] for the recognized shape.
    ///
    /// ClickBench Q28's `^https?://(?:www\.)?([^/]+)/.*$` is the motivating
    /// instance: `prefixes = ["https://www.", "http://www.", "https://", "http://"]`,
    /// `terminator = b'/'`.
    LiteralPrefixCapture(LiteralPrefixCaptureSpec),
}

/// Holds the normal compiled regex together with the optional fast path used
/// for `regexp_replace(str, '^...(capture)...*$', '\1')`.
struct OptimizedRegex {
    /// Full regex used for the normal replacement path and as a correctness fallback.
    re: Regex,
    /// Precomputed state for the direct-extraction fast path, when applicable.
    short_re: Option<ShortRegex>,
}

impl OptimizedRegex {
    /// Builds any reusable state needed by the extraction fast path.
    ///
    /// The fast path is only enabled for single replacements where the pattern
    /// and replacement satisfy [`try_build_short_extract_regex`].
    fn new(re: Regex, limit: usize, pattern: &str, replacement: &str) -> Self {
        let short_re = if limit == 1 {
            try_build_short_extract_regex(pattern, replacement)
        } else {
            None
        };

        Self { re, short_re }
    }

    /// Applies the direct-extraction fast path when it preserves the result of
    /// `Regex::replacen`; otherwise falls back to the full regex replacement.
    fn replacen<'a>(
        &mut self,
        val: &'a str,
        limit: usize,
        replacement: &str,
    ) -> Cow<'a, str> {
        // If this pattern is not eligible for direct extraction, use the full regex.
        let Some(short_re) = self.short_re.as_mut() else {
            return self.re.replacen(val, limit, replacement);
        };

        match short_re {
            ShortRegex::ShortenedRegex { re: short_re, locs } => {
                // If the shortened regex does not match, the original anchored regex would
                // also leave the input unchanged.
                if short_re.captures_read(locs, val).is_none() {
                    return Cow::Borrowed(val);
                };

                // `captures_read` succeeded, so the overall shortened match is present.
                let match_end = locs.get(0).unwrap().1;
                if memchr(b'\n', &val.as_bytes()[match_end..]).is_some() {
                    // If there is a newline after the match, we can't use the short
                    // regex since it won't match across lines. Fall back to the full
                    // regex replacement.
                    return self.re.replacen(val, limit, replacement);
                };
                // The fast path only applies to `${1}` replacements, so the result is
                // either capture group 1 or the empty string if that group did not match.
                if let Some((start, end)) = locs.get(1) {
                    Cow::Borrowed(&val[start..end])
                } else {
                    Cow::Borrowed("")
                }
            }
            ShortRegex::LiteralPrefixCapture(spec) => {
                let Some((start, end, match_end)) =
                    spec.extract(val.as_bytes())
                else {
                    return Cow::Borrowed(val);
                };

                if memchr(b'\n', &val.as_bytes()[match_end..]).is_some() {
                    // Same single-line safety as the shortened-regex path: `.*$`
                    // in the original anchored pattern doesn't cross `\n`, so
                    // hand off to the full regex when trailing content has one.
                    return self.re.replacen(val, limit, replacement);
                }

                Cow::Borrowed(&val[start..end])
            }
        }
    }
}

/// For anchored patterns like `^...(capture)....*$` where the replacement
/// is `\1`, build the fastest available direct-extraction state.
///
/// Two shapes are recognized:
///
/// 1. `^<literal-prefix-set>([^X]+)X.*$` with replacement `${1}`, where the
///    prefix reduces to a finite set of byte literals and `X` is a single
///    ASCII byte. Handled by [`LiteralPrefixCaptureSpec`] with a `memchr`
///    terminator scan — no regex engine involvement per row.
/// 2. Any other `^...(capture)....*$` with replacement `${1}`: the trailing
///    `.*$` is stripped and the resulting shortened regex is run with
///    `captures_read` against reusable `CaptureLocations`.
///
/// ClickBench Q28's `^https?://(?:www\.)?([^/]+)/.*$` matches shape 1.
fn try_build_short_extract_regex(pattern: &str, replacement: &str) -> Option<ShortRegex> {
    if replacement != "${1}" || !pattern.starts_with('^') || !pattern.ends_with(".*$") {
        return None;
    }

    if let Some(spec) = try_recognize_literal_prefix_capture(pattern) {
        return Some(ShortRegex::LiteralPrefixCapture(spec));
    }

    let short = &pattern[..pattern.len() - 3];
    let re = Regex::new(short).ok()?;
    if re.captures_len() != 2 {
        return None;
    }
    let locs = re.capture_locations();
    Some(ShortRegex::ShortenedRegex { re, locs })
}

/// Bound on enumerated prefix variants. Each `(?:...)?` doubles the count and
/// each `(a|b|c)` multiplies it, so this caps the explosion for adversarial
/// patterns. ClickBench Q28's pattern produces 4 variants.
const MAX_PREFIX_VARIANTS: usize = 32;

/// Precomputed state for the `^<prefix-set>([^X]+)X.*$` → `${1}` fast path.
#[derive(Debug)]
struct LiteralPrefixCaptureSpec {
    /// Distinct literal byte prefixes the input must start with, sorted
    /// longest-first so a single linear scan acts greedily.
    prefixes: Vec<Box<[u8]>>,
    /// Single ASCII byte that ends the capture (also the literal that must
    /// follow the capture in the original regex).
    terminator: u8,
}

impl LiteralPrefixCaptureSpec {
    /// Returns `(capture_start, capture_end, match_end)` where `match_end`
    /// points past the terminator, or `None` if the input doesn't match.
    ///
    /// Prefixes are tried longest-first to mimic greedy matching, but we
    /// fall back to shorter alternatives if the greedy choice leaves no
    /// room for the capture. That mirrors the full regex's backtracking
    /// behavior for cases like `http://www./path` against
    /// `^https?://(?:www\.)?([^/]+)/.*$`, where the regex prefers to leave
    /// `www.` outside the optional so the capture is non-empty.
    fn extract(&self, bytes: &[u8]) -> Option<(usize, usize, usize)> {
        for prefix in &self.prefixes {
            if !bytes.starts_with(prefix) {
                continue;
            }
            let capture_start = prefix.len();
            let Some(terminator_offset) =
                memchr(self.terminator, &bytes[capture_start..])
            else {
                continue;
            };
            if terminator_offset == 0 {
                // `[^X]+` requires at least one byte; try a shorter prefix.
                continue;
            }
            let capture_end = capture_start + terminator_offset;
            return Some((capture_start, capture_end, capture_end + 1));
        }
        None
    }
}

/// Recognizes the `^<prefix-set>([^X]+)X.*$` shape with default flags (no
/// `(?i)`, `(?m)`, etc. — those would alter the byte-level interpretation
/// we rely on).
fn try_recognize_literal_prefix_capture(
    pattern: &str,
) -> Option<LiteralPrefixCaptureSpec> {
    use regex_syntax::hir::{Hir, HirKind, Look};

    let hir = regex_syntax::parse(pattern).ok()?;
    let HirKind::Concat(parts) = hir.kind() else {
        return None;
    };

    let mut iter = parts.iter();

    // 1. `^` (start of text — multiline `(?m)` would give StartLF instead).
    if !matches!(iter.next()?.kind(), HirKind::Look(Look::Start)) {
        return None;
    }

    // 2. Literal prefix segments up to (but not including) the capture.
    let mut prefixes: Vec<Vec<u8>> = vec![Vec::new()];
    let capture: &Hir = loop {
        let part = iter.next()?;
        if matches!(part.kind(), HirKind::Capture(_)) {
            break part;
        }
        prefix_extend_variants(&mut prefixes, part)?;
        if prefixes.len() > MAX_PREFIX_VARIANTS {
            return None;
        }
    };

    // 3. Capture must be group 1 wrapping `[^X]+` (greedy, 1+) for a single
    //    ASCII byte X.
    let HirKind::Capture(cap) = capture.kind() else {
        unreachable!()
    };
    if cap.index != 1 {
        return None;
    }
    let terminator = capture_terminator_byte(&cap.sub)?;

    // 4. Literal terminator matching the excluded byte.
    let HirKind::Literal(lit) = iter.next()?.kind() else {
        return None;
    };
    if lit.0.as_ref() != [terminator] {
        return None;
    }

    // 5. `.*` (any byte except `\n`, zero or more).
    if !is_dot_star(iter.next()?) {
        return None;
    }

    // 6. `$` and nothing after.
    if !matches!(iter.next()?.kind(), HirKind::Look(Look::End)) {
        return None;
    }
    if iter.next().is_some() {
        return None;
    }

    // Dedupe + sort longest-first so the runtime probe is greedy.
    prefixes.sort();
    prefixes.dedup();
    let mut prefixes: Vec<Box<[u8]>> = prefixes
        .into_iter()
        .map(Vec::into_boxed_slice)
        .collect();
    prefixes.sort_by_key(|p| std::cmp::Reverse(p.len()));

    Some(LiteralPrefixCaptureSpec {
        prefixes,
        terminator,
    })
}

/// Extend the accumulator with one prefix segment. Returns `None` if the
/// segment isn't a finite literal shape (literal / concat / alternation /
/// `?`-optional combination of those).
fn prefix_extend_variants(
    variants: &mut Vec<Vec<u8>>,
    hir: &regex_syntax::hir::Hir,
) -> Option<()> {
    use regex_syntax::hir::HirKind;

    match hir.kind() {
        HirKind::Literal(lit) => {
            for v in variants.iter_mut() {
                v.extend_from_slice(&lit.0);
            }
            Some(())
        }
        HirKind::Concat(parts) => {
            for part in parts {
                prefix_extend_variants(variants, part)?;
                if variants.len() > MAX_PREFIX_VARIANTS {
                    return None;
                }
            }
            Some(())
        }
        HirKind::Repetition(rep) if rep.min == 0 && rep.max == Some(1) => {
            // `X?` → either nothing or X. Duplicate the accumulator and
            // append X to one copy.
            let mut with_x = variants.clone();
            prefix_extend_variants(&mut with_x, &rep.sub)?;
            if variants.len() + with_x.len() > MAX_PREFIX_VARIANTS {
                return None;
            }
            variants.extend(with_x);
            Some(())
        }
        HirKind::Alternation(branches) => {
            let base = std::mem::take(variants);
            for branch in branches {
                let mut local = base.clone();
                prefix_extend_variants(&mut local, branch)?;
                if variants.len() + local.len() > MAX_PREFIX_VARIANTS {
                    return None;
                }
                variants.extend(local);
            }
            Some(())
        }
        _ => None,
    }
}

/// Capture must be a greedy `[^X]+` over a single ASCII byte X.
fn capture_terminator_byte(hir: &regex_syntax::hir::Hir) -> Option<u8> {
    use regex_syntax::hir::HirKind;

    let HirKind::Repetition(rep) = hir.kind() else {
        return None;
    };
    if rep.min < 1 || rep.max.is_some() || !rep.greedy {
        return None;
    }
    let HirKind::Class(class) = rep.sub.kind() else {
        return None;
    };
    single_excluded_ascii_byte(class)
}

/// `.*` for default-flag regexes: any byte except `\n`, zero or more, greedy.
fn is_dot_star(hir: &regex_syntax::hir::Hir) -> bool {
    use regex_syntax::hir::HirKind;

    let HirKind::Repetition(rep) = hir.kind() else {
        return false;
    };
    if rep.min != 0 || rep.max.is_some() || !rep.greedy {
        return false;
    }
    let HirKind::Class(class) = rep.sub.kind() else {
        return false;
    };
    single_excluded_ascii_byte(class) == Some(b'\n')
}

/// Returns `Some(b)` iff `class` matches every Unicode codepoint or byte
/// except a single ASCII byte `b`. We require ASCII because the runtime
/// matcher uses `memchr` over byte slices.
fn single_excluded_ascii_byte(class: &regex_syntax::hir::Class) -> Option<u8> {
    use regex_syntax::hir::Class;

    match class {
        Class::Unicode(uc) => {
            let ranges = uc.ranges();
            if ranges.len() != 2 {
                return None;
            }
            let (r0, r1) = (&ranges[0], &ranges[1]);
            if (r0.start() as u32) != 0 || (r1.end() as u32) != 0x10FFFF {
                return None;
            }
            let gap_start = r0.end() as u32 + 1;
            let gap_end = r1.start() as u32 - 1;
            if gap_start != gap_end || gap_start > 0x7F {
                return None;
            }
            Some(gap_start as u8)
        }
        Class::Bytes(bc) => {
            let ranges = bc.ranges();
            if ranges.len() != 2 {
                return None;
            }
            let (r0, r1) = (&ranges[0], &ranges[1]);
            if r0.start() != 0 || r1.end() != 0xFF {
                return None;
            }
            let gap_start = r0.end() as u16 + 1;
            let gap_end = r1.start() as u16 - 1;
            if gap_start != gap_end || gap_start > 0x7F {
                return None;
            }
            Some(gap_start as u8)
        }
    }
}

/// Replaces substring(s) matching a PCRE-like regular expression.
///
/// The full list of supported features and syntax can be found at
/// <https://docs.rs/regex/latest/regex/#syntax>
///
/// Supported flags with the addition of 'g' can be found at
/// <https://docs.rs/regex/latest/regex/#grouping-and-flags>
///
/// # Examples
///
/// ```ignore
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/data/regex.csv", CsvReadOptions::new()).await?;
///
/// // use the regexp_replace function to replace substring(s) without flags
/// let df = df.with_column(
///     "a",
///     regexp_replace(vec![col("values"), col("patterns"), col("replacement")])
/// )?;
/// // use the regexp_replace function to replace substring(s) with flags
/// let df = df.with_column(
///     "b",
///     regexp_replace(vec![col("values"), col("patterns"), col("replacement"), col("flags")]),
/// )?;
///
/// // literals can be used as well
/// let df = df.with_column(
///     "c",
///     regexp_replace(vec![lit("foobarbequebaz"), lit("(bar)(beque)"), lit(r"\2")]),
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn regexp_replace<'a, T: OffsetSizeTrait, U>(
    string_array: U,
    pattern_array: U,
    replacement_array: U,
    flags_array: Option<U>,
) -> Result<ArrayRef>
where
    U: ArrayAccessor<Item = &'a str>,
{
    // Default implementation for regexp_replace, assumes all args are arrays
    // and args is a sequence of 3 or 4 elements.

    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    let datatype = string_array.data_type().to_owned();

    let string_array_iter = ArrayIter::new(string_array);
    let pattern_array_iter = ArrayIter::new(pattern_array);
    let replacement_array_iter = ArrayIter::new(replacement_array);

    match flags_array {
        None => {
            let result_iter = string_array_iter
                .zip(pattern_array_iter)
                .zip(replacement_array_iter)
                .map(|((string, pattern), replacement)| {
                    match (string, pattern, replacement) {
                        (Some(string), Some(pattern), Some(replacement)) => {
                            let replacement = regex_replace_posix_groups(replacement);
                            // if patterns hashmap already has regexp then use else create and return
                            let re = match patterns.get(pattern) {
                                Some(re) => Ok(re),
                                None => match Regex::new(pattern) {
                                    Ok(re) => {
                                        patterns.insert(pattern.to_string(), re);
                                        Ok(patterns.get(pattern).unwrap())
                                    }
                                    Err(err) => {
                                        Err(DataFusionError::External(Box::new(err)))
                                    }
                                },
                            };

                            Some(re.map(|re| re.replace(string, replacement.as_str())))
                                .transpose()
                        }
                        _ => Ok(None),
                    }
                });

            match datatype {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let result =
                        result_iter.collect::<Result<GenericStringArray<T>>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                DataType::Utf8View => {
                    let result = result_iter.collect::<Result<StringViewArray>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
        Some(flags_array) => {
            let flags_array_iter = ArrayIter::new(flags_array);

            let result_iter = string_array_iter
                .zip(pattern_array_iter)
                .zip(replacement_array_iter)
                .zip(flags_array_iter)
                .map(|(((string, pattern), replacement), flags)| {
                    match (string, pattern, replacement, flags) {
                        (Some(string), Some(pattern), Some(replacement), Some(flags)) => {
                            let replacement = regex_replace_posix_groups(replacement);

                            // format flags into rust pattern
                            let (pattern, replace_all) = if flags == "g" {
                                (pattern.to_string(), true)
                            } else if flags.contains('g') {
                                (
                                    format!(
                                        "(?{}){}",
                                        flags.to_string().replace('g', ""),
                                        pattern
                                    ),
                                    true,
                                )
                            } else {
                                (format!("(?{flags}){pattern}"), false)
                            };

                            // if patterns hashmap already has regexp then use else create and return
                            let re = match patterns.get(&pattern) {
                                Some(re) => Ok(re),
                                None => match Regex::new(pattern.as_str()) {
                                    Ok(re) => {
                                        patterns.insert(pattern.clone(), re);
                                        Ok(patterns.get(&pattern).unwrap())
                                    }
                                    Err(err) => {
                                        Err(DataFusionError::External(Box::new(err)))
                                    }
                                },
                            };

                            Some(re.map(|re| {
                                if replace_all {
                                    re.replace_all(string, replacement.as_str())
                                } else {
                                    re.replace(string, replacement.as_str())
                                }
                            }))
                            .transpose()
                        }
                        _ => Ok(None),
                    }
                });

            match datatype {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let result =
                        result_iter.collect::<Result<GenericStringArray<T>>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                DataType::Utf8View => {
                    let result = result_iter.collect::<Result<StringViewArray>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
    }
}

/// Get the first argument from the given string array.
///
/// Note: If the array is empty or the first argument is null,
/// then aborts early.
macro_rules! fetch_string_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_SIZE:expr) => {{
        let string_array_type = ($ARG).data_type();
        match string_array_type {
            dt if $ARG.len() == 0 || $ARG.is_null(0) => {
                // Mimicking the existing behavior of regexp_replace, if any of the scalar arguments
                // are actually null, then the result will be an array of the same size as the first argument with all nulls.
                //
                // Also acts like an early abort mechanism when the input array is empty.
                return Ok(new_null_array(dt, $ARRAY_SIZE));
            }
            DataType::Utf8 => {
                let array = as_string_array($ARG)?;
                array.value(0)
            }
            DataType::LargeUtf8 => {
                let array = as_large_string_array($ARG)?;
                array.value(0)
            }
            DataType::Utf8View => {
                let array = as_string_view_array($ARG)?;
                array.value(0)
            }
            _ => unreachable!(
                "Invalid data type for regexp_replace: {}",
                string_array_type
            ),
        }
    }};
}
/// Special cased regex_replace implementation for the scenario where
/// the pattern, replacement and flags are static (arrays that are derived
/// from scalars). This means we can skip regex caching system and basically
/// hold a single Regex object for the replace operation. This also speeds
/// up the pre-processing time of the replacement string, since it only
/// needs to processed once.
fn regexp_replace_static_pattern_replace<T: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let array_size = args[0].len();
    let pattern = fetch_string_arg!(&args[1], "pattern", array_size);
    let replacement = fetch_string_arg!(&args[2], "replacement", array_size);
    let flags = match args.len() {
        3 => None,
        4 => Some(fetch_string_arg!(&args[3], "flags", array_size)),
        other => {
            return exec_err!(
                "regexp_replace was called with {other} arguments. It requires at least 3 and at most 4."
            );
        }
    };

    // Embed the flag (if it exists) into the pattern. Limit will determine
    // whether this is a global match (as in replace all) or just a single
    // replace operation.
    let (pattern, limit) = match flags {
        Some("g") => (pattern.to_string(), 0),
        Some(flags) => (
            format!("(?{}){}", flags.to_string().replace('g', ""), pattern),
            !flags.contains('g') as usize,
        ),
        None => (pattern.to_string(), 1),
    };

    let re =
        Regex::new(&pattern).map_err(|err| DataFusionError::External(Box::new(err)))?;

    // Replaces the posix groups in the replacement string
    // with rust ones.
    let replacement = regex_replace_posix_groups(replacement);

    let mut opt_re = OptimizedRegex::new(re, limit, &pattern, &replacement);

    let string_array_type = args[0].data_type();
    match string_array_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;

            // We are going to create the underlying string buffer from its parts
            // to be able to re-use the existing null buffer for sparse arrays.
            let mut vals = BufferBuilder::<u8>::new({
                let offsets = string_array.value_offsets();
                (offsets[string_array.len()] - offsets[0])
                    .to_usize()
                    .unwrap()
            });
            let mut new_offsets = BufferBuilder::<T>::new(string_array.len() + 1);
            new_offsets.append(T::zero());

            string_array.iter().for_each(|val| {
                if let Some(val) = val {
                    let result = opt_re.replacen(val, limit, replacement.as_str());
                    vals.append_slice(result.as_bytes());
                }
                new_offsets.append(T::from_usize(vals.len()).unwrap());
            });

            let data = ArrayDataBuilder::new(GenericStringArray::<T>::DATA_TYPE)
                .len(string_array.len())
                .nulls(string_array.nulls().cloned())
                .buffers(vec![new_offsets.finish(), vals.finish()])
                .build()?;
            let result_array = GenericStringArray::<T>::from(data);
            Ok(Arc::new(result_array) as ArrayRef)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;

            let mut builder = StringViewBuilder::with_capacity(string_view_array.len());

            for val in string_view_array.iter() {
                if let Some(val) = val {
                    let result = opt_re.replacen(val, limit, replacement.as_str());
                    builder.append_value(result.as_ref());
                } else {
                    builder.append_null();
                }
            }

            let result = builder.finish();
            Ok(Arc::new(result) as ArrayRef)
        }
        _ => unreachable!(
            "Invalid data type for regexp_replace: {}",
            string_array_type
        ),
    }
}

/// Determine which implementation of the regexp_replace to use based
/// on the given set of arguments.
fn specialize_regexp_replace<T: OffsetSizeTrait>(
    args: &[ColumnarValue],
) -> Result<ArrayRef> {
    // This will serve as a dispatch table where we can
    // leverage it in order to determine whether the scalarity
    // of the given set of arguments fits a better specialized
    // function.
    let (is_source_scalar, is_pattern_scalar, is_replacement_scalar, is_flags_scalar) = (
        matches!(args[0], ColumnarValue::Scalar(_)),
        matches!(args[1], ColumnarValue::Scalar(_)),
        matches!(args[2], ColumnarValue::Scalar(_)),
        // The forth argument (flags) is optional; so in the event that
        // it is not available, we'll claim that it is scalar.
        matches!(args.get(3), Some(ColumnarValue::Scalar(_)) | None),
    );
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });
    let inferred_length = len.unwrap_or(1);
    match (
        is_source_scalar,
        is_pattern_scalar,
        is_replacement_scalar,
        is_flags_scalar,
    ) {
        // This represents a very hot path for the case where the there is
        // a single pattern that is being matched against and a single replacement.
        // This is extremely important to specialize on since it removes the overhead
        // of DF's in-house regex pattern cache (since there will be at most a single
        // pattern) and the pre-processing of the same replacement pattern at each
        // query.
        //
        // The flags needs to be a scalar as well since each pattern is actually
        // constructed with the flags embedded into the pattern itself. This means
        // even if the pattern itself is scalar, if the flags are an array then
        // we will create many regexes and it is best to use the implementation
        // that caches it. If there are no flags, we can simply ignore it here,
        // and let the specialized function handle it.
        (_, true, true, true) => {
            let hints = [
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
            ];
            let args = args
                .iter()
                .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
                .map(|(arg, hint)| {
                    // Decide on the length to expand this scalar to depending
                    // on the given hints.
                    let expansion_len = match hint {
                        Hint::AcceptsSingular => 1,
                        Hint::Pad => inferred_length,
                    };
                    arg.to_array(expansion_len)
                })
                .collect::<Result<Vec<_>>>()?;
            regexp_replace_static_pattern_replace::<T>(&args)
        }

        // If there are no specialized implementations, we'll fall back to the
        // generic implementation.
        (_, _, _, _) => {
            let args = args
                .iter()
                .map(|arg| arg.to_array(inferred_length))
                .collect::<Result<Vec<_>>>()?;

            match (
                args[0].data_type(),
                args[1].data_type(),
                args[2].data_type(),
                args.get(3).map(|a| a.data_type()),
            ) {
                (
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    Some(DataType::Utf8) | None,
                ) => {
                    let string_array = args[0].as_string::<i32>();
                    let pattern_array = args[1].as_string::<i32>();
                    let replacement_array = args[2].as_string::<i32>();
                    let flags_array = args.get(3).map(|a| a.as_string::<i32>());
                    regexp_replace::<i32, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        flags_array,
                    )
                }
                (
                    DataType::Utf8View,
                    DataType::Utf8View,
                    DataType::Utf8View,
                    Some(DataType::Utf8View) | None,
                ) => {
                    let string_array = args[0].as_string_view();
                    let pattern_array = args[1].as_string_view();
                    let replacement_array = args[2].as_string_view();
                    let flags_array = args.get(3).map(|a| a.as_string_view());
                    regexp_replace::<i32, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        flags_array,
                    )
                }
                (
                    DataType::LargeUtf8,
                    DataType::LargeUtf8,
                    DataType::LargeUtf8,
                    Some(DataType::LargeUtf8) | None,
                ) => {
                    let string_array = args[0].as_string::<i64>();
                    let pattern_array = args[1].as_string::<i64>();
                    let replacement_array = args[2].as_string::<i64>();
                    let flags_array = args.get(3).map(|a| a.as_string::<i64>());
                    regexp_replace::<i64, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        flags_array,
                    )
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use arrow::array::*;

    use super::*;

    #[test]
    fn test_regex_replace_posix_groups() {
        // Test that \1, \2, etc. are replaced with ${1}, ${2}, etc.
        assert_eq!(regex_replace_posix_groups(r"\1"), "${1}");
        assert_eq!(regex_replace_posix_groups(r"\12"), "${12}");
        assert_eq!(regex_replace_posix_groups(r"X\1Y"), "X${1}Y");
        assert_eq!(regex_replace_posix_groups(r"\1\2"), "${1}${2}");

        // Test double backslash (from SQL escaped strings like '\\1')
        assert_eq!(regex_replace_posix_groups(r"\\1"), "${1}");
        assert_eq!(regex_replace_posix_groups(r"X\\1Y"), "X${1}Y");
        assert_eq!(regex_replace_posix_groups(r"\\1\\2"), "${1}${2}");

        // Test 3 or 4 backslashes before digits to document expected behavior
        assert_eq!(regex_replace_posix_groups(r"\\\1"), r"\${1}");
        assert_eq!(regex_replace_posix_groups(r"\\\\1"), r"\\${1}");
        assert_eq!(regex_replace_posix_groups(r"\\\1\\\\2"), r"\${1}\\${2}");

        // Test that a lone backslash is NOT replaced (requires at least one digit)
        assert_eq!(regex_replace_posix_groups(r"\"), r"\");
        assert_eq!(regex_replace_posix_groups(r"foo\bar"), r"foo\bar");

        // Test that backslash followed by non-digit is preserved
        assert_eq!(regex_replace_posix_groups(r"\n"), r"\n");
        assert_eq!(regex_replace_posix_groups(r"\t"), r"\t");

        // Test \0 behavior: \0 is converted to ${0}, which in Rust's regex
        // replacement syntax substitutes the entire match. This is consistent
        // with POSIX behavior where \0 (or &) refers to the entire matched string.
        assert_eq!(regex_replace_posix_groups(r"\0"), "${0}");
        assert_eq!(
            regex_replace_posix_groups(r"prefix\0suffix"),
            "prefix${0}suffix"
        );
    }

    macro_rules! static_pattern_regexp_replace {
        ($name:ident, $T:ty, $O:ty) => {
            #[test]
            fn $name() {
                let values = vec!["abc", "acd", "abcd1234567890123", "123456789012abc"];
                let patterns = vec!["b"; 4];
                let replacement = vec!["foo"; 4];
                let expected =
                    vec!["afooc", "acd", "afoocd1234567890123", "123456789012afooc"];

                let values = <$T>::from(values);
                let patterns = <$T>::from(patterns);
                let replacements = <$T>::from(replacement);
                let expected = <$T>::from(expected);

                let re = regexp_replace_static_pattern_replace::<$O>(&[
                    Arc::new(values),
                    Arc::new(patterns),
                    Arc::new(replacements),
                ])
                .unwrap();

                assert_eq!(re.as_ref(), &expected);
            }
        };
    }

    static_pattern_regexp_replace!(string_array, StringArray, i32);
    static_pattern_regexp_replace!(string_view_array, StringViewArray, i32);
    static_pattern_regexp_replace!(large_string_array, LargeStringArray, i64);

    macro_rules! static_pattern_regexp_replace_with_flags {
        ($name:ident, $T:ty, $O: ty) => {
            #[test]
            fn $name() {
                let values = vec![
                    "abc",
                    "aBc",
                    "acd",
                    "abcd1234567890123",
                    "aBcd1234567890123",
                    "123456789012abc",
                    "123456789012aBc",
                ];
                let expected = vec![
                    "afooc",
                    "afooc",
                    "acd",
                    "afoocd1234567890123",
                    "afoocd1234567890123",
                    "123456789012afooc",
                    "123456789012afooc",
                ];

                let values = <$T>::from(values);
                let patterns = StringArray::from(vec!["b"; 7]);
                let replacements = StringArray::from(vec!["foo"; 7]);
                let flags = StringArray::from(vec!["i"; 5]);
                let expected = <$T>::from(expected);

                let re = regexp_replace_static_pattern_replace::<$O>(&[
                    Arc::new(values),
                    Arc::new(patterns),
                    Arc::new(replacements),
                    Arc::new(flags),
                ])
                .unwrap();

                assert_eq!(re.as_ref(), &expected);
            }
        };
    }

    static_pattern_regexp_replace_with_flags!(string_array_with_flags, StringArray, i32);
    static_pattern_regexp_replace_with_flags!(
        string_view_array_with_flags,
        StringViewArray,
        i32
    );
    static_pattern_regexp_replace_with_flags!(
        large_string_array_with_flags,
        LargeStringArray,
        i64
    );

    #[test]
    fn test_static_pattern_regexp_replace_early_abort() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec![None::<&str>; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let expected = StringArray::from(vec![None::<&str>; 5]);

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_early_abort_when_empty() {
        let values = StringArray::from(Vec::<Option<&str>>::new());
        let patterns = StringArray::from(Vec::<Option<&str>>::new());
        let replacements = StringArray::from(Vec::<Option<&str>>::new());
        let expected = StringArray::from(Vec::<Option<&str>>::new());

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_early_abort_flags() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec!["a"; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let flags = StringArray::from(vec![None::<&str>; 5]);
        let expected = StringArray::from(vec![None::<&str>; 5]);

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
            Arc::new(flags),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_pattern_error() {
        let values = StringArray::from(vec!["abc"; 5]);
        // Deliberately using an invalid pattern to see how the single pattern
        // error is propagated on regexp_replace.
        let patterns = StringArray::from(vec!["["; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ]);
        let pattern_err = re.expect_err("broken pattern should have failed");
        assert_eq!(
            pattern_err.strip_backtrace(),
            "External error: regex parse error:\n    [\n    ^\nerror: unclosed character class"
        );
    }

    #[test]
    fn test_static_pattern_regexp_replace_with_null_buffers() {
        let values = StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            None,
            Some("a"),
            None,
            None,
            Some("c"),
        ]);
        let patterns = StringArray::from(vec!["a"; 1]);
        let replacements = StringArray::from(vec!["foo"; 1]);
        let expected = StringArray::from(vec![
            Some("foo"),
            None,
            Some("b"),
            None,
            Some("foo"),
            None,
            None,
            Some("c"),
        ]);

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
        assert_eq!(re.null_count(), 4);
    }

    #[test]
    fn test_static_pattern_regexp_replace_with_sliced_null_buffer() {
        let values = StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            None,
            Some("a"),
            None,
            None,
            Some("c"),
        ]);
        let values = values.slice(2, 5);
        let patterns = StringArray::from(vec!["a"; 1]);
        let replacements = StringArray::from(vec!["foo"; 1]);
        let expected = StringArray::from(vec![Some("b"), None, Some("foo"), None, None]);

        let re = regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
        assert_eq!(re.null_count(), 3);
    }

    /// Assert that the fast path was selected for `pattern`.
    fn assert_literal_prefix_capture(
        pattern: &str,
        expected_prefixes: &[&[u8]],
        expected_terminator: u8,
    ) {
        let spec =
            try_recognize_literal_prefix_capture(pattern).unwrap_or_else(|| {
                panic!("expected literal-prefix recognizer to accept {pattern}")
            });
        assert_eq!(spec.terminator, expected_terminator, "pattern {pattern}");
        let actual: Vec<&[u8]> = spec.prefixes.iter().map(|p| &p[..]).collect();
        assert_eq!(actual, expected_prefixes, "pattern {pattern}");
    }

    /// Run the optimized path and the full regex on every value and confirm they
    /// agree. Acts as a generic differential test for the fast path.
    fn assert_optimized_matches_regex(pattern: &str, values: &[&str]) {
        let re = Regex::new(pattern).unwrap();
        let mut opt = OptimizedRegex::new(re.clone(), 1, pattern, "${1}");
        for value in values {
            let expected = re.replacen(value, 1, "${1}");
            let actual = opt.replacen(value, 1, "${1}");
            assert_eq!(actual, expected, "pattern {pattern}, value {value:?}");
        }
    }

    #[test]
    fn literal_prefix_recognizer_accepts_clickbench_q28() {
        assert_literal_prefix_capture(
            r"^https?://(?:www\.)?([^/]+)/.*$",
            &[
                b"https://www.",
                b"http://www.",
                b"https://",
                b"http://",
            ],
            b'/',
        );
    }

    #[test]
    fn literal_prefix_recognizer_accepts_single_literal() {
        assert_literal_prefix_capture(r"^foo:([^,]+),.*$", &[b"foo:"], b',');
    }

    #[test]
    fn literal_prefix_recognizer_accepts_alternation() {
        // Equal-length prefixes end up in dedupe (alphabetical) order after
        // the descending-length sort, which is stable.
        assert_literal_prefix_capture(
            r"^(?:foo|bar|baz):([^/]+)/.*$",
            &[b"bar:", b"baz:", b"foo:"],
            b'/',
        );
    }

    #[test]
    fn literal_prefix_recognizer_rejects_non_anchored() {
        assert!(try_recognize_literal_prefix_capture(r"https?://([^/]+)/.*$").is_none());
    }

    #[test]
    fn literal_prefix_recognizer_rejects_unbounded_prefix() {
        assert!(
            try_recognize_literal_prefix_capture(r"^.+?:([^/]+)/.*$").is_none()
        );
    }

    #[test]
    fn literal_prefix_recognizer_rejects_non_ascii_terminator() {
        assert!(
            try_recognize_literal_prefix_capture(r"^a([^\u{0080}]+)\u{0080}.*$")
                .is_none()
        );
    }

    #[test]
    fn literal_prefix_recognizer_rejects_case_insensitive() {
        // `(?i)` folds the negated class to two bytes, which would break the
        // single-byte memchr terminator assumption.
        assert!(
            try_recognize_literal_prefix_capture(r"^(?i)foo:([^a]+)a.*$")
                .is_none()
        );
    }

    #[test]
    fn literal_prefix_fast_path_matches_full_regex_for_q28_pattern() {
        assert_optimized_matches_regex(
            r"^https?://(?:www\.)?([^/]+)/.*$",
            &[
                "http://example.com/path",
                "https://www.example.com/path",
                "http://www./path",
                "http://wwww.example.com/path",
                "http://example.com/path\nnext",
                "http://example.com/path\n",
                "http://exa\nmple.com/path",
                "https://example.com",
                "ftp://example.com/path",
                "http:///path",
                "",
                "/",
                "http://example.com/",
            ],
        );
    }

    #[test]
    fn literal_prefix_fast_path_matches_full_regex_for_alternation_pattern() {
        assert_optimized_matches_regex(
            r"^(?:foo|bar|baz):([^/]+)/.*$",
            &[
                "foo:one/two",
                "bar:value/",
                "baz:x/y/z",
                "qux:one/two",
                "foo:/empty",
                "foo:abc",
                "",
            ],
        );
    }
}

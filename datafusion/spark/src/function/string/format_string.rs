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

use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

use core::num::FpCategory;

use arrow::{
    array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray},
    datatypes::{DataType, Field, FieldRef},
};
use bigdecimal::{
    BigDecimal, ToPrimitive,
    num_bigint::{BigInt, Sign},
};
use chrono::{DateTime, Datelike, Timelike, Utc};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, plan_err,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// Spark-compatible `format_string` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#format_string>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FormatStringFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for FormatStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
            aliases: vec![String::from("printf")],
        }
    }
}

impl ScalarUDFImpl for FormatStringFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format_string"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called, use return_field_from_args instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields[0].data_type() {
            DataType::Null => {
                Ok(Arc::new(Field::new("format_string", DataType::Utf8, true)))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(Arc::clone(&args.arg_fields[0]))
            }
            _ => exec_err!(
                "format_string expects the first argument to be Utf8, LargeUtf8 or Utf8View, got {} instead",
                args.arg_fields[0].data_type()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let len = args.args.iter().find_map(|arg| match arg {
            ColumnarValue::Scalar(_) => None,
            ColumnarValue::Array(a) => Some(a.len()),
        });
        let is_scalar = len.is_none();
        let data_types = args.args[1..]
            .iter()
            .map(|arg| arg.data_type())
            .collect::<Vec<_>>();
        let fmt_type = args.args[0].data_type();

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(fmt)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(fmt))) => {
                let formatter = Formatter::parse(fmt, &data_types)?;
                let mut result = Vec::with_capacity(len.unwrap_or(1));
                for i in 0..len.unwrap_or(1) {
                    let scalars = args.args[1..]
                        .iter()
                        .map(|arg| try_to_scalar(arg.clone(), i))
                        .collect::<Result<Vec<_>>>()?;
                    let formatted = formatter.format(&scalars)?;
                    result.push(formatted);
                }
                if is_scalar {
                    let scalar_result = result.pop().unwrap();
                    match fmt_type {
                        DataType::Utf8 => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                            Some(scalar_result),
                        ))),
                        DataType::LargeUtf8 => Ok(ColumnarValue::Scalar(
                            ScalarValue::LargeUtf8(Some(scalar_result)),
                        )),
                        DataType::Utf8View => Ok(ColumnarValue::Scalar(
                            ScalarValue::Utf8View(Some(scalar_result)),
                        )),
                        _ => unreachable!(),
                    }
                } else {
                    let array: ArrayRef = match fmt_type {
                        DataType::Utf8 => Arc::new(StringArray::from(result)),
                        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(result)),
                        DataType::Utf8View => Arc::new(StringViewArray::from(result)),
                        _ => unreachable!(),
                    };
                    Ok(ColumnarValue::Array(array))
                }
            }
            ColumnarValue::Array(fmts) => {
                let mut result = Vec::with_capacity(len.unwrap());
                for i in 0..len.unwrap() {
                    let fmt = ScalarValue::try_from_array(fmts, i)?;
                    match fmt.try_as_str() {
                        Some(Some(fmt)) => {
                            let formatter = Formatter::parse(fmt, &data_types)?;
                            let scalars = args.args[1..]
                                .iter()
                                .map(|arg| try_to_scalar(arg.clone(), i))
                                .collect::<Result<Vec<_>>>()?;
                            let formatted = formatter.format(&scalars)?;
                            result.push(Some(formatted));
                        }
                        Some(None) => {
                            result.push(None);
                        }
                        _ => unreachable!(),
                    }
                }
                let array: ArrayRef = match fmt_type {
                    DataType::Utf8 => Arc::new(StringArray::from(result)),
                    DataType::LargeUtf8 => Arc::new(LargeStringArray::from(result)),
                    DataType::Utf8View => Arc::new(StringViewArray::from(result)),
                    _ => unreachable!(),
                };
                Ok(ColumnarValue::Array(array))
            }
            _ => exec_err!(
                "The format_string function expects the first argument to be a string"
            ),
        }
    }
}

fn try_to_scalar(arg: ColumnarValue, index: usize) -> Result<ScalarValue> {
    match arg {
        ColumnarValue::Scalar(scalar) => Ok(scalar),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, index),
    }
}

/// Compatible with `java.util.Formatter`
#[derive(Debug)]
pub struct Formatter<'a> {
    pub elements: Vec<FormatElement<'a>>,
    pub arg_num: usize,
}

impl<'a> Formatter<'a> {
    pub fn new(elements: Vec<FormatElement<'a>>) -> Self {
        let arg_num = elements
            .iter()
            .map(|element| match element {
                FormatElement::Format(spec) => spec.argument_index,
                _ => 0,
            })
            .max()
            .unwrap_or(0);
        Self { elements, arg_num }
    }

    /// Parses a printf-style format string into a Formatter with validation.
    ///
    /// This method implements a comprehensive parser for Java `java.util.Formatter` syntax,
    /// processing the format string character by character to identify and validate format
    /// specifiers against the provided argument types.
    ///
    /// # Arguments
    ///
    /// * `fmt` - The format string containing literal text and format specifiers
    /// * `arg_types` - Array of DataFusion DataTypes corresponding to the arguments
    ///
    /// # Parsing Process
    ///
    /// The parser operates in several phases:
    ///
    /// 1. **String Scanning**: Iterates through the format string looking for '%' characters
    ///    that mark the beginning of format specifiers or special sequences.
    ///
    /// 2. **Special Sequence Handling**: Processes escape sequences:
    ///    - `%%` becomes a literal '%' character
    ///    - `%n` becomes a newline character
    ///    - `%<` indicates reuse of the previous argument with a new format specifier
    ///
    /// 3. **Argument Index Resolution**: Determines which argument each format specifier refers to:
    ///    - Sequential indexing: arguments are consumed in order (1, 2, 3, ...)
    ///    - Positional indexing: explicit argument position using `%n$` syntax
    ///    - Previous argument reuse: `%<` references the last used argument
    ///
    /// 4. **Format Specifier Parsing**: For each format specifier, extracts:
    ///    - Flags (-, +, space, #, 0, ',', '(')
    ///    - Width specification (minimum field width)
    ///    - Precision specification (decimal places or maximum characters)
    ///    - Conversion type (d, s, f, x, etc.)
    ///
    /// 5. **Type Validation**: Verifies that each format specifier's conversion type
    ///    is compatible with the corresponding argument's DataType. For example:
    ///    - Integer conversions (%d, %x, %o) require integer DataTypes
    ///    - String conversions (%s, %S) accept any DataType
    ///    - Float conversions (%f, %e, %g) require numeric DataTypes
    ///
    /// 6. **Element Construction**: Creates FormatElement instances for:
    ///    - Verbatim text sections (copied directly to output)
    ///    - Validated format specifiers with their parsed parameters
    ///
    /// # Internal State Management
    ///
    /// The parser maintains several state variables:
    /// - `argument_index`: Tracks the current sequential argument position
    /// - `prev`: Remembers the last used argument index for `%<` references
    /// - `res`: Accumulates the parsed FormatElement instances
    /// - `rem`: Points to the remaining unparsed portion of the format string
    ///
    /// # Validation and Error Handling
    ///
    /// The parser performs extensive validation including:
    /// - Argument index bounds checking against the provided arg_types array
    /// - Format specifier syntax validation
    /// - Type compatibility verification between conversion types and DataTypes
    /// - Detection of malformed numeric parameters and invalid flag combinations
    ///
    /// # Returns
    ///
    /// Returns a Formatter containing the parsed elements and the maximum argument
    /// index encountered, enabling efficient argument validation during formatting.
    pub fn parse(fmt: &'a str, arg_types: &[DataType]) -> Result<Self> {
        // find the first %
        let mut res = Vec::new();

        let mut rem = fmt;
        let mut argument_index = 0;

        let mut prev: Option<usize> = None;

        while !rem.is_empty() {
            if let Some((verbatim_prefix, rest)) = rem.split_once('%') {
                if !verbatim_prefix.is_empty() {
                    res.push(FormatElement::Verbatim(verbatim_prefix));
                }
                if let Some(rest) = rest.strip_prefix('%') {
                    res.push(FormatElement::Verbatim("%"));
                    rem = rest;
                    continue;
                }
                if let Some(rest) = rest.strip_prefix('n') {
                    res.push(FormatElement::Verbatim("\n"));
                    rem = rest;
                    continue;
                }
                if let Some(rest) = rest.strip_prefix('<') {
                    // %< means reuse the previous argument
                    let Some(p) = prev else {
                        return exec_err!("No previous argument to reference");
                    };
                    let (spec, rest) =
                        take_conversion_specifier(rest, p, &arg_types[p - 1])?;
                    res.push(FormatElement::Format(spec));
                    rem = rest;
                    continue;
                }

                let (current_argument_index, rest2) = take_numeric_param(rest, false);
                let (current_argument_index, rest) =
                    match (current_argument_index, rest2.starts_with('$')) {
                        (NumericParam::Literal(index), true) => {
                            (index as usize, &rest2[1..])
                        }
                        (NumericParam::FromArgument, true) => {
                            return exec_err!("Invalid numeric parameter");
                        }
                        (_, false) => {
                            argument_index += 1;
                            (argument_index, rest)
                        }
                    };
                if current_argument_index == 0 || current_argument_index > arg_types.len()
                {
                    return exec_err!(
                        "Argument index {} is out of bounds",
                        current_argument_index
                    );
                }

                let (spec, rest) = take_conversion_specifier(
                    rest,
                    current_argument_index,
                    &arg_types[current_argument_index - 1],
                )
                .map_err(|e| exec_datafusion_err!("{:?}, format string: {:?}", e, fmt))?;
                res.push(FormatElement::Format(spec));
                prev = Some(spec.argument_index);
                rem = rest;
            } else {
                res.push(FormatElement::Verbatim(rem));
                break;
            }
        }

        Ok(Self::new(res))
    }

    pub fn format(&self, args: &[ScalarValue]) -> Result<String> {
        if args.len() < self.arg_num {
            return exec_err!(
                "Expected at least {} arguments, got {}",
                self.arg_num,
                args.len()
            );
        }
        let mut string = String::new();
        for element in &self.elements {
            match element {
                FormatElement::Verbatim(text) => {
                    string.push_str(text);
                }
                FormatElement::Format(spec) => {
                    spec.format(&mut string, &args[spec.argument_index - 1])?;
                }
            }
        }
        Ok(string)
    }
}

#[derive(Debug)]
pub enum FormatElement<'a> {
    /// Some characters that are copied to the output as-is
    Verbatim(&'a str),
    /// A format specifier
    Format(ConversionSpecifier),
}

/// Parsed printf conversion specifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionSpecifier {
    pub argument_index: usize,
    /// flag `#`: use `0x`, etc?
    pub alt_form: bool,
    /// flag `0`: left-pad with zeros?
    pub zero_pad: bool,
    /// flag `-`: left-adjust (pad with spaces on the right)
    pub left_adj: bool,
    /// flag `' '` (space): indicate sign with a space?
    pub space_sign: bool,
    /// flag `+`: Always show sign? (for signed numbers)
    pub force_sign: bool,
    /// flag `,`: include locale-specific grouping separators
    pub grouping_separator: bool,
    /// flag `(`: enclose negative numbers in parentheses
    pub negative_in_parentheses: bool,
    /// field width
    pub width: NumericParam,
    /// floating point field precision
    pub precision: NumericParam,
    /// data type
    pub conversion_type: ConversionType,
}

/// Width / precision parameter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericParam {
    /// The literal width
    Literal(i32),
    /// Get the width from the previous argument
    FromArgument,
}

/// Printf data type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionType {
    /// `B`
    BooleanUpper,
    /// `b`
    BooleanLower,
    /// Not implemented yet. Can be implemented after <https://github.com/apache/datafusion/pull/17093> is merged
    /// `h`
    HexHashLower,
    /// `H`
    HexHashUpper,
    /// `d`
    DecInt,
    /// `o`
    OctInt,
    /// `x`
    HexIntLower,
    /// `X`
    HexIntUpper,
    /// `e`
    SciFloatLower,
    /// `E`
    SciFloatUpper,
    /// `f`
    DecFloatLower,
    /// `g`
    CompactFloatLower,
    /// `G`
    CompactFloatUpper,
    /// `a`
    HexFloatLower,
    /// `A`
    HexFloatUpper,
    /// `t`
    TimeLower(TimeFormat),
    /// `T`
    TimeUpper(TimeFormat),
    /// `c`
    CharLower,
    /// `C`
    CharUpper,
    /// `s`
    StringLower,
    /// `S`
    StringUpper,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeFormat {
    // Hour of the day for the 24-hour clock,
    // formatted as two digits with a leading zero as necessary i.e. 00 - 23. 00 corresponds to midnight.
    HUpper,
    // Hour for the 12-hour clock,
    // formatted as two digits with a leading zero as necessary, i.e. 01 - 12. 01 corresponds to one o'clock (either morning or afternoon).
    IUpper,
    // Hour of the day for the 24-hour clock,
    // i.e. 0 - 23. 0 corresponds to midnight.
    KLower,
    // Hour for the 12-hour clock,
    // i.e. 1 - 12. 1 corresponds to one o'clock (either morning or afternoon).
    LLower,
    // Minute within the hour formatted as two digits with a leading zero as necessary, i.e. 00 - 59.
    MUpper,
    // Seconds within the minute, formatted as two digits with a leading zero as necessary,
    // i.e. 00 - 60 ("60" is a special value required to support leap seconds).
    SUpper,
    // Millisecond within the second formatted as three digits with leading zeros as necessary, i.e. 000 - 999.
    LUpper,
    // Nanosecond within the second, formatted as nine digits with leading zeros as necessary,
    // i.e. 000000000 - 999999999. The precision of this value is limited by the resolution of the underlying operating system or hardware.
    NUpper,
    // Locale-specific morning or afternoon marker in lower case, e.g."am" or "pm".
    // Use of the conversion prefix 'T' forces this output to upper case. (Note that 'p' produces lower-case output.
    // This is different from GNU date and POSIX strftime(3c) which produce upper-case output.)
    PLower,
    // RFC 822 style numeric time zone offset from GMT,
    // e.g. -0800. This value will be adjusted as necessary for Daylight Saving Time.
    // For long, Long, and Date the time zone used is the default time zone for this instance of the Java virtual machine.
    ZLower,
    // A string representing the abbreviation for the time zone. This value will be adjusted as necessary for Daylight Saving Time.
    // For long, Long, and Date the time zone used is the default time zone for this instance of the Java virtual machine.
    // The Formatter's locale will supersede the locale of the argument (if any).
    ZUpper,
    // Seconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC,
    // i.e. Long.MIN_VALUE/1000 to Long.MAX_VALUE/1000.
    SLower,
    // Milliseconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC,
    // i.e. Long.MIN_VALUE to Long.MAX_VALUE. The precision of this value is limited by the resolution of the underlying operating system or hardware.
    QUpper,
    // Locale-specific full month name, e.g. "January", "February".
    BUpper,
    // Locale-specific abbreviated month name, e.g. "Jan", "Feb".
    BLower,
    // Locale-specific full weekday name, e.g. "Monday", "Tuesday".
    AUpper,
    // Locale-specific abbreviated weekday name, e.g. "Mon", "Tue".
    ALower,
    // Four-digit year divided by 100, formatted as two digits with leading zero as necessary, i.e. 00 - 99
    CUpper,
    // Year, formatted to at least four digits with leading zeros as necessary, e.g. 0092 equals 92 CE for the Gregorian calendar.
    YUpper,
    // Last two digits of the year, formatted with leading zeros as necessary, i.e. 00 - 99.
    YLower,
    // Day of year, formatted as three digits with leading zeros as necessary, e.g. 001 - 366 for the Gregorian calendar. 001 corresponds to the first day of the year.
    JLower,
    // Month, formatted as two digits with leading zeros as necessary, i.e. 01 - 13, where "01" is the first month of the year and ("13" is a special value required to support lunar calendars).
    MLower,
    // Day of month, formatted as two digits with leading zeros as necessary, i.e. 01 - 31, where "01" is the first day of the month.
    DLower,
    // Day of month, formatted as two digits, i.e. 1 - 31 where "1" is the first day of the month.
    ELower,
    // Time formatted for the 24-hour clock as "%tH:%tM"
    RUpper,
    // Time formatted for the 24-hour clock as "%tH:%tM:%tS"
    TUpper,
    // Time formatted for the 12-hour clock as "%tI:%tM:%tS %Tp". The location of the morning or afternoon marker ('%Tp') may be locale-dependent.
    RLower,
    // Date formatted as "%tm/%td/%ty"
    DUpper,
    // ISO 8601 complete date formatted as "%tY-%tm-%td"
    FUpper,
    // Date and time formatted as "%ta %tb %td %tT %tZ %tY", e.g. "Sun Jul 20 16:17:00 EDT 1969"
    CLower,
}

impl TryFrom<char> for TimeFormat {
    type Error = DataFusionError;
    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            'H' => Ok(TimeFormat::HUpper),
            'I' => Ok(TimeFormat::IUpper),
            'k' => Ok(TimeFormat::KLower),
            'l' => Ok(TimeFormat::LLower),
            'M' => Ok(TimeFormat::MUpper),
            'S' => Ok(TimeFormat::SUpper),
            'L' => Ok(TimeFormat::LUpper),
            'N' => Ok(TimeFormat::NUpper),
            'p' => Ok(TimeFormat::PLower),
            'z' => Ok(TimeFormat::ZLower),
            'Z' => Ok(TimeFormat::ZUpper),
            's' => Ok(TimeFormat::SLower),
            'Q' => Ok(TimeFormat::QUpper),
            'B' => Ok(TimeFormat::BUpper),
            'b' | 'h' => Ok(TimeFormat::BLower),
            'A' => Ok(TimeFormat::AUpper),
            'a' => Ok(TimeFormat::ALower),
            'C' => Ok(TimeFormat::CUpper),
            'Y' => Ok(TimeFormat::YUpper),
            'y' => Ok(TimeFormat::YLower),
            'j' => Ok(TimeFormat::JLower),
            'm' => Ok(TimeFormat::MLower),
            'd' => Ok(TimeFormat::DLower),
            'e' => Ok(TimeFormat::ELower),
            'R' => Ok(TimeFormat::RUpper),
            'T' => Ok(TimeFormat::TUpper),
            'r' => Ok(TimeFormat::RLower),
            'D' => Ok(TimeFormat::DUpper),
            'F' => Ok(TimeFormat::FUpper),
            'c' => Ok(TimeFormat::CLower),
            _ => exec_err!("Invalid time format: {}", value),
        }
    }
}

impl ConversionType {
    pub fn validate(&self, arg_type: &DataType) -> Result<()> {
        match self {
            ConversionType::BooleanLower | ConversionType::BooleanUpper => {
                if !matches!(arg_type, DataType::Boolean) {
                    return exec_err!(
                        "Invalid argument type for boolean conversion: {:?}",
                        arg_type
                    );
                }
            }
            ConversionType::CharLower | ConversionType::CharUpper => {
                if !matches!(
                    arg_type,
                    DataType::Int8
                        | DataType::UInt8
                        | DataType::Int16
                        | DataType::UInt16
                        | DataType::Int32
                        | DataType::UInt32
                        | DataType::Int64
                        | DataType::UInt64
                ) {
                    return exec_err!(
                        "Invalid argument type for char conversion: {:?}",
                        arg_type
                    );
                }
            }
            ConversionType::DecInt
            | ConversionType::OctInt
            | ConversionType::HexIntLower
            | ConversionType::HexIntUpper => {
                if !arg_type.is_integer() {
                    return exec_err!(
                        "Invalid argument type for integer conversion: {:?}",
                        arg_type
                    );
                }
            }
            ConversionType::SciFloatLower
            | ConversionType::SciFloatUpper
            | ConversionType::DecFloatLower
            | ConversionType::CompactFloatLower
            | ConversionType::CompactFloatUpper
            | ConversionType::HexFloatLower
            | ConversionType::HexFloatUpper => {
                if !arg_type.is_numeric() {
                    return exec_err!(
                        "Invalid argument type for float conversion: {:?}",
                        arg_type
                    );
                }
            }
            ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                if !arg_type.is_temporal() {
                    return exec_err!(
                        "Invalid argument type for time conversion: {:?}",
                        arg_type
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn supports_integer(&self) -> bool {
        matches!(
            self,
            ConversionType::DecInt
                | ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt
                | ConversionType::CharLower
                | ConversionType::CharUpper
                | ConversionType::StringLower
                | ConversionType::StringUpper
        )
    }

    fn supports_float(&self) -> bool {
        matches!(
            self,
            ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper
                | ConversionType::StringLower
                | ConversionType::StringUpper
                | ConversionType::HexFloatLower
                | ConversionType::HexFloatUpper
        )
    }

    fn supports_decimal(&self) -> bool {
        matches!(
            self,
            ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper
                | ConversionType::StringLower
                | ConversionType::StringUpper
        )
    }

    fn supports_time(&self) -> bool {
        matches!(
            self,
            ConversionType::TimeLower(_)
                | ConversionType::TimeUpper(_)
                | ConversionType::StringLower
                | ConversionType::StringUpper
        )
    }

    fn is_upper(&self) -> bool {
        matches!(
            self,
            ConversionType::BooleanUpper
                | ConversionType::HexHashUpper
                | ConversionType::HexIntUpper
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatUpper
                | ConversionType::HexFloatUpper
                | ConversionType::TimeUpper(_)
                | ConversionType::CharUpper
                | ConversionType::StringUpper
        )
    }
}

fn take_conversion_specifier<'a>(
    mut s: &'a str,
    argument_index: usize,
    arg_type: &DataType,
) -> Result<(ConversionSpecifier, &'a str)> {
    let mut spec = ConversionSpecifier {
        argument_index,
        alt_form: false,
        zero_pad: false,
        left_adj: false,
        space_sign: false,
        force_sign: false,
        grouping_separator: false,
        negative_in_parentheses: false,
        width: NumericParam::Literal(0),
        precision: NumericParam::FromArgument, // Placeholder - must not be returned!
        // ignore length modifier
        conversion_type: ConversionType::DecInt,
    };

    // parse flags
    loop {
        match s.chars().next() {
            Some('#') => {
                spec.alt_form = true;
            }
            Some('0') => {
                if spec.left_adj {
                    return exec_err!("Invalid flag combination: '0' and '-'");
                }
                spec.zero_pad = true;
            }
            Some('-') => {
                spec.left_adj = true;
            }
            Some(' ') => {
                if spec.force_sign {
                    return exec_err!("Invalid flag combination: '+' and ' '");
                }
                spec.space_sign = true;
            }
            Some('+') => {
                if spec.space_sign {
                    return exec_err!("Invalid flag combination: '+' and ' '");
                }
                spec.force_sign = true;
            }
            Some(',') => {
                spec.grouping_separator = true;
            }
            Some('(') => {
                spec.negative_in_parentheses = true;
            }
            _ => {
                break;
            }
        }
        s = &s[1..];
    }
    // parse width
    let (w, mut s) = take_numeric_param(s, false);
    spec.width = w;
    // parse precision
    if matches!(s.chars().next(), Some('.')) {
        s = &s[1..];
        let (p, s2) = take_numeric_param(s, true);
        spec.precision = p;
        s = s2;
    }
    let mut chars = s.chars();
    let mut offset = 1;
    // parse conversion type
    spec.conversion_type = match chars.next() {
        Some('b') => ConversionType::BooleanLower,
        Some('B') => ConversionType::BooleanUpper,
        Some('h') => ConversionType::HexHashLower,
        Some('H') => ConversionType::HexHashUpper,
        Some('s') => ConversionType::StringLower,
        Some('S') => ConversionType::StringUpper,
        Some('c') => ConversionType::CharLower,
        Some('C') => ConversionType::CharUpper,
        Some('d') => ConversionType::DecInt,
        Some('o') => ConversionType::OctInt,
        Some('x') => ConversionType::HexIntLower,
        Some('X') => ConversionType::HexIntUpper,
        Some('e') => ConversionType::SciFloatLower,
        Some('E') => ConversionType::SciFloatUpper,
        Some('f') => ConversionType::DecFloatLower,
        Some('g') => ConversionType::CompactFloatLower,
        Some('G') => ConversionType::CompactFloatUpper,
        Some('a') => ConversionType::HexFloatLower,
        Some('A') => ConversionType::HexFloatUpper,
        Some('t') => {
            let Some(chr) = chars.next() else {
                return exec_err!("Invalid time format: {}", s);
            };
            offset += 1;
            ConversionType::TimeLower(chr.try_into()?)
        }
        Some('T') => {
            let Some(chr) = chars.next() else {
                return exec_err!("Invalid time format: {}", s);
            };
            offset += 1;
            ConversionType::TimeUpper(chr.try_into()?)
        }
        chr => {
            return plan_err!("Invalid conversion type: {:?}", chr);
        }
    };

    spec.conversion_type.validate(arg_type)?;
    Ok((spec, &s[offset..]))
}

fn take_numeric_param(s: &str, zero: bool) -> (NumericParam, &str) {
    match s.chars().next() {
        Some(digit) if (if zero { '0'..='9' } else { '1'..='9' }).contains(&digit) => {
            let mut s = s;
            let mut w = 0;
            loop {
                match s.chars().next() {
                    Some(digit) if digit.is_ascii_digit() => {
                        w = 10 * w + (digit as i32 - '0' as i32);
                    }
                    _ => {
                        break;
                    }
                }
                s = &s[1..];
            }
            (NumericParam::Literal(w), s)
        }
        _ => (NumericParam::FromArgument, s),
    }
}

impl ConversionSpecifier {
    pub fn format(&self, string: &mut String, value: &ScalarValue) -> Result<()> {
        match value {
            ScalarValue::Boolean(value) => match self.conversion_type {
                ConversionType::StringLower | ConversionType::StringUpper => {
                    self.format_string(string, &value.unwrap_or(false).to_string())
                }

                _ => self.format_boolean(string, value),
            },
            ScalarValue::Int8(value) => match (self.conversion_type, value) {
                (ConversionType::DecInt, Some(value)) => {
                    self.format_signed(string, *value as i64)
                }
                (
                    ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, (*value as u8) as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, *value as u8 as char)
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Int8",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int16(value) => match (self.conversion_type, value) {
                (ConversionType::DecInt, Some(value)) => {
                    self.format_signed(string, *value as i64)
                }
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(
                        string,
                        char::from_u32((*value as u16) as u32).unwrap(),
                    )
                }
                (
                    ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, (*value as u16) as u64),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Int16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int32(value) => match (self.conversion_type, value) {
                (ConversionType::DecInt, Some(value)) => {
                    self.format_signed(string, *value as i64)
                }
                (
                    ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, (*value as u32) as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, char::from_u32(*value as u32).unwrap())
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Int32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int64(value) => match (self.conversion_type, value) {
                (ConversionType::DecInt, Some(value)) => {
                    self.format_signed(string, *value)
                }
                (
                    ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, *value as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(
                        string,
                        char::from_u32((*value as u64) as u32).unwrap(),
                    )
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Int64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt8(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecInt
                    | ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, *value as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, *value as char)
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for UInt8",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt16(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecInt
                    | ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, *value as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, char::from_u32(*value as u32).unwrap())
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for UInt16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt32(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecInt
                    | ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, *value as u64),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, char::from_u32(*value).unwrap())
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for UInt32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt64(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecInt
                    | ConversionType::HexIntLower
                    | ConversionType::HexIntUpper
                    | ConversionType::OctInt,
                    Some(value),
                ) => self.format_unsigned(string, *value),
                (ConversionType::CharLower | ConversionType::CharUpper, Some(value)) => {
                    self.format_char(string, char::from_u32(*value as u32).unwrap())
                }
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_integer() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for UInt64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float16(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecFloatLower
                    | ConversionType::SciFloatLower
                    | ConversionType::SciFloatUpper
                    | ConversionType::CompactFloatLower
                    | ConversionType::CompactFloatUpper,
                    Some(value),
                ) => self.format_float(string, value.to_f64().unwrap()),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_f32().unwrap().spark_string()),
                (
                    ConversionType::HexFloatLower | ConversionType::HexFloatUpper,
                    Some(value),
                ) => self.format_hex_float(string, value.to_f64().unwrap()),
                (t, None) if t.supports_float() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Float16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float32(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecFloatLower
                    | ConversionType::SciFloatLower
                    | ConversionType::SciFloatUpper
                    | ConversionType::CompactFloatLower
                    | ConversionType::CompactFloatUpper,
                    Some(value),
                ) => self.format_float(string, *value as f64),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.spark_string()),
                (
                    ConversionType::HexFloatLower | ConversionType::HexFloatUpper,
                    Some(value),
                ) => self.format_hex_float(string, *value as f64),
                (t, None) if t.supports_float() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Float32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float64(value) => match (self.conversion_type, value) {
                (
                    ConversionType::DecFloatLower
                    | ConversionType::SciFloatLower
                    | ConversionType::SciFloatUpper
                    | ConversionType::CompactFloatLower
                    | ConversionType::CompactFloatUpper,
                    Some(value),
                ) => self.format_float(string, *value),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.spark_string()),
                (
                    ConversionType::HexFloatLower | ConversionType::HexFloatUpper,
                    Some(value),
                ) => self.format_hex_float(string, *value),
                (t, None) if t.supports_float() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Float64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Utf8(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                if matches!(
                    self.conversion_type,
                    ConversionType::StringLower | ConversionType::StringUpper
                ) {
                    self.format_string(string, value)
                } else {
                    exec_err!(
                        "Invalid conversion type: {:?} for Utf8",
                        self.conversion_type
                    )
                }
            }
            ScalarValue::LargeUtf8(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                if matches!(
                    self.conversion_type,
                    ConversionType::StringLower | ConversionType::StringUpper
                ) {
                    self.format_string(string, value)
                } else {
                    exec_err!(
                        "Invalid conversion type: {:?} for LargeUtf8",
                        self.conversion_type
                    )
                }
            }
            ScalarValue::Utf8View(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                self.format_string(string, value)
            }
            ScalarValue::Decimal128(value, _, scale) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::DecFloatLower
                        | ConversionType::SciFloatLower
                        | ConversionType::SciFloatUpper
                        | ConversionType::CompactFloatLower
                        | ConversionType::CompactFloatUpper,
                        Some(value),
                    ) => self.format_decimal(string, &value.to_string(), *scale as i64),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_decimal() => {
                        self.format_string(string, "null")
                    }

                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for Decimal128",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::Decimal256(value, _, scale) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::DecFloatLower
                        | ConversionType::SciFloatLower
                        | ConversionType::SciFloatUpper
                        | ConversionType::CompactFloatLower
                        | ConversionType::CompactFloatUpper,
                        Some(value),
                    ) => self.format_decimal(string, &value.to_string(), *scale as i64),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_decimal() => {
                        self.format_string(string, "null")
                    }

                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for Decimal256",
                            self.conversion_type
                        )
                    }
                }
            }

            ScalarValue::Time32Second(value) => match (self.conversion_type, value) {
                (
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                    Some(value),
                ) => self.format_time(string, *value as i64 * 1000000000, &None),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_time() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Time32Second",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Time32Millisecond(value) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, *value as i64 * 1000000, &None),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for Time32Millisecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::Time64Microsecond(value) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, *value * 1000, &None),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for Time64Microsecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::Time64Nanosecond(value) => match (self.conversion_type, value) {
                (
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                    Some(value),
                ) => self.format_time(string, *value, &None),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_time() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Time64Nanosecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::TimestampSecond(value, zone) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, value * 1000000000, zone),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for TimestampSecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::TimestampMillisecond(value, zone) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, *value * 1000000, zone),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),

                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for TimestampMillisecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::TimestampMicrosecond(value, zone) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, value * 1000, zone),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for timestampmicrosecond",
                            self.conversion_type
                        )
                    }
                }
            }

            ScalarValue::TimestampNanosecond(value, zone) => {
                match (self.conversion_type, value) {
                    (
                        ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                        Some(value),
                    ) => self.format_time(string, *value, zone),
                    (
                        ConversionType::StringLower | ConversionType::StringUpper,
                        Some(value),
                    ) => self.format_string(string, &value.to_string()),
                    (t, None) if t.supports_time() => self.format_string(string, "null"),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for TimestampNanosecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::Date32(value) => match (self.conversion_type, value) {
                (
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                    Some(value),
                ) => self.format_date(string, *value as i64),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_time() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Date32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Date64(value) => match (self.conversion_type, value) {
                (
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_),
                    Some(value),
                ) => self.format_date(string, *value),
                (
                    ConversionType::StringLower | ConversionType::StringUpper,
                    Some(value),
                ) => self.format_string(string, &value.to_string()),
                (t, None) if t.supports_time() => self.format_string(string, "null"),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for Date64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Null => {
                let value = "null".to_string();
                self.format_string(string, &value)
            }
            _ => exec_err!("Invalid scalar value: {value}"),
        }
    }

    fn format_hex_float(&self, writer: &mut String, value: f64) -> Result<()> {
        // Handle special cases first
        let (sign, raw_exponent, mantissa) = value.to_parts();
        let is_subnormal = raw_exponent == 0;

        let precision = match self.precision {
            NumericParam::FromArgument => None,
            NumericParam::Literal(p) => Some(p),
        };

        // Determine if we need to normalize subnormal numbers
        // Only normalize when precision is specified and less than full mantissa width
        let mantissa_hex_digits = f64::MANTISSA_BITS.div_ceil(4); // 13 for f64
        let should_normalize = is_subnormal
            && precision.is_some()
            && precision.unwrap() < mantissa_hex_digits as i32;

        let (value, raw_exponent, mantissa) = if should_normalize {
            let value = value * f64::SCALEUP;
            let (_, raw_exponent, mantissa) = value.to_parts();
            (value, raw_exponent, mantissa)
        } else {
            (value, raw_exponent, mantissa)
        };

        let mut temp = String::new();

        let sign_char = if sign {
            "-"
        } else if self.force_sign {
            "+"
        } else if self.space_sign {
            " "
        } else {
            ""
        };
        match value.category() {
            FpCategory::Nan => {
                write!(&mut temp, "NaN")?;
            }
            FpCategory::Infinite => {
                write!(&mut temp, "{sign_char}Infinity")?;
            }
            FpCategory::Zero => {
                write!(&mut temp, "{sign_char}0x0.0p0")?;
            }
            _ => {
                let bias = i32::from(f64::EXPONENT_BIAS);
                // Calculate actual exponent
                // For subnormal numbers, the exponent is 1 - bias (not 0 - bias)
                let exponent = if is_subnormal && !should_normalize {
                    1 - bias
                } else {
                    raw_exponent as i32 - bias
                };

                // Handle precision for rounding
                let final_mantissa = if let Some(p) = precision {
                    if p == 0 {
                        // For precision 0, we still need at least 1 hex digit
                        // Round to the nearest integer mantissa value
                        let shift_distance = f64::MANTISSA_BITS as i32 - 4; // Keep 1 hex digit (4 bits)
                        let shifted = mantissa >> shift_distance;
                        let rounding_bits = mantissa & ((1u64 << shift_distance) - 1);
                        let round_bit = 1u64 << (shift_distance - 1);

                        // Round to nearest, ties to even
                        if rounding_bits > round_bit
                            || (rounding_bits == round_bit && (shifted & 1) != 0)
                        {
                            (shifted + 1) << shift_distance
                        } else {
                            shifted << shift_distance
                        }
                    } else {
                        // Apply rounding based on precision
                        let precision_bits = p * 4; // Each hex digit is 4 bits
                        let keep_bits = f64::MANTISSA_BITS as i32;
                        let shift_distance = keep_bits - precision_bits;

                        if shift_distance > 0 {
                            let shifted = mantissa >> shift_distance;
                            let rounding_bits = mantissa & ((1u64 << shift_distance) - 1);
                            let round_bit = 1u64 << (shift_distance - 1);

                            // Round to nearest, ties to even
                            if rounding_bits > round_bit
                                || (rounding_bits == round_bit && (shifted & 1) != 0)
                            {
                                (shifted + 1) << shift_distance
                            } else {
                                shifted << shift_distance
                            }
                        } else {
                            mantissa
                        }
                    }
                } else {
                    mantissa
                };

                if is_subnormal && !should_normalize {
                    // Original subnormal format: 0x0.xxxp-1022
                    if precision.is_some() {
                        // precision >= 13, show as subnormal
                        let full_hex = format!(
                            "{:0width$x}",
                            final_mantissa,
                            width = mantissa_hex_digits as usize
                        );
                        write!(&mut temp, "{sign_char}0x0.{full_hex}p{exponent}")?;
                    } else {
                        // No precision specified, show full subnormal
                        let hex_digits = format!(
                            "{:0width$x}",
                            final_mantissa,
                            width = mantissa_hex_digits as usize
                        );
                        write!(&mut temp, "{sign_char}0x0.{hex_digits}p{exponent}")?;
                    }
                } else {
                    // Normal format or normalized subnormal: 0x1.xxxpN
                    if let Some(p) = precision {
                        let p = if p == 0 { 1 } else { p };
                        let hex_digits = format!("{final_mantissa:x}");
                        let formatted_digits = if p as usize >= hex_digits.len() {
                            // Pad with zeros to match precision
                            format!("{:0<width$}", hex_digits, width = p as usize)
                        } else {
                            hex_digits[..p as usize].to_string()
                        };
                        write!(
                            &mut temp,
                            "{sign_char}0x1.{formatted_digits}p{exponent}"
                        )?;
                    } else {
                        // Default: show all significant digits
                        let mut hex_digits = format!("{final_mantissa:x}");
                        hex_digits = trim_trailing_0s_hex(&hex_digits).to_owned();
                        if hex_digits.is_empty() {
                            write!(&mut temp, "{sign_char}0x1.0p{exponent}")?;
                        } else {
                            write!(&mut temp, "{sign_char}0x1.{hex_digits}p{exponent}")?;
                        }
                    }
                }
                if should_normalize {
                    let (prefix, exp) = temp.split_once('p').unwrap();
                    let iexp = exp.parse::<i32>().unwrap() - f64::SCALEUP_POWER as i32;
                    temp = format!("{prefix}p{iexp}");
                }
            }
        };

        if self.conversion_type.is_upper() {
            temp = temp.to_ascii_uppercase();
        }

        let NumericParam::Literal(width) = self.width else {
            writer.push_str(&temp);
            return Ok(());
        };
        if self.left_adj {
            writer.push_str(&temp);
            for _ in temp.len()..width as usize {
                writer.push(' ');
            }
        } else if self.zero_pad && value.is_finite() {
            let delimiter = if self.conversion_type.is_upper() {
                "0X"
            } else {
                "0x"
            };
            let (prefix, suffix) = temp.split_once(delimiter).unwrap();
            writer.push_str(prefix);
            writer.push_str(delimiter);
            for _ in temp.len()..width as usize {
                writer.push('0');
            }
            writer.push_str(suffix);
        } else {
            while temp.len() < width as usize {
                temp = " ".to_owned() + &temp;
            }
            writer.push_str(&temp);
        };
        Ok(())
    }

    fn format_char(&self, writer: &mut String, value: char) -> Result<()> {
        let upper = self.conversion_type.is_upper();
        match self.conversion_type {
            ConversionType::CharLower | ConversionType::CharUpper => {
                let NumericParam::Literal(width) = self.width else {
                    if upper {
                        writer.push(value.to_ascii_uppercase());
                    } else {
                        writer.push(value);
                    }
                    return Ok(());
                };

                let start_len = writer.len();
                if self.left_adj {
                    if upper {
                        writer.push(value.to_ascii_uppercase());
                    } else {
                        writer.push(value);
                    }
                    while writer.len() - start_len < width as usize {
                        writer.push(' ');
                    }
                } else {
                    while writer.len() - start_len + value.len_utf8() < width as usize {
                        writer.push(' ');
                    }
                    if upper {
                        writer.push(value.to_ascii_uppercase());
                    } else {
                        writer.push(value);
                    }
                }
                Ok(())
            }
            _ => exec_err!(
                "Invalid conversion type: {:?} for char",
                self.conversion_type
            ),
        }
    }

    fn format_boolean(&self, writer: &mut String, value: &Option<bool>) -> Result<()> {
        let value = value.unwrap_or(false);

        let formatted = match self.conversion_type {
            ConversionType::BooleanUpper => {
                if value {
                    "TRUE"
                } else {
                    "FALSE"
                }
            }
            ConversionType::BooleanLower => {
                if value {
                    "true"
                } else {
                    "false"
                }
            }
            _ => {
                return exec_err!(
                    "Invalid conversion type: {:?} for boolean array",
                    self.conversion_type
                );
            }
        };
        self.format_str(writer, formatted)
    }

    fn format_float(&self, writer: &mut String, value: f64) -> Result<()> {
        let mut prefix = String::new();
        let mut suffix = String::new();
        let mut number = String::new();
        let upper = self.conversion_type.is_upper();

        // set up the sign
        if value.is_sign_negative() {
            if self.negative_in_parentheses {
                prefix.push('(');
                suffix.push(')');
            } else {
                prefix.push('-');
            }
        } else if self.space_sign {
            prefix.push(' ');
        } else if self.force_sign {
            prefix.push('+');
        }

        if value.is_finite() {
            let mut use_scientific = false;
            let mut strip_trailing_0s = false;
            let mut abs = value.abs();
            let mut exponent = abs.log10().floor() as i32;
            let mut precision = match self.precision {
                NumericParam::Literal(p) => p,
                _ => 6,
            };
            match self.conversion_type {
                ConversionType::DecFloatLower => {
                    // default
                }
                ConversionType::SciFloatLower => {
                    use_scientific = true;
                }
                ConversionType::SciFloatUpper => {
                    use_scientific = true;
                }
                ConversionType::CompactFloatLower | ConversionType::CompactFloatUpper => {
                    strip_trailing_0s = true;
                    if precision == 0 {
                        precision = 1;
                    }
                    // exponent signifies significant digits - we must round now
                    // to (re)calculate the exponent
                    let rounding_factor =
                        10.0_f64.powf((precision - 1 - exponent) as f64);
                    let rounded_fixed = (abs * rounding_factor).round();
                    abs = rounded_fixed / rounding_factor;
                    exponent = abs.log10().floor() as i32;
                    if exponent < -4 || exponent >= precision {
                        use_scientific = true;
                        precision -= 1;
                    } else {
                        // precision specifies the number of significant digits
                        precision -= 1 + exponent;
                    }
                }
                _ => {
                    return exec_err!(
                        "Invalid conversion type: {:?} for float",
                        self.conversion_type
                    );
                }
            }

            if use_scientific {
                // Manual scientific notation formatting for uppercase E
                let mantissa = abs / 10.0_f64.powf(exponent as f64);
                let exp_char = if upper { 'E' } else { 'e' };
                number = format!("{mantissa:.prec$}", prec = precision as usize);
                if strip_trailing_0s {
                    number = trim_trailing_0s(&number).to_owned();
                }
                number = format!("{number}{exp_char}{exponent:+03}");
            } else {
                number = format!("{abs:.prec$}", prec = precision as usize);
                if strip_trailing_0s {
                    number = trim_trailing_0s(&number).to_owned();
                }
            }
            if self.alt_form && !number.contains('.') {
                number += ".";
            }
        } else {
            // not finite
            match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::CompactFloatLower => {
                    if value.is_infinite() {
                        number.push_str("Infinity")
                    } else {
                        number.push_str("NaN")
                    }
                }
                ConversionType::SciFloatUpper | ConversionType::CompactFloatUpper => {
                    if value.is_infinite() {
                        number.push_str("INFINITY")
                    } else {
                        number.push_str("NAN")
                    }
                }
                _ => {
                    return exec_err!(
                        "Invalid conversion type: {:?} for float",
                        self.conversion_type
                    );
                }
            }
        }
        // Take care of padding
        let NumericParam::Literal(width) = self.width else {
            writer.push_str(&prefix);
            writer.push_str(&number);
            writer.push_str(&suffix);
            return Ok(());
        };
        if self.left_adj {
            let mut full_num = prefix + &number + &suffix;
            while full_num.len() < width as usize {
                full_num.push(' ');
            }
            writer.push_str(&full_num);
        } else if self.zero_pad && value.is_finite() {
            while prefix.len() + number.len() + suffix.len() < width as usize {
                prefix.push('0');
            }
            writer.push_str(&prefix);
            writer.push_str(&number);
            writer.push_str(&suffix);
        } else {
            let mut full_num = prefix + &number + &suffix;
            while full_num.len() < width as usize {
                full_num = " ".to_owned() + &full_num;
            }
            writer.push_str(&full_num);
        };

        Ok(())
    }

    fn format_signed(&self, writer: &mut String, value: i64) -> Result<()> {
        let negative = value < 0;
        let abs_val = value.abs();

        let (sign_prefix, sign_suffix) = if negative && self.negative_in_parentheses {
            ("(".to_owned(), ")".to_owned())
        } else if negative {
            ("-".to_owned(), "".to_owned())
        } else if self.force_sign {
            ("+".to_owned(), "".to_owned())
        } else if self.space_sign {
            (" ".to_owned(), "".to_owned())
        } else {
            ("".to_owned(), "".to_owned())
        };

        let mut mod_spec = *self;
        mod_spec.width = match self.width {
            NumericParam::Literal(w) => NumericParam::Literal(
                w - sign_prefix.len() as i32 - sign_suffix.len() as i32,
            ),
            _ => NumericParam::FromArgument,
        };
        let mut formatted = String::new();
        mod_spec.format_unsigned(&mut formatted, abs_val as u64)?;
        // put the sign a after any leading spaces
        let mut actual_number = &formatted[0..];
        let mut leading_spaces = &formatted[0..0];
        if let Some(first_non_space) = formatted.find(|c| c != ' ') {
            actual_number = &formatted[first_non_space..];
            leading_spaces = &formatted[0..first_non_space];
        }
        write!(
            writer,
            "{}{}{}{}",
            leading_spaces.to_owned(),
            sign_prefix,
            actual_number,
            sign_suffix
        )
        .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
        Ok(())
    }

    fn format_unsigned(&self, writer: &mut String, value: u64) -> Result<()> {
        let mut s = String::new();
        let mut alt_prefix = "";
        match self.conversion_type {
            ConversionType::DecInt => {
                let num_str = format!("{value}");
                if self.grouping_separator {
                    // Add thousands separators
                    let mut result = String::new();
                    let chars: Vec<char> = num_str.chars().collect();
                    for (i, c) in chars.iter().enumerate() {
                        if i > 0 && (chars.len() - i).is_multiple_of(3) {
                            result.push(',');
                        }
                        result.push(*c);
                    }
                    s = result;
                } else {
                    s = num_str;
                }
            }
            ConversionType::HexIntLower => {
                alt_prefix = "0x";
                write!(&mut s, "{value:x}")
                    .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
            }
            ConversionType::HexIntUpper => {
                alt_prefix = "0X";
                write!(&mut s, "{value:X}")
                    .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
            }
            ConversionType::OctInt => {
                alt_prefix = "0";
                write!(&mut s, "{value:o}")
                    .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
            }
            _ => {
                return exec_err!(
                    "Invalid conversion type: {:?} for u64",
                    self.conversion_type
                );
            }
        }
        let mut prefix = if self.alt_form {
            alt_prefix.to_owned()
        } else {
            String::new()
        };

        let formatted = if let NumericParam::Literal(width) = self.width {
            if self.left_adj {
                let mut num_str = prefix + &s;
                while num_str.len() < width as usize {
                    num_str.push(' ');
                }
                num_str
            } else if self.zero_pad {
                while prefix.len() + s.len() < width as usize {
                    prefix.push('0');
                }
                prefix + &s
            } else {
                let mut num_str = prefix + &s;
                while num_str.len() < width as usize {
                    num_str = " ".to_owned() + &num_str;
                }
                num_str
            }
        } else {
            prefix + &s
        };
        write!(writer, "{formatted}")
            .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
        Ok(())
    }

    fn format_str(&self, writer: &mut String, value: &str) -> Result<()> {
        // Take care of precision, putting the truncated string in `content`
        let precision: usize = match self.precision {
            NumericParam::Literal(p) => p,
            _ => i32::MAX,
        }
        .try_into()
        .unwrap_or_default();
        let content_len = {
            let mut content_len = precision.min(value.len());
            while !value.is_char_boundary(content_len) {
                content_len -= 1;
            }
            content_len
        };
        let content = &value[..content_len];

        // Pad to width if needed, putting the padded string in `s`

        if let NumericParam::Literal(width) = self.width {
            let start_len = writer.len();
            if self.left_adj {
                writer.push_str(content);
                while writer.len() - start_len < width as usize {
                    writer.push(' ');
                }
            } else {
                while writer.len() - start_len + content.len() < width as usize {
                    writer.push(' ');
                }
                writer.push_str(content);
            }
        } else {
            writer.push_str(content);
        }
        Ok(())
    }

    fn format_string(&self, writer: &mut String, value: &str) -> Result<()> {
        if self.conversion_type.is_upper() {
            let upper = value.to_ascii_uppercase();
            self.format_str(writer, &upper)
        } else {
            self.format_str(writer, value)
        }
    }

    fn format_decimal(&self, writer: &mut String, value: &str, scale: i64) -> Result<()> {
        let mut prefix = String::new();
        let upper = self.conversion_type.is_upper();

        // Parse as BigDecimal
        let decimal = value
            .parse::<BigInt>()
            .map_err(|e| exec_datafusion_err!("Failed to parse decimal: {}", e))?;
        let decimal = BigDecimal::from_bigint(decimal, scale);

        // Handle sign
        let is_negative = decimal.sign() == Sign::Minus;
        let abs_decimal = decimal.abs();

        if is_negative {
            prefix.push('-');
        } else if self.space_sign {
            prefix.push(' ');
        } else if self.force_sign {
            prefix.push('+');
        }

        let exp_symb = if upper { 'E' } else { 'e' };
        let mut strip_trailing_0s = false;

        // Get precision setting
        let mut precision = match self.precision {
            NumericParam::Literal(p) => p,
            _ => 6,
        };

        let number = match self.conversion_type {
            ConversionType::DecFloatLower => {
                // Format as fixed-point decimal
                self.format_decimal_fixed(&abs_decimal, precision, strip_trailing_0s)?
            }
            ConversionType::SciFloatLower => self.format_decimal_scientific(
                &abs_decimal,
                precision,
                'e',
                strip_trailing_0s,
            )?,
            ConversionType::SciFloatUpper => self.format_decimal_scientific(
                &abs_decimal,
                precision,
                'E',
                strip_trailing_0s,
            )?,
            ConversionType::CompactFloatLower | ConversionType::CompactFloatUpper => {
                strip_trailing_0s = true;
                if precision == 0 {
                    precision = 1;
                }
                // Determine if we should use scientific notation
                let log10_val = abs_decimal.to_f64().map(|f| f.log10()).unwrap_or(0.0);
                if log10_val < -4.0 || log10_val >= precision as f64 {
                    self.format_decimal_scientific(
                        &abs_decimal,
                        precision - 1,
                        exp_symb,
                        strip_trailing_0s,
                    )?
                } else {
                    self.format_decimal_fixed(
                        &abs_decimal,
                        precision - 1 - log10_val.floor() as i32,
                        strip_trailing_0s,
                    )?
                }
            }
            _ => {
                return exec_err!(
                    "Invalid conversion type: {:?} for decimal",
                    self.conversion_type
                );
            }
        };

        // Handle padding
        let NumericParam::Literal(width) = self.width else {
            writer.push_str(&prefix);
            writer.push_str(&number);
            return Ok(());
        };

        if self.left_adj {
            let mut full_num = prefix + &number;
            while full_num.len() < width as usize {
                full_num.push(' ');
            }
            writer.push_str(&full_num);
        } else if self.zero_pad {
            while prefix.len() + number.len() < width as usize {
                prefix.push('0');
            }
            writer.push_str(&prefix);
            writer.push_str(&number);
        } else {
            let mut full_num = prefix + &number;
            while full_num.len() < width as usize {
                full_num = " ".to_owned() + &full_num;
            }
            writer.push_str(&full_num);
        }

        Ok(())
    }

    fn format_decimal_fixed(
        &self,
        decimal: &BigDecimal,
        precision: i32,
        strip_trailing_0s: bool,
    ) -> Result<String> {
        if precision <= 0 {
            Ok(decimal.round(0).to_string())
        } else {
            // Use BigDecimal's with_scale method for precise decimal formatting
            let scaled = decimal.round(precision as i64);
            let mut number = scaled.to_string();
            if strip_trailing_0s {
                number = trim_trailing_0s(&number).to_owned();
            }
            Ok(number)
        }
    }

    fn format_decimal_scientific(
        &self,
        decimal: &BigDecimal,
        precision: i32,
        exp_char: char,
        strip_trailing_0s: bool,
    ) -> Result<String> {
        // Convert to f64 for scientific notation (may lose precision for very large numbers)
        let float_val = decimal.to_f64().unwrap_or(0.0);
        if float_val == 0.0 {
            return Ok(format!("0{exp_char}+00"));
        }

        let abs_val = float_val.abs();
        let exponent = abs_val.log10().floor() as i32;
        let mantissa = abs_val / 10.0_f64.powf(exponent as f64);

        let mut number = if precision <= 0 {
            format!("{mantissa:.0}")
        } else {
            format!("{mantissa:.prec$}", prec = precision as usize)
        };

        if strip_trailing_0s {
            number = trim_trailing_0s(&number).to_owned();
        }

        Ok(format!("{number}{exp_char}{exponent:+03}"))
    }

    fn format_time(
        &self,
        writer: &mut String,
        timestamp_nanos: i64,
        timezone: &Option<Arc<str>>,
    ) -> Result<()> {
        let upper = self.conversion_type.is_upper();
        match &self.conversion_type {
            ConversionType::TimeLower(time_format)
            | ConversionType::TimeUpper(time_format) => {
                let formatted =
                    self.format_time_component(timestamp_nanos, *time_format, timezone)?;
                let result = if upper {
                    formatted.to_uppercase()
                } else {
                    formatted
                };
                write!(writer, "{result}")
                    .map_err(|e| exec_datafusion_err!("Write error: {}", e))?;
                Ok(())
            }
            _ => exec_err!(
                "Invalid conversion type for time: {:?}",
                self.conversion_type
            ),
        }
    }

    fn format_date(&self, writer: &mut String, date_days: i64) -> Result<()> {
        // Convert days since epoch to timestamp in nanoseconds
        let timestamp_nanos = date_days * 24 * 60 * 60 * 1_000_000_000;
        self.format_time(writer, timestamp_nanos, &None)
    }

    fn format_time_component(
        &self,
        timestamp_nanos: i64,
        time_format: TimeFormat,
        _timezone: &Option<Arc<str>>,
    ) -> Result<String> {
        // Convert nanoseconds to seconds and nanoseconds remainder
        let secs = timestamp_nanos / 1_000_000_000;
        let nanos = (timestamp_nanos % 1_000_000_000) as u32;

        // Create DateTime from timestamp
        let dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
            exec_datafusion_err!("Invalid timestamp: {}", timestamp_nanos)
        })?;

        match time_format {
            TimeFormat::HUpper => Ok(format!("{:02}", dt.hour())),
            TimeFormat::IUpper => {
                let hour_12 = match dt.hour12() {
                    (true, h) => h,  // PM
                    (false, h) => h, // AM
                };
                Ok(format!("{hour_12:02}"))
            }
            TimeFormat::KLower => Ok(format!("{}", dt.hour())),
            TimeFormat::LLower => {
                let hour_12 = match dt.hour12() {
                    (true, h) => h,  // PM
                    (false, h) => h, // AM
                };
                Ok(format!("{hour_12}"))
            }
            TimeFormat::MUpper => Ok(format!("{:02}", dt.minute())),
            TimeFormat::SUpper => Ok(format!("{:02}", dt.second())),
            TimeFormat::LUpper => Ok(format!("{:03}", dt.timestamp_millis() % 1000)),
            TimeFormat::NUpper => Ok(format!("{:09}", dt.nanosecond())),
            TimeFormat::PLower => {
                let (is_pm, _) = dt.hour12();
                Ok(if is_pm {
                    "pm".to_string()
                } else {
                    "am".to_string()
                })
            }
            TimeFormat::ZLower => Ok("+0000".to_string()), // UTC timezone offset
            TimeFormat::ZUpper => Ok("UTC".to_string()),   // UTC timezone name
            TimeFormat::SLower => Ok(format!("{}", dt.timestamp())),
            TimeFormat::QUpper => Ok(format!("{}", dt.timestamp_millis())),
            TimeFormat::BUpper => Ok(dt.format("%B").to_string()), // Full month name
            TimeFormat::BLower => Ok(dt.format("%b").to_string()), // Abbreviated month name
            TimeFormat::AUpper => Ok(dt.format("%A").to_string()), // Full weekday name
            TimeFormat::ALower => Ok(dt.format("%a").to_string()), // Abbreviated weekday name
            TimeFormat::CUpper => Ok(format!("{:02}", dt.year() / 100)),
            TimeFormat::YUpper => Ok(format!("{:04}", dt.year())),
            TimeFormat::YLower => Ok(format!("{:02}", dt.year() % 100)),
            TimeFormat::JLower => Ok(format!("{:03}", dt.ordinal())), // Day of year
            TimeFormat::MLower => Ok(format!("{:02}", dt.month())),
            TimeFormat::DLower => Ok(format!("{:02}", dt.day())),
            TimeFormat::ELower => Ok(format!("{}", dt.day())),
            TimeFormat::RUpper => Ok(dt.format("%H:%M").to_string()),
            TimeFormat::TUpper => Ok(dt.format("%H:%M:%S").to_string()),
            TimeFormat::RLower => {
                let (is_pm, hour_12) = dt.hour12();
                let am_pm = if is_pm { "PM" } else { "AM" };
                Ok(format!(
                    "{:02}:{:02}:{:02} {}",
                    hour_12,
                    dt.minute(),
                    dt.second(),
                    am_pm
                ))
            }
            TimeFormat::DUpper => Ok(dt.format("%m/%d/%y").to_string()),
            TimeFormat::FUpper => Ok(dt.format("%Y-%m-%d").to_string()),
            TimeFormat::CLower => Ok(dt.format("%a %b %d %H:%M:%S UTC %Y").to_string()),
        }
    }
}

trait FloatFormattable: std::fmt::Display {
    fn category(&self) -> FpCategory;

    fn spark_string(&self) -> String {
        match self.category() {
            FpCategory::Nan => "NaN".to_string(),
            FpCategory::Infinite => {
                if self.negative() {
                    "-Infinity".to_string()
                } else {
                    "Infinity".to_string()
                }
            }
            _ => self.to_string(),
        }
    }
    fn negative(&self) -> bool;
}

impl FloatFormattable for f32 {
    fn category(&self) -> FpCategory {
        self.classify()
    }

    fn negative(&self) -> bool {
        self.is_sign_negative()
    }
}

impl FloatFormattable for f64 {
    fn category(&self) -> FpCategory {
        self.classify()
    }

    fn negative(&self) -> bool {
        self.is_sign_negative()
    }
}

trait FloatBits: FloatFormattable {
    const MANTISSA_BITS: u8;
    const EXPONENT_BIAS: u16;
    const SCALEUP_POWER: u8;
    const SCALEUP: Self;

    fn to_parts(&self) -> (bool, u16, u64);
}

impl FloatBits for f64 {
    const MANTISSA_BITS: u8 = 52;
    const EXPONENT_BIAS: u16 = 1023;
    const SCALEUP_POWER: u8 = 54;
    const SCALEUP: f64 = (1_i64 << Self::SCALEUP_POWER) as f64;

    fn to_parts(&self) -> (bool, u16, u64) {
        let bits = self.to_bits();
        let sign: bool = (bits >> 63) == 1;
        let exponent = ((bits >> 52) & 0x7FF) as u16;
        let mantissa = bits & 0x000F_FFFF_FFFF_FFFF;
        (sign, exponent, mantissa)
    }
}

fn trim_trailing_0s(number: &str) -> &str {
    if number.contains('.') {
        for (i, c) in number.chars().rev().enumerate() {
            if c != '0' {
                return &number[..number.len() - i];
            }
        }
    }
    number
}

fn trim_trailing_0s_hex(number: &str) -> &str {
    for (i, c) in number.chars().rev().enumerate() {
        if c != '0' {
            return &number[..number.len() - i];
        }
    }
    number
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType::Utf8;
    use datafusion_common::Result;

    #[test]
    fn test_format_string_nullability() -> Result<()> {
        let func = FormatStringFunc::new();
        let nullable_format: FieldRef = Arc::new(Field::new("fmt", Utf8, true));

        let out_nullable = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[nullable_format],
            scalar_arguments: &[None],
        })?;

        assert!(
            out_nullable.is_nullable(),
            "format_string(fmt, ...) should be nullable when fmt is nullable"
        );
        let non_nullable_format: FieldRef = Arc::new(Field::new("fmt", Utf8, false));

        let out_non_nullable = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[non_nullable_format],
            scalar_arguments: &[None],
        })?;

        assert!(
            !out_non_nullable.is_nullable(),
            "format_string(fmt, ...) should NOT be nullable when fmt is NOT nullable"
        );

        Ok(())
    }
}

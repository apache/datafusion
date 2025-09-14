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
    datatypes::DataType,
};
use bigdecimal::{
    num_bigint::{BigInt, Sign},
    BigDecimal, ToPrimitive,
};
use chrono::{DateTime, Datelike, Timelike, Utc};
use datafusion_common::{
    exec_datafusion_err, exec_err, plan_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return plan_err!("The format_string function expects at least one argument");
        }
        if arg_types[0] == DataType::Null {
            return Ok(DataType::Utf8);
        }
        if !matches!(
            arg_types[0],
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return plan_err!("The format_string function expects the first argument to be Utf8, LargeUtf8 or Utf8View");
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let len = args
            .args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
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
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        result.first().unwrap().clone(),
                    ))))
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
                        _ => {
                            return exec_err!(
                                "Expected string type, got {:?}",
                                fmt.data_type()
                            )
                        }
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
        ColumnarValue::Array(array) => Ok(ScalarValue::try_from_array(&array, index)?),
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
                        take_conversion_specifier(rest, p, arg_types[p - 1].clone())?;
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
                            return exec_err!("Invalid numeric parameter")
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
                    arg_types[current_argument_index - 1].clone(),
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
    pub negative_in_parantheses: bool,
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
    ///
    /// This should never be passed to [Printf::format()][crate::Printf::format()].
    FromArgument,
}

/// Printf data type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionType {
    /// `B`
    BooleanUpper,
    /// `b`
    BooleanLower,
    /// Not implemented yet. Can be implemented after https://github.com/apache/datafusion/pull/17093 is merged
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
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            ConversionType::StringLower | ConversionType::StringUpper
        )
    }

    pub fn validate(&self, arg_type: DataType) -> Result<()> {
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
}

fn take_conversion_specifier(
    s: &str,
    argument_index: usize,
    arg_type: DataType,
) -> Result<(ConversionSpecifier, &str)> {
    let mut spec = ConversionSpecifier {
        argument_index,
        alt_form: false,
        zero_pad: false,
        left_adj: false,
        space_sign: false,
        force_sign: false,
        grouping_separator: false,
        negative_in_parantheses: false,
        width: NumericParam::Literal(0),
        precision: NumericParam::FromArgument, // Placeholder - must not be returned!
        // ignore length modifier
        conversion_type: ConversionType::DecInt,
    };

    let mut s = s;

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
                spec.negative_in_parantheses = true;
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
            ScalarValue::Int8(value) => match self.conversion_type {
                ConversionType::DecInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_signed(string, value as i64)
                }
                ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, (value as u8) as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(string, value as u8 as char)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for int8",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int16(value) => match self.conversion_type {
                ConversionType::DecInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_signed(string, value as i64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32((value as u16) as u32).unwrap(),
                    )
                }
                ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, (value as u16) as u64)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for int16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int32(value) => match self.conversion_type {
                ConversionType::DecInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_signed(string, value as i64)
                }
                ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, (value as u32) as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32(value as u32).unwrap(),
                    )
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for int32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Int64(value) => match self.conversion_type {
                ConversionType::DecInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_signed(string, value)
                }
                ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, value as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32((value as u64) as u32).unwrap(),
                    )
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for int64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt8(value) => match self.conversion_type {
                ConversionType::DecInt
                | ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, value as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(string, value as char)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for uint8",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt16(value) => match self.conversion_type {
                ConversionType::DecInt
                | ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, value as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32(value as u32).unwrap(),
                    )
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for uint16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt32(value) => match self.conversion_type {
                ConversionType::DecInt
                | ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, value as u64)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32(value).unwrap(),
                    )
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for uint32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::UInt64(value) => match self.conversion_type {
                ConversionType::DecInt
                | ConversionType::HexIntLower
                | ConversionType::HexIntUpper
                | ConversionType::OctInt => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_unsigned(string, value)
                }
                ConversionType::CharLower | ConversionType::CharUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_char(
                        string,
                        char::from_u32(value as u32).unwrap(),
                    )
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for uint64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float16(value) => match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected float value, got {:?}",
                        value
                    ))?;
                    self.format_float(string, value.to_f64())
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value
                            .map(|v| v.to_f32().spark_string())
                            .unwrap_or_else(||"null".to_string()),
                    ),

                ConversionType::HexFloatLower | ConversionType::HexFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_hex_float(string, value.to_f32())
                }
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for float16",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float32(value) => match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected float value, got {:?}",
                        value
                    ))?;
                    self.format_float(string, value as f64)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value
                            .map(|v| v.spark_string())
                            .unwrap_or_else(||"null".to_string()),
                    ),
                ConversionType::HexFloatLower | ConversionType::HexFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_hex_float(string, value)
                }
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for float32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Float64(value) => match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected float value, got {:?}",
                        value
                    ))?;
                    self.format_float(string, value)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value
                            .map(|v| v.spark_string())
                            .unwrap_or_else(||"null".to_string()),
                    ),
                ConversionType::HexFloatLower | ConversionType::HexFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected integer value, got {:?}",
                        value
                    ))?;
                    self.format_hex_float(string, value)
                }
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for float64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Utf8(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                self.format_string(string, value)
            }
            ScalarValue::LargeUtf8(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                self.format_string(string, value)
            }
            ScalarValue::Utf8View(value) => {
                let value: &str = match value {
                    Some(value) => value.as_str(),
                    None => "null",
                };
                self.format_string(string, value)
            }
            ScalarValue::Decimal128(value, _, scale) => match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected decimal value, got {:?}",
                        value
                    ))?;
                    self.format_decimal(string, value.to_string(), *scale as i64)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for decimal128",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Decimal256(value, _, scale) => match self.conversion_type {
                ConversionType::DecFloatLower
                | ConversionType::SciFloatLower
                | ConversionType::SciFloatUpper
                | ConversionType::CompactFloatLower
                | ConversionType::CompactFloatUpper => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected decimal value, got {:?}",
                        value
                    ))?;
                    self.format_decimal(string, value.to_string(), *scale as i64)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for decimal256",
                        self.conversion_type
                    )
                }
            },

            ScalarValue::Time32Second(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected time value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value as i64 * 1000000000, &None)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for time32second",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Time32Millisecond(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected time value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value as i64 * 1000000, &None)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for time32millisecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Time64Microsecond(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected time value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value * 1000, &None)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for time64microsecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Time64Nanosecond(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected time value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value, &None)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for time64nanosecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::TimestampSecond(value, zone) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected timestamp value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value * 1000000000, zone)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for timestampsecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::TimestampMillisecond(value, zone) => {
                match self.conversion_type {
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                        let value = value.ok_or(exec_datafusion_err!(
                            "Expected timestamp value, got {:?}",
                            value
                        ))?;
                        self.format_time(string, value * 1000000, zone)
                    }
                    ConversionType::StringLower | ConversionType::StringUpper => self
                        .format_string(
                            string,
                            &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                        ),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for timestampmillisecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::TimestampMicrosecond(value, zone) => {
                match self.conversion_type {
                    ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                        let value = value.ok_or(exec_datafusion_err!(
                            "Expected timestamp value, got {:?}",
                            value
                        ))?;
                        self.format_time(string, value * 1000, zone)
                    }
                    ConversionType::StringLower | ConversionType::StringUpper => self
                        .format_string(
                            string,
                            &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                        ),
                    _ => {
                        exec_err!(
                            "Invalid conversion type: {:?} for timestampmicrosecond",
                            self.conversion_type
                        )
                    }
                }
            }
            ScalarValue::TimestampNanosecond(value, zone) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected timestamp value, got {:?}",
                        value
                    ))?;
                    self.format_time(string, value, zone)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for timestampnanosecond",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Date32(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected date value, got {:?}",
                        value
                    ))?;
                    self.formate_date(string, value as i64)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for date32",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Date64(value) => match self.conversion_type {
                ConversionType::TimeLower(_) | ConversionType::TimeUpper(_) => {
                    let value = value.ok_or(exec_datafusion_err!(
                        "Expected date value, got {:?}",
                        value
                    ))?;
                    self.formate_date(string, value)
                }
                ConversionType::StringLower | ConversionType::StringUpper => self
                    .format_string(
                        string,
                        &value.map(|v| v.to_string()).unwrap_or_else(||"null".to_string()),
                    ),
                _ => {
                    exec_err!(
                        "Invalid conversion type: {:?} for date64",
                        self.conversion_type
                    )
                }
            },
            ScalarValue::Null => {
                let value = "null".to_string();
                self.format_string(string, &value)
            }
            _ => exec_err!("Invalid scalar value: {:?}", value),
        }
    }

    fn format_hex_float<F: FloatBits>(
        &self,
        writer: &mut String,
        value: F,
    ) -> Result<()> {
        let upper = self.conversion_type == ConversionType::HexFloatUpper;
        match self.precision {
            NumericParam::FromArgument => {
                value.format_hex(writer, None, upper).map_err(|e| {
                    exec_datafusion_err!("Failed to format hex float: {}", e)
                })
            }
            NumericParam::Literal(p) if p >= 13 => {
                value.format_hex(writer, None, upper).map_err(|e| {
                    exec_datafusion_err!("Failed to format hex float: {}", e)
                })
            }
            NumericParam::Literal(p) => {
                value.format_hex(writer, Some(p), upper).map_err(|e| {
                    exec_datafusion_err!("Failed to format hex float: {}", e)
                })
            }
        }
    }

    fn format_char(&self, writer: &mut String, value: char) -> Result<()> {
        match self.conversion_type {
            ConversionType::CharLower | ConversionType::CharUpper => {
                let NumericParam::Literal(width) = self.width else {
                    if self.conversion_type == ConversionType::CharUpper {
                        writer.push(value.to_ascii_uppercase());
                    } else {
                        writer.push(value);
                    }
                    return Ok(());
                };

                let start_len = writer.len();
                if self.left_adj {
                    if self.conversion_type == ConversionType::CharUpper {
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
                    if self.conversion_type == ConversionType::CharUpper {
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
            )
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
                )
            }
        };
        self.format_str(writer, formatted)
    }

    fn format_float(&self, writer: &mut String, value: f64) -> Result<()> {
        let mut prefix = String::new();
        let mut suffix = String::new();
        let mut number = String::new();
        let mut upper = false;

        // set up the sign
        if value.is_sign_negative() {
            if self.negative_in_parantheses {
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
            if precision <= 0 {
                precision = 0;
            }
            match self.conversion_type {
                ConversionType::DecFloatLower => {
                    // default
                }
                ConversionType::SciFloatLower => {
                    use_scientific = true;
                }
                ConversionType::SciFloatUpper => {
                    use_scientific = true;
                    upper = true;
                }
                ConversionType::CompactFloatLower | ConversionType::CompactFloatUpper => {
                    upper = self.conversion_type == ConversionType::CompactFloatUpper;
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
                    )
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
                    )
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

        let (sign_prefix, sign_suffix) = if negative && self.negative_in_parantheses {
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
                        if i > 0 && (chars.len() - i) % 3 == 0 {
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
                )
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
        match self.conversion_type {
            ConversionType::StringLower => self.format_str(writer, value),
            ConversionType::StringUpper => {
                let upper = value.to_ascii_uppercase();
                self.format_str(writer, &upper)
            }
            _ => exec_err!(
                "Invalid conversion type: {:?} for string",
                self.conversion_type
            )
        }
    }

    fn format_decimal(
        &self,
        writer: &mut String,
        value: String,
        scale: i64,
    ) -> Result<()> {
        let mut prefix = String::new();

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

        let mut use_scientific = false;
        let mut exp_symb = 'e';
        let mut strip_trailing_0s = false;

        // Get precision setting
        let mut precision = match self.precision {
            NumericParam::Literal(p) => p,
            _ => 6,
        };
        if precision <= 0 {
            precision = 6; // Default precision
        }

        let number = match self.conversion_type {
            ConversionType::DecFloatLower => {
                // Format as fixed-point decimal
                self.format_decimal_fixed(&abs_decimal, precision, strip_trailing_0s)?
            }
            ConversionType::SciFloatLower => {
                use_scientific = true;
                self.format_decimal_scientific(
                    &abs_decimal,
                    precision,
                    'e',
                    strip_trailing_0s,
                )?
            }
            ConversionType::SciFloatUpper => {
                use_scientific = true;
                self.format_decimal_scientific(
                    &abs_decimal,
                    precision,
                    'E',
                    strip_trailing_0s,
                )?
            }
            ConversionType::CompactFloatLower | ConversionType::CompactFloatUpper => {
                if self.conversion_type == ConversionType::CompactFloatUpper {
                    exp_symb = 'E';
                }
                strip_trailing_0s = true;
                if precision == 0 {
                    precision = 1;
                }
                // Determine if we should use scientific notation
                let log10_val = abs_decimal.to_f64().map(|f| f.log10()).unwrap_or(0.0);
                if log10_val < -4.0 || log10_val >= precision as f64 {
                    use_scientific = true;
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
                )
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
        } else if self.zero_pad && !use_scientific {
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
        match &self.conversion_type {
            ConversionType::TimeLower(time_format)
            | ConversionType::TimeUpper(time_format) => {
                let formatted =
                    self.format_time_component(timestamp_nanos, *time_format, timezone)?;
                let result =
                    if matches!(self.conversion_type, ConversionType::TimeUpper(_)) {
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

    fn formate_date(&self, writer: &mut String, date_days: i64) -> Result<()> {
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

pub trait FloatBits: std::fmt::Display {
    const EXPONENT_BITS: u8;
    const MANTISSA_BITS: u8;
    const EXPONENT_BIAS: u16;
    const SIGNIFICAND_WIDTH: u8;

    fn to_parts(&self) -> (bool, u16, u64);
    fn category(&self) -> FpCategory;

    fn is_finite(&self) -> bool;
    fn is_zero(&self) -> bool;

    fn spark_string(&self) -> String {
        match self.category() {
            FpCategory::Nan => {
                "NaN".to_string()
            }
            FpCategory::Infinite => {
                if self.to_parts().0 {
                    "-Infinity".to_string()
                } else {
                    "Infinity".to_string()
                }
            }
            _ => {
                self.to_string()
            }
        }
    }

    fn format_hex(
        &self,
        f: &mut String,
        prec: Option<i32>,
        upper: bool,
    ) -> core::fmt::Result {
        // Handle special cases first
        let p_char = if upper { "P" } else { "p" };
        let x_char = if upper { "X" } else { "x" };
        match self.category() {
            FpCategory::Nan => {
                let ret = if upper { "NAN" } else { "NaN" };
                return write!(f, "{ret}");
            }
            FpCategory::Infinite => {
                let (sign, _, _) = self.to_parts();
                let sign_char = if sign { "-" } else { "" };
                let ret = if upper { "INFINITY" } else { "Infinity" };
                return write!(f, "{sign_char}{ret}");
            }
            FpCategory::Zero => {
                return write!(f, "0{x_char}0.0{p_char}0");
            }
            _ => {}
        };

        let (sign, raw_exponent, mantissa) = self.to_parts();
        let bias = i32::from(Self::EXPONENT_BIAS);
        let is_subnormal = raw_exponent == 0;

        // Calculate actual exponent
        let exponent = if is_subnormal {
            1 - bias // For subnormal numbers, exponent is 1-bias
        } else {
            raw_exponent as i32 - bias
        };

        let sign_char = if sign { "-" } else { "" };

        // Handle precision for rounding
        let final_mantissa = if let Some(precision) = prec {
            if precision == 0 {
                0
            } else if precision >= 13 {
                // Full precision - no rounding needed
                mantissa
            } else {
                // Apply rounding based on precision
                let precision_bits = precision * 4; // Each hex digit is 4 bits
                let keep_bits = Self::MANTISSA_BITS as i32;
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

        if is_subnormal {
            // Subnormal number: 0.xxxxx
            if let Some(precision) = prec {
                if precision == 0 {
                    write!(f, "{sign_char}0x0.{p_char}{exponent}")
                } else {
                    let hex_digits = if upper {
                        format!("{final_mantissa:X}")
                    } else {
                        format!("{final_mantissa:x}")
                    };
                    let truncated = if precision as usize >= hex_digits.len() {
                        hex_digits
                    } else {
                        hex_digits[..precision as usize].to_string()
                    };
                    write!(f, "{sign_char}0{x_char}0.{truncated}{p_char}{exponent}")
                }
            } else {
                // Default: show all significant digits for subnormal
                let mut hex_digits = format!("{final_mantissa:x}")
                    .trim_end_matches('0')
                    .to_string();
                if hex_digits.is_empty() {
                    write!(f, "{sign_char}0{x_char}0.0{p_char}{exponent}")
                } else {
                    hex_digits = trim_trailing_0s_hex(&hex_digits).to_owned();
                    write!(f, "{sign_char}0{x_char}0.{hex_digits}{p_char}{exponent}")
                }
            }
        } else {
            // Normal number: 1.xxxxx
            if let Some(precision) = prec {
                if precision == 0 {
                    write!(f, "{sign_char}0{x_char}1.{p_char}{exponent}")
                } else {
                    let hex_digits = if upper {
                        format!("{final_mantissa:X}")
                    } else {
                        format!("{final_mantissa:x}")
                    };
                    let truncated = if precision as usize >= hex_digits.len() {
                        hex_digits
                    } else {
                        hex_digits[..precision as usize].to_string()
                    };
                    write!(f, "{sign_char}0{x_char}1.{truncated}{p_char}{exponent}")
                }
            } else {
                // Default: show all significant digits
                let mut hex_digits = if upper {
                    format!("{final_mantissa:X}")
                } else {
                    format!("{final_mantissa:x}")
                };
                hex_digits = trim_trailing_0s_hex(&hex_digits).to_owned();
                if hex_digits.is_empty() {
                    write!(f, "{sign_char}0{x_char}1.0{p_char}{exponent}")
                } else {
                    write!(f, "{sign_char}0{x_char}1.{hex_digits}{p_char}{exponent}")
                }
            }
        }
    }
}

impl FloatBits for f32 {
    const EXPONENT_BITS: u8 = 8;
    const MANTISSA_BITS: u8 = 23;
    const EXPONENT_BIAS: u16 = 127;
    const SIGNIFICAND_WIDTH: u8 = 24;

    fn to_parts(&self) -> (bool, u16, u64) {
        let bits = self.to_bits();
        let sign: bool = (bits >> 31) == 1;
        let exponent = u8::try_from((bits >> 23) & 0xFF).unwrap();
        let mantissa = bits & 0x7F_FFFF;
        (sign, exponent as u16, mantissa as u64)
    }

    fn category(&self) -> FpCategory {
        self.classify()
    }

    fn is_finite(&self) -> bool {
        f32::is_finite(*self)
    }

    fn is_zero(&self) -> bool {
        *self == 0.0
    }
}

impl FloatBits for f64 {
    const EXPONENT_BITS: u8 = 11;
    const MANTISSA_BITS: u8 = 52;
    const EXPONENT_BIAS: u16 = 1023;
    const SIGNIFICAND_WIDTH: u8 = 53;

    fn to_parts(&self) -> (bool, u16, u64) {
        let bits = self.to_bits();
        let sign: bool = (bits >> 63) == 1;
        let exponent = ((bits >> 52) & 0x7FF) as u16;
        let mantissa = bits & 0x000F_FFFF_FFFF_FFFF;
        (sign, exponent, mantissa)
    }

    fn category(&self) -> FpCategory {
        self.classify()
    }

    fn is_finite(&self) -> bool {
        f64::is_finite(*self)
    }

    fn is_zero(&self) -> bool {
        *self == 0.0
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

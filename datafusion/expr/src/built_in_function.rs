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

//! Built-in functions module contains all the built-in functions definitions.

use crate::Volatility;
use datafusion_common::{DataFusionError, Result};
use lazy_static::lazy_static;
use std::fmt;
use std::str::FromStr;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BuiltinScalarFunction {
    // math functions
    /// abs
    Abs,
    /// acos
    Acos,
    /// asin
    Asin,
    /// atan
    Atan,
    /// atan2
    Atan2,
    /// acosh
    Acosh,
    /// asinh
    Asinh,
    /// atanh
    Atanh,
    /// cbrt
    Cbrt,
    /// ceil
    Ceil,
    /// coalesce
    Coalesce,
    /// cos
    Cos,
    /// cos
    Cosh,
    /// degrees
    Degrees,
    /// Digest
    Digest,
    /// exp
    Exp,
    /// factorial
    Factorial,
    /// floor
    Floor,
    /// gcd, Greatest common divisor
    Gcd,
    /// lcm, Least common multiple
    Lcm,
    /// ln, Natural logarithm
    Ln,
    /// log, same as log10
    Log,
    /// log10
    Log10,
    /// log2
    Log2,
    /// pi
    Pi,
    /// power
    Power,
    /// radians
    Radians,
    /// round
    Round,
    /// signum
    Signum,
    /// sin
    Sin,
    /// sinh
    Sinh,
    /// sqrt
    Sqrt,
    /// tan
    Tan,
    /// tanh
    Tanh,
    /// trunc
    Trunc,

    // array functions
    /// construct an array from columns
    MakeArray,
    /// array_append
    ArrayAppend,
    /// array_prepend
    ArrayPrepend,
    /// array_concat
    ArrayConcat,
    /// array_fill
    ArrayFill,
    /// array_position
    ArrayPosition,
    /// array_positions
    ArrayPositions,
    /// array_remove
    ArrayRemove,
    /// array_replace
    ArrayReplace,
    /// array_to_string
    ArrayToString,
    /// cardinality
    Cardinality,
    /// trim_array
    TrimArray,
    /// array_length
    ArrayLength,
    /// array_dims
    ArrayDims,
    /// array_ndims
    ArrayNdims,

    // string functions
    /// ascii
    Ascii,
    /// bit_length
    BitLength,
    /// btrim
    Btrim,
    /// character_length
    CharacterLength,
    /// chr
    Chr,
    /// concat
    Concat,
    /// concat_ws
    ConcatWithSeparator,
    /// date_part
    DatePart,
    /// date_trunc
    DateTrunc,
    /// date_bin
    DateBin,
    /// initcap
    InitCap,
    /// left
    Left,
    /// lpad
    Lpad,
    /// lower
    Lower,
    /// ltrim
    Ltrim,
    /// md5
    MD5,
    /// nullif
    NullIf,
    /// octet_length
    OctetLength,
    /// random
    Random,
    /// regexp_replace
    RegexpReplace,
    /// repeat
    Repeat,
    /// replace
    Replace,
    /// reverse
    Reverse,
    /// right
    Right,
    /// rpad
    Rpad,
    /// rtrim
    Rtrim,
    /// sha224
    SHA224,
    /// sha256
    SHA256,
    /// sha384
    SHA384,
    /// Sha512
    SHA512,
    /// split_part
    SplitPart,
    /// starts_with
    StartsWith,
    /// strpos
    Strpos,
    /// substr
    Substr,
    /// to_hex
    ToHex,
    /// to_timestamp
    ToTimestamp,
    /// to_timestamp_millis
    ToTimestampMillis,
    /// to_timestamp_micros
    ToTimestampMicros,
    /// to_timestamp_seconds
    ToTimestampSeconds,
    /// from_unixtime
    FromUnixtime,
    ///now
    Now,
    ///current_date
    CurrentDate,
    /// current_time
    CurrentTime,
    /// translate
    Translate,
    /// trim
    Trim,
    /// upper
    Upper,
    /// uuid
    Uuid,
    /// regexp_match
    RegexpMatch,
    /// struct
    Struct,
    /// arrow_typeof
    ArrowTypeof,
}

lazy_static! {
    /// Mapping between SQL function names to `BuiltinScalarFunction` types.
    /// Note that multiple SQL function names can represent the same `BuiltinScalarFunction`. These are treated as aliases.
    /// In case of such aliases, the first SQL function name in the vector is used when displaying the function.
    static ref NAME_TO_FUNCTION: Vec<(&'static str, BuiltinScalarFunction)> = vec![
        // math functions
        ("abs", BuiltinScalarFunction::Abs),
        ("acos", BuiltinScalarFunction::Acos),
        ("acosh", BuiltinScalarFunction::Acosh),
        ("asin", BuiltinScalarFunction::Asin),
        ("asinh", BuiltinScalarFunction::Asinh),
        ("atan", BuiltinScalarFunction::Atan),
        ("atanh", BuiltinScalarFunction::Atanh),
        ("atan2", BuiltinScalarFunction::Atan2),
        ("cbrt", BuiltinScalarFunction::Cbrt),
        ("ceil", BuiltinScalarFunction::Ceil),
        ("cos", BuiltinScalarFunction::Cos),
        ("cosh", BuiltinScalarFunction::Cosh),
        ("degrees", BuiltinScalarFunction::Degrees),
        ("exp", BuiltinScalarFunction::Exp),
        ("factorial", BuiltinScalarFunction::Factorial),
        ("floor", BuiltinScalarFunction::Floor),
        ("gcd", BuiltinScalarFunction::Gcd),
        ("lcm", BuiltinScalarFunction::Lcm),
        ("ln", BuiltinScalarFunction::Ln),
        ("log", BuiltinScalarFunction::Log),
        ("log10", BuiltinScalarFunction::Log10),
        ("log2", BuiltinScalarFunction::Log2),
        ("pi", BuiltinScalarFunction::Pi),
        ("power", BuiltinScalarFunction::Power),
        ("pow", BuiltinScalarFunction::Power),
        ("radians", BuiltinScalarFunction::Radians),
        ("random", BuiltinScalarFunction::Random),
        ("round", BuiltinScalarFunction::Round),
        ("signum", BuiltinScalarFunction::Signum),
        ("sin", BuiltinScalarFunction::Sin),
        ("sinh", BuiltinScalarFunction::Sinh),
        ("sqrt", BuiltinScalarFunction::Sqrt),
        ("tan", BuiltinScalarFunction::Tan),
        ("tanh", BuiltinScalarFunction::Tanh),
        ("trunc", BuiltinScalarFunction::Trunc),

        // conditional functions
        ("coalesce", BuiltinScalarFunction::Coalesce),
        ("nullif", BuiltinScalarFunction::NullIf),

        // string functions
        ("ascii", BuiltinScalarFunction::Ascii),
        ("bit_length", BuiltinScalarFunction::BitLength),
        ("btrim", BuiltinScalarFunction::Btrim),
        ("character_length", BuiltinScalarFunction::CharacterLength),
        ("char_length", BuiltinScalarFunction::CharacterLength),
        ("concat", BuiltinScalarFunction::Concat),
        ("concat_ws", BuiltinScalarFunction::ConcatWithSeparator),
        ("chr", BuiltinScalarFunction::Chr),
        ("initcap", BuiltinScalarFunction::InitCap),
        ("left", BuiltinScalarFunction::Left),
        ("length", BuiltinScalarFunction::CharacterLength),
        ("lower", BuiltinScalarFunction::Lower),
        ("lpad", BuiltinScalarFunction::Lpad),
        ("ltrim", BuiltinScalarFunction::Ltrim),
        ("octet_length", BuiltinScalarFunction::OctetLength),
        ("repeat", BuiltinScalarFunction::Repeat),
        ("replace", BuiltinScalarFunction::Replace),
        ("reverse", BuiltinScalarFunction::Reverse),
        ("right", BuiltinScalarFunction::Right),
        ("rpad", BuiltinScalarFunction::Rpad),
        ("rtrim", BuiltinScalarFunction::Rtrim),
        ("split_part", BuiltinScalarFunction::SplitPart),
        ("starts_with", BuiltinScalarFunction::StartsWith),
        ("strpos", BuiltinScalarFunction::Strpos),
        ("substr", BuiltinScalarFunction::Substr),
        ("to_hex", BuiltinScalarFunction::ToHex),
        ("translate", BuiltinScalarFunction::Translate),
        ("trim", BuiltinScalarFunction::Trim),
        ("upper", BuiltinScalarFunction::Upper),
        ("uuid", BuiltinScalarFunction::Uuid),

        // regex functions
        ("regexp_match", BuiltinScalarFunction::RegexpMatch),
        ("regexp_replace", BuiltinScalarFunction::RegexpReplace),

        // time/date functions
        ("now", BuiltinScalarFunction::Now),
        ("current_date", BuiltinScalarFunction::CurrentDate),
        ("current_time", BuiltinScalarFunction::CurrentTime),
        ("date_bin", BuiltinScalarFunction::DateBin),
        ("date_trunc", BuiltinScalarFunction::DateTrunc),
        ("datetrunc", BuiltinScalarFunction::DateTrunc),
        ("date_part", BuiltinScalarFunction::DatePart),
        ("datepart", BuiltinScalarFunction::DatePart),
        ("to_timestamp", BuiltinScalarFunction::ToTimestamp),
        ("to_timestamp_millis", BuiltinScalarFunction::ToTimestampMillis),
        ("to_timestamp_micros", BuiltinScalarFunction::ToTimestampMicros),
        ("to_timestamp_seconds", BuiltinScalarFunction::ToTimestampSeconds),
        ("from_unixtime", BuiltinScalarFunction::FromUnixtime),

        // hashing functions
        ("digest", BuiltinScalarFunction::Digest),
        ("md5", BuiltinScalarFunction::MD5),
        ("sha224", BuiltinScalarFunction::SHA224),
        ("sha256", BuiltinScalarFunction::SHA256),
        ("sha384", BuiltinScalarFunction::SHA384),
        ("sha512", BuiltinScalarFunction::SHA512),

        // other functions
        ("struct", BuiltinScalarFunction::Struct),
        ("arrow_typeof", BuiltinScalarFunction::ArrowTypeof),

        // array functions
        ("make_array", BuiltinScalarFunction::MakeArray),
        ("array_append", BuiltinScalarFunction::ArrayAppend),
        ("array_prepend", BuiltinScalarFunction::ArrayPrepend),
        ("array_concat", BuiltinScalarFunction::ArrayConcat),
        ("array_fill",  BuiltinScalarFunction::ArrayFill),
        ("array_position", BuiltinScalarFunction::ArrayPosition),
        ("array_positions", BuiltinScalarFunction::ArrayPositions),
        ("array_remove", BuiltinScalarFunction::ArrayRemove),
        ("array_replace", BuiltinScalarFunction::ArrayReplace),
        ("array_to_string", BuiltinScalarFunction::ArrayToString),
        ("cardinality", BuiltinScalarFunction::Cardinality),
        ("trim_array", BuiltinScalarFunction::TrimArray),
        ("array_length", BuiltinScalarFunction::ArrayLength),
        ("array_dims", BuiltinScalarFunction::ArrayDims),
        ("array_ndims", BuiltinScalarFunction::ArrayNdims),
    ];
}

impl BuiltinScalarFunction {
    /// an allowlist of functions to take zero arguments, so that they will get special treatment
    /// while executing.
    pub fn supports_zero_argument(&self) -> bool {
        matches!(
            self,
            BuiltinScalarFunction::Pi
                | BuiltinScalarFunction::Random
                | BuiltinScalarFunction::Now
                | BuiltinScalarFunction::CurrentDate
                | BuiltinScalarFunction::CurrentTime
                | BuiltinScalarFunction::Uuid
        )
    }
    /// Returns the [Volatility] of the builtin function.
    pub fn volatility(&self) -> Volatility {
        match self {
            // Immutable scalar builtins
            BuiltinScalarFunction::Abs => Volatility::Immutable,
            BuiltinScalarFunction::Acos => Volatility::Immutable,
            BuiltinScalarFunction::Asin => Volatility::Immutable,
            BuiltinScalarFunction::Atan => Volatility::Immutable,
            BuiltinScalarFunction::Atan2 => Volatility::Immutable,
            BuiltinScalarFunction::Acosh => Volatility::Immutable,
            BuiltinScalarFunction::Asinh => Volatility::Immutable,
            BuiltinScalarFunction::Atanh => Volatility::Immutable,
            BuiltinScalarFunction::Ceil => Volatility::Immutable,
            BuiltinScalarFunction::Coalesce => Volatility::Immutable,
            BuiltinScalarFunction::Cos => Volatility::Immutable,
            BuiltinScalarFunction::Cosh => Volatility::Immutable,
            BuiltinScalarFunction::Degrees => Volatility::Immutable,
            BuiltinScalarFunction::Exp => Volatility::Immutable,
            BuiltinScalarFunction::Factorial => Volatility::Immutable,
            BuiltinScalarFunction::Floor => Volatility::Immutable,
            BuiltinScalarFunction::Gcd => Volatility::Immutable,
            BuiltinScalarFunction::Lcm => Volatility::Immutable,
            BuiltinScalarFunction::Ln => Volatility::Immutable,
            BuiltinScalarFunction::Log => Volatility::Immutable,
            BuiltinScalarFunction::Log10 => Volatility::Immutable,
            BuiltinScalarFunction::Log2 => Volatility::Immutable,
            BuiltinScalarFunction::Pi => Volatility::Immutable,
            BuiltinScalarFunction::Power => Volatility::Immutable,
            BuiltinScalarFunction::Round => Volatility::Immutable,
            BuiltinScalarFunction::Signum => Volatility::Immutable,
            BuiltinScalarFunction::Sin => Volatility::Immutable,
            BuiltinScalarFunction::Sinh => Volatility::Immutable,
            BuiltinScalarFunction::Sqrt => Volatility::Immutable,
            BuiltinScalarFunction::Cbrt => Volatility::Immutable,
            BuiltinScalarFunction::Tan => Volatility::Immutable,
            BuiltinScalarFunction::Tanh => Volatility::Immutable,
            BuiltinScalarFunction::Trunc => Volatility::Immutable,
            BuiltinScalarFunction::MakeArray => Volatility::Immutable,
            BuiltinScalarFunction::ArrayAppend => Volatility::Immutable,
            BuiltinScalarFunction::ArrayPrepend => Volatility::Immutable,
            BuiltinScalarFunction::ArrayConcat => Volatility::Immutable,
            BuiltinScalarFunction::ArrayFill => Volatility::Immutable,
            BuiltinScalarFunction::ArrayPosition => Volatility::Immutable,
            BuiltinScalarFunction::ArrayPositions => Volatility::Immutable,
            BuiltinScalarFunction::ArrayRemove => Volatility::Immutable,
            BuiltinScalarFunction::ArrayReplace => Volatility::Immutable,
            BuiltinScalarFunction::ArrayToString => Volatility::Immutable,
            BuiltinScalarFunction::Cardinality => Volatility::Immutable,
            BuiltinScalarFunction::TrimArray => Volatility::Immutable,
            BuiltinScalarFunction::ArrayLength => Volatility::Immutable,
            BuiltinScalarFunction::ArrayDims => Volatility::Immutable,
            BuiltinScalarFunction::ArrayNdims => Volatility::Immutable,
            BuiltinScalarFunction::Ascii => Volatility::Immutable,
            BuiltinScalarFunction::BitLength => Volatility::Immutable,
            BuiltinScalarFunction::Btrim => Volatility::Immutable,
            BuiltinScalarFunction::CharacterLength => Volatility::Immutable,
            BuiltinScalarFunction::Chr => Volatility::Immutable,
            BuiltinScalarFunction::Concat => Volatility::Immutable,
            BuiltinScalarFunction::ConcatWithSeparator => Volatility::Immutable,
            BuiltinScalarFunction::DatePart => Volatility::Immutable,
            BuiltinScalarFunction::DateTrunc => Volatility::Immutable,
            BuiltinScalarFunction::DateBin => Volatility::Immutable,
            BuiltinScalarFunction::InitCap => Volatility::Immutable,
            BuiltinScalarFunction::Left => Volatility::Immutable,
            BuiltinScalarFunction::Lpad => Volatility::Immutable,
            BuiltinScalarFunction::Lower => Volatility::Immutable,
            BuiltinScalarFunction::Ltrim => Volatility::Immutable,
            BuiltinScalarFunction::MD5 => Volatility::Immutable,
            BuiltinScalarFunction::NullIf => Volatility::Immutable,
            BuiltinScalarFunction::OctetLength => Volatility::Immutable,
            BuiltinScalarFunction::Radians => Volatility::Immutable,
            BuiltinScalarFunction::RegexpReplace => Volatility::Immutable,
            BuiltinScalarFunction::Repeat => Volatility::Immutable,
            BuiltinScalarFunction::Replace => Volatility::Immutable,
            BuiltinScalarFunction::Reverse => Volatility::Immutable,
            BuiltinScalarFunction::Right => Volatility::Immutable,
            BuiltinScalarFunction::Rpad => Volatility::Immutable,
            BuiltinScalarFunction::Rtrim => Volatility::Immutable,
            BuiltinScalarFunction::SHA224 => Volatility::Immutable,
            BuiltinScalarFunction::SHA256 => Volatility::Immutable,
            BuiltinScalarFunction::SHA384 => Volatility::Immutable,
            BuiltinScalarFunction::SHA512 => Volatility::Immutable,
            BuiltinScalarFunction::Digest => Volatility::Immutable,
            BuiltinScalarFunction::SplitPart => Volatility::Immutable,
            BuiltinScalarFunction::StartsWith => Volatility::Immutable,
            BuiltinScalarFunction::Strpos => Volatility::Immutable,
            BuiltinScalarFunction::Substr => Volatility::Immutable,
            BuiltinScalarFunction::ToHex => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestamp => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMillis => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMicros => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampSeconds => Volatility::Immutable,
            BuiltinScalarFunction::Translate => Volatility::Immutable,
            BuiltinScalarFunction::Trim => Volatility::Immutable,
            BuiltinScalarFunction::Upper => Volatility::Immutable,
            BuiltinScalarFunction::RegexpMatch => Volatility::Immutable,
            BuiltinScalarFunction::Struct => Volatility::Immutable,
            BuiltinScalarFunction::FromUnixtime => Volatility::Immutable,
            BuiltinScalarFunction::ArrowTypeof => Volatility::Immutable,

            // Stable builtin functions
            BuiltinScalarFunction::Now => Volatility::Stable,
            BuiltinScalarFunction::CurrentDate => Volatility::Stable,
            BuiltinScalarFunction::CurrentTime => Volatility::Stable,

            // Volatile builtin functions
            BuiltinScalarFunction::Random => Volatility::Volatile,
            BuiltinScalarFunction::Uuid => Volatility::Volatile,
        }
    }
}

impl fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (func_name, func) in NAME_TO_FUNCTION.iter() {
            if func == self {
                return write!(f, "{}", func_name);
            }
        }

        // Should not be reached
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltinScalarFunction> {
        for (func_name, func) in NAME_TO_FUNCTION.iter() {
            if name == *func_name {
                return Ok(func.clone());
            }
        }

        Err(DataFusionError::Plan(format!(
            "There is no built-in function named {name}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test for BuiltinScalarFunction's Display and from_str() implementations.
    // For each variant in BuiltinScalarFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same.
    fn test_display_and_from_str() {
        for (_, func_original) in NAME_TO_FUNCTION.iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltinScalarFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, *func_original);
        }
    }
}

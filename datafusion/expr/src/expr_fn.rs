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

//! Functions for creating logical expressions

use crate::expr::{BinaryExpr, Cast, GroupingSet};
use crate::{
    aggregate_function, built_in_function, conditional_expressions::CaseBuilder,
    logical_plan::Subquery, AccumulatorFunctionImplementation, AggregateUDF,
    BuiltinScalarFunction, Expr, LogicalPlan, Operator, ReturnTypeFunction,
    ScalarFunctionImplementation, ScalarUDF, Signature, StateTypeFunction, Volatility,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::sync::Arc;

/// Create a column expression based on a qualified or unqualified column name
pub fn col(ident: &str) -> Expr {
    Expr::Column(ident.into())
}

/// Return a new expression left <op> right
pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// Return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::And,
        Box::new(right),
    ))
}

/// Return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Or,
        Box::new(right),
    ))
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Min,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Max,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Sum,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Avg,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Count,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Create an expression to represent the count(distinct) aggregate function
pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Count,
        distinct: true,
        args: vec![expr],
        filter: None,
    }
}

/// Create an in_list expression
pub fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList {
        expr: Box::new(expr),
        list,
        negated,
    }
}

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
pub fn concat(args: &[Expr]) -> Expr {
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::Concat,
        args: args.to_vec(),
    }
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator.
/// NULL arguments in `values` are ignored.
pub fn concat_ws(sep: Expr, values: Vec<Expr>) -> Expr {
    let mut args = values;
    args.insert(0, sep);
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::ConcatWithSeparator,
        args,
    }
}

/// Returns a random value in the range 0.0 <= x < 1.0
pub fn random() -> Expr {
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::Random,
        args: vec![],
    }
}

/// Returns the approximate number of distinct input values.
/// This function provides an approximation of count(DISTINCT x).
/// Zero is returned if all input values are null.
/// This function should produce a standard error of 0.81%,
/// which is the standard deviation of the (approximately normal)
/// error distribution over all possible sets.
/// It does not guarantee an upper bound on the error for any specific input set.
pub fn approx_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxDistinct,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Calculate an approximation of the median for `expr`.
pub fn approx_median(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxMedian,
        distinct: false,
        args: vec![expr],
        filter: None,
    }
}

/// Calculate an approximation of the specified `percentile` for `expr`.
pub fn approx_percentile_cont(expr: Expr, percentile: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxPercentileCont,
        distinct: false,
        args: vec![expr, percentile],
        filter: None,
    }
}

/// Calculate an approximation of the specified `percentile` for `expr` and `weight_expr`.
pub fn approx_percentile_cont_with_weight(
    expr: Expr,
    weight_expr: Expr,
    percentile: Expr,
) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxPercentileContWithWeight,
        distinct: false,
        args: vec![expr, weight_expr, percentile],
        filter: None,
    }
}

/// Create an EXISTS subquery expression
pub fn exists(subquery: Arc<LogicalPlan>) -> Expr {
    Expr::Exists {
        subquery: Subquery { subquery },
        negated: false,
    }
}

/// Create a NOT EXISTS subquery expression
pub fn not_exists(subquery: Arc<LogicalPlan>) -> Expr {
    Expr::Exists {
        subquery: Subquery { subquery },
        negated: true,
    }
}

/// Create an IN subquery expression
pub fn in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    Expr::InSubquery {
        expr: Box::new(expr),
        subquery: Subquery { subquery },
        negated: false,
    }
}

/// Create a NOT IN subquery expression
pub fn not_in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    Expr::InSubquery {
        expr: Box::new(expr),
        subquery: Subquery { subquery },
        negated: true,
    }
}

/// Create a scalar subquery expression
pub fn scalar_subquery(subquery: Arc<LogicalPlan>) -> Expr {
    Expr::ScalarSubquery(Subquery { subquery })
}

/// Create a grouping set
pub fn grouping_set(exprs: Vec<Vec<Expr>>) -> Expr {
    Expr::GroupingSet(GroupingSet::GroupingSets(exprs))
}

/// Create a grouping set for all combination of `exprs`
pub fn cube(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Cube(exprs))
}

/// Create a grouping set for rollup
pub fn rollup(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Rollup(exprs))
}

/// Create a cast expression
pub fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), data_type))
}

/// Create a try cast expression
pub fn try_cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::TryCast {
        expr: Box::new(expr),
        data_type,
    }
}

/// Create is null expression
pub fn is_null(expr: Expr) -> Expr {
    Expr::IsNull(Box::new(expr))
}

/// Create is true expression
pub fn is_true(expr: Expr) -> Expr {
    Expr::IsTrue(Box::new(expr))
}

/// Create is not true expression
pub fn is_not_true(expr: Expr) -> Expr {
    Expr::IsNotTrue(Box::new(expr))
}

/// Create is false expression
pub fn is_false(expr: Expr) -> Expr {
    Expr::IsFalse(Box::new(expr))
}

/// Create is not false expression
pub fn is_not_false(expr: Expr) -> Expr {
    Expr::IsNotFalse(Box::new(expr))
}

/// Create is unknown expression
pub fn is_unknown(expr: Expr) -> Expr {
    Expr::IsUnknown(Box::new(expr))
}

/// Create is not unknown expression
pub fn is_not_unknown(expr: Expr) -> Expr {
    Expr::IsNotUnknown(Box::new(expr))
}

macro_rules! scalar_expr {
    ($ENUM:ident, $FUNC:ident, $($arg:ident)*, $DOC:expr) => {
        #[doc = $DOC ]
        pub fn $FUNC($($arg: Expr),*) -> Expr {
            Expr::ScalarFunction {
                fun: built_in_function::BuiltinScalarFunction::$ENUM,
                args: vec![$($arg),*],
            }
        }
    };
}

macro_rules! nary_scalar_expr {
    ($ENUM:ident, $FUNC:ident, $DOC:expr) => {
        #[doc = $DOC ]
        pub fn $FUNC(args: Vec<Expr>) -> Expr {
            Expr::ScalarFunction {
                fun: built_in_function::BuiltinScalarFunction::$ENUM,
                args,
            }
        }
    };
}

// generate methods for creating the supported unary/binary expressions

// math functions
scalar_expr!(Sqrt, sqrt, num, "square root of a number");
scalar_expr!(Sin, sin, num, "sine");
scalar_expr!(Cos, cos, num, "cosine");
scalar_expr!(Tan, tan, num, "tangent");
scalar_expr!(Asin, asin, num, "inverse sine");
scalar_expr!(Acos, acos, num, "inverse cosine");
scalar_expr!(Atan, atan, num, "inverse tangent");
scalar_expr!(
    Floor,
    floor,
    num,
    "nearest integer less than or equal to argument"
);
scalar_expr!(
    Ceil,
    ceil,
    num,
    "nearest integer greater than or equal to argument"
);
scalar_expr!(Round, round, num, "round to nearest integer");
scalar_expr!(Trunc, trunc, num, "truncate toward zero");
scalar_expr!(Abs, abs, num, "absolute value");
scalar_expr!(Signum, signum, num, "sign of the argument (-1, 0, +1) ");
scalar_expr!(Exp, exp, num, "exponential");
scalar_expr!(Log2, log2, num, "base 2 logarithm");
scalar_expr!(Log10, log10, num, "base 10 logarithm");
scalar_expr!(Ln, ln, num, "natural logarithm");
scalar_expr!(NullIf, nullif, arg_1 arg_2, "returns NULL if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE expression.");
scalar_expr!(Power, power, base exponent, "`base` raised to the power of `exponent`");
scalar_expr!(Atan2, atan2, y x, "inverse tangent of a division given in the argument");
scalar_expr!(
    ToHex,
    to_hex,
    num,
    "returns the hexdecimal representation of an integer"
);
scalar_expr!(Uuid, uuid, , "Returns uuid v4 as a string value");

// string functions
scalar_expr!(Ascii, ascii, chr, "ASCII code value of the character");
scalar_expr!(
    BitLength,
    bit_length,
    string,
    "the number of bits in the `string`"
);
scalar_expr!(
    CharacterLength,
    character_length,
    string,
    "the number of characters in the `string`"
);
scalar_expr!(
    Chr,
    chr,
    code_point,
    "converts the Unicode code point to a UTF8 character"
);
scalar_expr!(Digest, digest, input algorithm, "compute the binary hash of `input`, using the `algorithm`");
scalar_expr!(InitCap, initcap, string, "converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase");
scalar_expr!(Left, left, string n, "returns the first `n` characters in the `string`");
scalar_expr!(Lower, lower, string, "convert the string to lower case");
scalar_expr!(
    Ltrim,
    ltrim,
    string,
    "removes all characters, spaces by default, from the beginning of a string"
);
scalar_expr!(MD5, md5, string, "returns the MD5 hash of a string");
scalar_expr!(
    OctetLength,
    octet_length,
    string,
    "returns the number of bytes of a string"
);
scalar_expr!(Replace, replace, string from to, "replaces all occurrences of `from` with `to` in the `string`");
scalar_expr!(Repeat, repeat, string n, "repeats the `string` to `n` times");
scalar_expr!(Reverse, reverse, string, "reverses the `string`");
scalar_expr!(Right, right, string n, "returns the last `n` characters in the `string`");
scalar_expr!(
    Rtrim,
    rtrim,
    string,
    "removes all characters, spaces by default, from the end of a string"
);
scalar_expr!(SHA224, sha224, string, "SHA-224 hash");
scalar_expr!(SHA256, sha256, string, "SHA-256 hash");
scalar_expr!(SHA384, sha384, string, "SHA-384 hash");
scalar_expr!(SHA512, sha512, string, "SHA-512 hash");
scalar_expr!(SplitPart, split_part, string delimiter index, "splits a string based on a delimiter and picks out the desired field based on the index. ");
scalar_expr!(StartsWith, starts_with, string prefix, "whether the `string` starts with the `prefix`");
scalar_expr!(Strpos, strpos, string substring, "finds the position from where the `substring` matchs the `string`");
scalar_expr!(Substr, substr, string position, "substring from the `position` to the end");
scalar_expr!(Substr, substring, string position length, "substring from the `position` with `length` characters");
scalar_expr!(Translate, translate, string from to, "replaces the characters in `from` with the counterpart in `to`");
scalar_expr!(
    Trim,
    trim,
    string,
    "removes all characters, space by default from the string"
);
scalar_expr!(Upper, upper, string, "converts the string to upper case");
//use vec as parameter
nary_scalar_expr!(
    Lpad,
    lpad,
    "fill up a string to the length by prepending the characters"
);
nary_scalar_expr!(
    Rpad,
    rpad,
    "fill up a string to the length by appending the characters"
);
nary_scalar_expr!(
    RegexpReplace,
    regexp_replace,
    "replace strings that match a regular expression"
);
nary_scalar_expr!(
    RegexpMatch,
    regexp_match,
    "matches a regular expression against a string and returns matched substrings."
);
nary_scalar_expr!(
    Btrim,
    btrim,
    "removes all characters, spaces by default, from both sides of a string"
);
nary_scalar_expr!(
    MakeArray,
    array,
    "returns an array of fixed size with each argument on it."
);
nary_scalar_expr!(Coalesce, coalesce, "returns `coalesce(args...)`, which evaluates to the value of the first [Expr] which is not NULL");
//there is a func concat_ws before, so use concat_ws_expr as name.c
nary_scalar_expr!(
    ConcatWithSeparator,
    concat_ws_expr,
    "concatenates several strings, placing a seperator between each one"
);
nary_scalar_expr!(Concat, concat_expr, "concatenates several strings");

// date functions
scalar_expr!(DatePart, date_part, part date, "extracts a subfield from the date");
scalar_expr!(DateTrunc, date_trunc, part date, "truncates the date to a specified level of precision");
scalar_expr!(DateBin, date_bin, stride source origin, "coerces an arbitrary timestamp to the start of the nearest specified interval");
scalar_expr!(
    ToTimestampMillis,
    to_timestamp_millis,
    date,
    "converts a string to a `Timestamp(Milliseconds, None)`"
);
scalar_expr!(
    ToTimestampMicros,
    to_timestamp_micros,
    date,
    "converts a string to a `Timestamp(Microseconds, None)`"
);
scalar_expr!(
    ToTimestampSeconds,
    to_timestamp_seconds,
    date,
    "converts a string to a `Timestamp(Seconds, None)`"
);
scalar_expr!(
    FromUnixtime,
    from_unixtime,
    unixtime,
    "returns the unix time in format"
);
scalar_expr!(CurrentDate, current_date, ,"returns current UTC date as a [`DataType::Date32`] value");
scalar_expr!(Now, now, ,"returns current timestamp in nanoseconds, using the same value for all instances of now() in same statement");
scalar_expr!(CurrentTime, current_time, , "returns current UTC time as a [`DataType::Time64`] value");

scalar_expr!(ArrowTypeof, arrow_typeof, val, "data type");

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
pub fn case(expr: Expr) -> CaseBuilder {
    CaseBuilder::new(Some(Box::new(expr)), vec![], vec![], None)
}

/// Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
pub fn when(when: Expr, then: Expr) -> CaseBuilder {
    CaseBuilder::new(None, vec![when], vec![then], None)
}

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
#[allow(clippy::rc_buffer)]
pub fn create_udaf(
    name: &str,
    input_type: DataType,
    return_type: Arc<DataType>,
    volatility: Volatility,
    accumulator: AccumulatorFunctionImplementation,
    state_type: Arc<Vec<DataType>>,
) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));
    AggregateUDF::new(
        name,
        &Signature::exact(vec![input_type], volatility),
        &return_type,
        &accumulator,
        &state_type,
    )
}

/// Calls a named built in function
/// ```
/// use datafusion_expr::{col, lit, call_fn};
///
/// // create the expression sin(x) < 0.2
/// let expr = call_fn("sin", vec![col("x")]).unwrap().lt(lit(0.2));
/// ```
pub fn call_fn(name: impl AsRef<str>, args: Vec<Expr>) -> Result<Expr> {
    match name.as_ref().parse::<BuiltinScalarFunction>() {
        Ok(fun) => Ok(Expr::ScalarFunction { fun, args }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::lit;

    #[test]
    fn filter_is_null_and_is_not_null() {
        let col_null = col("col1");
        let col_not_null = col("col2");
        assert_eq!(format!("{:?}", col_null.is_null()), "col1 IS NULL");
        assert_eq!(
            format!("{:?}", col_not_null.is_not_null()),
            "col2 IS NOT NULL"
        );
    }

    macro_rules! test_unary_scalar_expr {
        ($ENUM:ident, $FUNC:ident) => {{
            if let Expr::ScalarFunction { fun, args } = $FUNC(col("tableA.a")) {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(1, args.len());
            } else {
                assert!(false, "unexpected");
            }
        }};
    }

    macro_rules! test_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                $(
                    col(stringify!($arg.to_string()))
                ),*
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    macro_rules! test_nary_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                vec![
                    $(
                        col(stringify!($arg.to_string()))
                    ),*
                ]
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    #[test]
    fn scalar_function_definitions() {
        test_unary_scalar_expr!(Sqrt, sqrt);
        test_unary_scalar_expr!(Sin, sin);
        test_unary_scalar_expr!(Cos, cos);
        test_unary_scalar_expr!(Tan, tan);
        test_unary_scalar_expr!(Asin, asin);
        test_unary_scalar_expr!(Acos, acos);
        test_unary_scalar_expr!(Atan, atan);
        test_unary_scalar_expr!(Floor, floor);
        test_unary_scalar_expr!(Ceil, ceil);
        test_unary_scalar_expr!(Round, round);
        test_unary_scalar_expr!(Trunc, trunc);
        test_unary_scalar_expr!(Abs, abs);
        test_unary_scalar_expr!(Signum, signum);
        test_unary_scalar_expr!(Exp, exp);
        test_unary_scalar_expr!(Log2, log2);
        test_unary_scalar_expr!(Log10, log10);
        test_unary_scalar_expr!(Ln, ln);
        test_scalar_expr!(Atan2, atan2, y, x);

        test_scalar_expr!(Ascii, ascii, input);
        test_scalar_expr!(BitLength, bit_length, string);
        test_nary_scalar_expr!(Btrim, btrim, string);
        test_nary_scalar_expr!(Btrim, btrim, string, characters);
        test_scalar_expr!(CharacterLength, character_length, string);
        test_scalar_expr!(Chr, chr, string);
        test_scalar_expr!(Digest, digest, string, algorithm);
        test_scalar_expr!(InitCap, initcap, string);
        test_scalar_expr!(Left, left, string, count);
        test_scalar_expr!(Lower, lower, string);
        test_nary_scalar_expr!(Lpad, lpad, string, count);
        test_nary_scalar_expr!(Lpad, lpad, string, count, characters);
        test_scalar_expr!(Ltrim, ltrim, string);
        test_scalar_expr!(MD5, md5, string);
        test_scalar_expr!(OctetLength, octet_length, string);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern, flags);
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement
        );
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement,
            flags
        );
        test_scalar_expr!(Replace, replace, string, from, to);
        test_scalar_expr!(Repeat, repeat, string, count);
        test_scalar_expr!(Reverse, reverse, string);
        test_scalar_expr!(Right, right, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count, characters);
        test_scalar_expr!(Rtrim, rtrim, string);
        test_scalar_expr!(SHA224, sha224, string);
        test_scalar_expr!(SHA256, sha256, string);
        test_scalar_expr!(SHA384, sha384, string);
        test_scalar_expr!(SHA512, sha512, string);
        test_scalar_expr!(SplitPart, split_part, expr, delimiter, index);
        test_scalar_expr!(StartsWith, starts_with, string, characters);
        test_scalar_expr!(Strpos, strpos, string, substring);
        test_scalar_expr!(Substr, substr, string, position);
        test_scalar_expr!(Substr, substring, string, position, count);
        test_scalar_expr!(ToHex, to_hex, string);
        test_scalar_expr!(Translate, translate, string, from, to);
        test_scalar_expr!(Trim, trim, string);
        test_scalar_expr!(Upper, upper, string);

        test_scalar_expr!(DatePart, date_part, part, date);
        test_scalar_expr!(DateTrunc, date_trunc, part, date);
        test_scalar_expr!(DateBin, date_bin, stride, source, origin);
        test_scalar_expr!(FromUnixtime, from_unixtime, unixtime);

        test_unary_scalar_expr!(ArrowTypeof, arrow_typeof);
    }

    #[test]
    fn uuid_function_definitions() {
        if let Expr::ScalarFunction { fun, args } = uuid() {
            let name = built_in_function::BuiltinScalarFunction::Uuid;
            assert_eq!(name, fun);
            assert_eq!(0, args.len());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn digest_function_definitions() {
        if let Expr::ScalarFunction { fun, args } = digest(col("tableA.a"), lit("md5")) {
            let name = BuiltinScalarFunction::Digest;
            assert_eq!(name, fun);
            assert_eq!(2, args.len());
        } else {
            unreachable!();
        }
    }
}

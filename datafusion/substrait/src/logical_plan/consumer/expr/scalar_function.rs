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

use crate::logical_plan::consumer::{
    SubstraitConsumer, from_substrait_func_args, from_substrait_type_without_names,
};
use crate::logical_plan::decimal::DecimalArithmetic;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, validate_decimal_precision_and_scale,
};
use datafusion::common::Result;
use datafusion::common::{
    DFSchema, DataFusionError, ScalarValue, not_impl_err, plan_err, substrait_err,
};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Like, Operator, ScalarUDF, expr,
};
use std::vec::Drain;
use substrait::proto::expression::ScalarFunction;
use substrait::proto::r#type::Kind;

pub async fn from_scalar_function(
    consumer: &impl SubstraitConsumer,
    f: &ScalarFunction,
    input_schema: &DFSchema,
) -> Result<Expr> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&f.function_reference)
    else {
        return plan_err!(
            "Scalar function not found: function reference = {:?}",
            f.function_reference
        );
    };

    let fn_name = substrait_fun_name(fn_signature);
    let args = from_substrait_func_args(consumer, &f.arguments, input_schema).await?;

    let higher_order_func = consumer
        .get_function_registry()
        .higher_order_function(fn_name)
        .or_else(|e| {
            if let Some(alt_name) = substrait_to_df_name(fn_name) {
                consumer
                    .get_function_registry()
                    .higher_order_function(alt_name)
                    .or(Err(e))
            } else {
                Err(e)
            }
        });

    let udf_func = consumer.get_function_registry().udf(fn_name).or_else(|e| {
        if let Some(alt_name) = substrait_to_df_name(fn_name) {
            consumer.get_function_registry().udf(alt_name).or(Err(e))
        } else {
            Err(e)
        }
    });

    // try to first match the requested function into registered higher-order functions, then udfs, built-in ops
    // and finally built-in expressions
    if let Ok(func) = higher_order_func {
        Ok(Expr::HigherOrderFunction(expr::HigherOrderFunction::new(
            func.to_owned(),
            args,
        )))
    } else if let Ok(func) = udf_func {
        Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
            func.to_owned(),
            args,
        )))
    } else if let Some(op) = name_to_op(fn_name) {
        if args.len() < 2 {
            return not_impl_err!(
                "Expect at least two arguments for binary operator {op:?}, the provided number of operators is {:?}",
                f.arguments.len()
            );
        }
        // In those cases we build a balanced tree of BinaryExprs
        let expr = arg_list_to_binary_op_tree(op, args)?;
        if matches!(
            op,
            Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
        ) {
            return apply_decimal_output_type(
                consumer,
                f,
                fn_signature,
                expr,
                input_schema,
            );
        }
        Ok(expr)
    } else if let Some(builder) = BuiltinExprBuilder::try_from_name(fn_name) {
        builder.build(consumer, f, args)
    } else {
        not_impl_err!("Unsupported function name: {fn_name:?}")
    }
}

/// Preserve the declared decimal type with arithmetic evaluated at that scale.
/// Keep native expressions when their result type matches and no explicit
/// function options require different execution behavior.
fn apply_decimal_output_type(
    consumer: &impl SubstraitConsumer,
    f: &ScalarFunction,
    fn_signature: &str,
    expr: Expr,
    input_schema: &DFSchema,
) -> Result<Expr> {
    // Preserve compatibility with existing plans that omit the output type.
    let Some(output_type) = &f.output_type else {
        return Ok(expr);
    };
    // Native inference can itself fail (for example, multiplication with an
    // intermediate scale over 38). A valid declared decimal type can still be
    // implemented by DecimalArithmetic in that case.
    let derived_type = expr.get_type(input_schema);
    if !matches!(output_type.kind, Some(Kind::Decimal(_)))
        && !derived_type.as_ref().is_ok_and(|dt| dt.is_decimal())
    {
        return Ok(expr);
    }

    // The type decoder narrows these protobuf integers with `as`. Check them
    // first so an invalid declaration cannot wrap into a matching type.
    if let Some(Kind::Decimal(decimal)) = &output_type.kind
        && (u8::try_from(decimal.precision).is_err()
            || i8::try_from(decimal.scale).is_err())
    {
        return substrait_err!(
            "Invalid decimal output type for {fn_signature}: precision {}, scale {}",
            decimal.precision,
            decimal.scale
        );
    }
    let declared_type = from_substrait_type_without_names(consumer, output_type)?;
    match &declared_type {
        DataType::Decimal128(p, s) => {
            validate_decimal_precision_and_scale::<Decimal128Type>(*p, *s)?;
        }
        DataType::Decimal256(p, s) => {
            validate_decimal_precision_and_scale::<Decimal256Type>(*p, *s)?;
        }
        _ => {}
    }
    if derived_type.as_ref().is_ok_and(|dt| *dt == declared_type) && f.options.is_empty()
    {
        return Ok(expr);
    }
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        unreachable!("called only for native binary expressions")
    };
    if f.arguments.len() != 2 {
        return substrait_err!(
            "Declared decimal arithmetic requires two arguments for {fn_signature}"
        );
    }
    let function = DecimalArithmetic::try_new(
        fn_signature,
        op,
        [left.get_type(input_schema)?, right.get_type(input_schema)?],
        &declared_type,
        &f.options,
    )?;
    Ok(ScalarUDF::from(function).call(vec![*left, *right]))
}

pub fn substrait_fun_name(name: &str) -> &str {
    (match name.rsplit_once(':') {
        // Since 0.32.0, Substrait requires the function names to be in a compound format
        // https://substrait.io/extensions/#function-signature-compound-names
        // for example, `add:i8_i8`.
        // On the consumer side, we don't really care about the signature though, just the name.
        Some((name, _)) => name,
        None => name,
    }) as _
}

pub fn name_to_op(name: &str) -> Option<Operator> {
    match name {
        "equal" => Some(Operator::Eq),
        "not_equal" => Some(Operator::NotEq),
        "lt" => Some(Operator::Lt),
        "lte" => Some(Operator::LtEq),
        "gt" => Some(Operator::Gt),
        "gte" => Some(Operator::GtEq),
        "add" => Some(Operator::Plus),
        "subtract" => Some(Operator::Minus),
        "multiply" => Some(Operator::Multiply),
        "divide" => Some(Operator::Divide),
        "mod" => Some(Operator::Modulo),
        "modulus" => Some(Operator::Modulo),
        "and" => Some(Operator::And),
        "or" => Some(Operator::Or),
        "is_distinct_from" => Some(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Some(Operator::IsNotDistinctFrom),
        "regex_match" => Some(Operator::RegexMatch),
        "regex_imatch" => Some(Operator::RegexIMatch),
        "regex_not_match" => Some(Operator::RegexNotMatch),
        "regex_not_imatch" => Some(Operator::RegexNotIMatch),
        "bitwise_and" => Some(Operator::BitwiseAnd),
        "bitwise_or" => Some(Operator::BitwiseOr),
        "str_concat" => Some(Operator::StringConcat),
        "at_arrow" => Some(Operator::AtArrow),
        "arrow_at" => Some(Operator::ArrowAt),
        "bitwise_xor" => Some(Operator::BitwiseXor),
        "bitwise_shift_right" => Some(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Some(Operator::BitwiseShiftLeft),
        _ => None,
    }
}

pub fn substrait_to_df_name(name: &str) -> Option<&str> {
    match name {
        "is_nan" => Some("isnan"),
        _ => None,
    }
}

/// Build a balanced tree of binary operations from a binary operator and a list of arguments.
///
/// For example, `OR` `(a, b, c, d, e)` will be converted to: `OR(OR(a, OR(b, c)), OR(d, e))`.
///
/// `args` must not be empty.
fn arg_list_to_binary_op_tree(op: Operator, mut args: Vec<Expr>) -> Result<Expr> {
    let n_args = args.len();
    let mut drained_args = args.drain(..);
    arg_list_to_binary_op_tree_inner(op, &mut drained_args, n_args)
}

/// Helper function for [`arg_list_to_binary_op_tree`] implementation
///
/// `take_len` represents the number of elements to take from `args` before returning.
/// We use `take_len` to avoid recursively building a `Take<Take<Take<...>>>` type.
fn arg_list_to_binary_op_tree_inner(
    op: Operator,
    args: &mut Drain<Expr>,
    take_len: usize,
) -> Result<Expr> {
    if take_len == 1 {
        return args.next().ok_or_else(|| {
            DataFusionError::Substrait(
                "Expected one more available element in iterator, found none".to_string(),
            )
        });
    } else if take_len == 0 {
        return substrait_err!("Cannot build binary operation tree with 0 arguments");
    }
    // Cut argument list in 2 balanced parts
    let left_take = take_len / 2;
    let right_take = take_len - left_take;
    let left = arg_list_to_binary_op_tree_inner(op, args, left_take)?;
    let right = arg_list_to_binary_op_tree_inner(op, args, right_take)?;
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

/// Build [`Expr`] from its name and required inputs.
struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "like_match" | "like_imatch"
            | "like_not_match" | "like_not_imatch" | "is_null" | "is_not_null"
            | "is_true" | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" | "negate" | "and_not" | "xor"
            | "between" | "logb" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub fn build(
        self,
        consumer: &impl SubstraitConsumer,
        f: &ScalarFunction,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        match self.expr_name.as_str() {
            "like" => Self::build_like_expr(false, false, f, args),
            "ilike" => Self::build_like_expr(true, false, f, args),
            "like_match" => Self::build_like_expr(false, false, f, args),
            "like_imatch" => Self::build_like_expr(true, false, f, args),
            "like_not_match" => Self::build_like_expr(false, true, f, args),
            "like_not_imatch" => Self::build_like_expr(true, true, f, args),
            "not" | "negative" | "negate" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" => Self::build_unary_expr(&self.expr_name, args),
            "and_not" | "xor" => Self::build_binary_expr(&self.expr_name, args),
            "between" => Self::build_between_expr(&self.expr_name, args),
            "logb" => Self::build_custom_handling_expr(consumer, &self.expr_name, args),
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    fn build_unary_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let Ok([arg]) = <[Expr; 1]>::try_from(args) else {
            return substrait_err!("Expected one argument for {fn_name} expr");
        };
        let arg = Box::new(arg);

        let expr = match fn_name {
            "not" => Expr::Not(arg),
            "negative" | "negate" => Expr::Negative(arg),
            "is_null" => Expr::IsNull(arg),
            "is_not_null" => Expr::IsNotNull(arg),
            "is_true" => Expr::IsTrue(arg),
            "is_false" => Expr::IsFalse(arg),
            "is_not_true" => Expr::IsNotTrue(arg),
            "is_not_false" => Expr::IsNotFalse(arg),
            "is_unknown" => Expr::IsUnknown(arg),
            "is_not_unknown" => Expr::IsNotUnknown(arg),
            _ => return not_impl_err!("Unsupported builtin expression: {}", fn_name),
        };

        Ok(expr)
    }

    fn build_like_expr(
        case_insensitive: bool,
        negated: bool,
        f: &ScalarFunction,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if args.len() != 2 && args.len() != 3 {
            return substrait_err!("Expect two or three arguments for `{fn_name}` expr");
        }

        let mut args_iter = args.into_iter();
        let Some(expr) = args_iter.next() else {
            return substrait_err!("Missing first argument for {fn_name} expression");
        };
        let Some(pattern) = args_iter.next() else {
            return substrait_err!("Missing second argument for {fn_name} expression");
        };

        // Default case: escape character is Literal(Utf8(None))
        let escape_char = if f.arguments.len() == 3 {
            let Some(escape_char_expr) = args_iter.next() else {
                return substrait_err!("Missing third argument for {fn_name} expression");
            };

            match escape_char_expr {
                Expr::Literal(ScalarValue::Utf8(escape_char_string), _) => {
                    // Convert Option<String> to Option<char>
                    escape_char_string.and_then(|s| s.chars().next())
                }
                _ => {
                    return substrait_err!(
                        "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
                    );
                }
            }
        } else {
            None
        };

        Ok(Expr::Like(Like {
            negated,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char,
            case_insensitive,
        }))
    }

    fn build_binary_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let Ok([a, b]) = <[Expr; 2]>::try_from(args) else {
            return substrait_err!("Expected two arguments for `{fn_name}` expr");
        };
        match fn_name {
            "and_not" => Ok(Self::build_and_not_expr(a, b)),
            "xor" => Ok(Self::build_xor_expr(a, b)),
            _ => not_impl_err!("Unsupported builtin expression: {}", fn_name),
        }
    }

    fn build_and_not_expr(a: Expr, b: Expr) -> Expr {
        a.and(Expr::Not(Box::new(b)))
    }

    fn build_xor_expr(a: Expr, b: Expr) -> Expr {
        let or_expr = a.clone().or(b.clone());
        let and_expr = a.and(b);
        Self::build_and_not_expr(or_expr, and_expr)
    }

    fn build_between_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let Ok([expression, low, high]) = <[Expr; 3]>::try_from(args) else {
            return substrait_err!("Expected three arguments for `{fn_name}` expr");
        };

        Ok(Expr::Between(Between {
            expr: Box::new(expression),
            negated: false,
            low: Box::new(low),
            high: Box::new(high),
        }))
    }

    //This handles any functions that require custom handling
    fn build_custom_handling_expr(
        consumer: &impl SubstraitConsumer,
        fn_name: &str,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        match fn_name {
            "logb" => Self::build_logb_expr(consumer, args),
            _ => not_impl_err!("Unsupported custom handled expression: {}", fn_name),
        }
    }

    fn build_logb_expr(
        consumer: &impl SubstraitConsumer,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        if args.len() != 2 {
            return substrait_err!("Expect two arguments for logb function");
        }

        let mut args = args;
        args.swap(0, 1);

        //The equivalent of logb in DataFusion is the log function (which has its arguments in reverse order)
        if let Ok(func) = consumer.get_function_registry().udf("log") {
            Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
                func.to_owned(),
                args,
            )))
        } else {
            not_impl_err!("Unsupported function name: logb")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::arg_list_to_binary_op_tree;
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::tests::TEST_SESSION_STATE;
    use crate::logical_plan::consumer::{DefaultSubstraitConsumer, SubstraitConsumer};
    use crate::logical_plan::producer::{
        DefaultSubstraitProducer, substrait_field_ref, to_substrait_type,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{DFSchema, Result, ScalarValue};
    use datafusion::logical_expr::{Expr, ExprSchemable, Operator};
    use insta::assert_snapshot;
    use substrait::proto::expression::literal::LiteralType;
    use substrait::proto::expression::{Literal, RexType, ScalarFunction};
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::{Expression, FunctionArgument};

    fn decimal_function(
        name: &str,
        left_type: DataType,
        right_type: DataType,
        output_type: Option<DataType>,
    ) -> Result<(Extensions, ScalarFunction, DFSchema)> {
        let mut extensions = Extensions::default();
        extensions.functions.insert(0, format!("{name}:dec_dec"));
        let mut producer = DefaultSubstraitProducer::new(&TEST_SESSION_STATE);
        let func = ScalarFunction {
            function_reference: 0,
            arguments: (0..2)
                .map(|index| {
                    Ok(FunctionArgument {
                        arg_type: Some(ArgType::Value(substrait_field_ref(index)?)),
                    })
                })
                .collect::<Result<_>>()?,
            output_type: output_type
                .map(|dt| to_substrait_type(&mut producer, &dt, false))
                .transpose()?,
            ..Default::default()
        };
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("a", left_type, false),
            Field::new("b", right_type, false),
        ]))?;
        Ok((extensions, func, schema))
    }

    #[tokio::test]
    async fn test_decimal_arithmetic_output_types() -> Result<()> {
        use DataType::Decimal128 as D;

        // The five cases from #25043, plus subtraction and both remainder names.
        for (name, left, right, declared, derived) in [
            ("add", D(10, 2), D(5, 1), D(11, 2), D(11, 2)),
            ("add", D(38, 10), D(38, 10), D(38, 9), D(38, 10)),
            ("multiply", D(10, 2), D(5, 1), D(16, 3), D(16, 3)),
            ("multiply", D(38, 10), D(38, 10), D(38, 6), D(38, 20)),
            ("divide", D(10, 2), D(5, 1), D(21, 8), D(15, 6)),
            ("subtract", D(38, 10), D(38, 10), D(38, 9), D(38, 10)),
            ("modulus", D(10, 2), D(5, 1), D(6, 2), D(6, 2)),
            ("mod", D(10, 2), D(5, 1), D(7, 2), D(6, 2)),
            (
                "add",
                DataType::Decimal256(10, 2),
                DataType::Decimal256(5, 1),
                DataType::Decimal256(11, 2),
                DataType::Decimal256(11, 2),
            ),
        ] {
            let (extensions, func, schema) =
                decimal_function(name, left, right, Some(declared.clone()))?;
            let consumer =
                DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
            let expr = consumer.consume_scalar_function(&func, &schema).await?;
            assert_eq!(expr.get_type(&schema)?, declared);
            if declared == derived {
                assert!(matches!(expr, Expr::BinaryExpr(_)));
            } else {
                assert!(matches!(expr, Expr::ScalarFunction(_)));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_output_type_compatibility() -> Result<()> {
        for output_type in [None, Some(DataType::Decimal128(15, 6))] {
            let (extensions, func, schema) = decimal_function(
                "divide",
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 1),
                output_type,
            )?;
            let consumer =
                DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
            let expr = consumer.consume_scalar_function(&func, &schema).await?;
            assert!(matches!(expr, Expr::BinaryExpr(_)));
            assert_eq!(expr.get_type(&schema)?, DataType::Decimal128(15, 6));
        }

        for (left, right, declared) in [
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 1),
                DataType::Decimal256(11, 2),
            ),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 1),
                DataType::Float64,
            ),
            (
                DataType::Int64,
                DataType::Int64,
                DataType::Decimal128(20, 0),
            ),
        ] {
            let (extensions, func, schema) =
                decimal_function("add", left, right, Some(declared))?;
            let consumer =
                DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
            let err = consumer
                .consume_scalar_function(&func, &schema)
                .await
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("Unsupported decimal arithmetic type"),
                "{err}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_decimal_output_type() -> Result<()> {
        for (precision, scale) in
            [(267, 2), (11, 258), (-245, 2), (0, 0), (39, 2), (11, 12)]
        {
            let (extensions, mut func, schema) = decimal_function(
                "add",
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 1),
                Some(DataType::Decimal128(11, 2)),
            )?;
            let Some(super::Kind::Decimal(decimal)) =
                &mut func.output_type.as_mut().unwrap().kind
            else {
                unreachable!()
            };
            decimal.precision = precision;
            decimal.scale = scale;
            let consumer =
                DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
            assert!(
                consumer
                    .consume_scalar_function(&func, &schema)
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_nested_decimal_output_type() -> Result<()> {
        let (mut extensions, mut func, schema) = decimal_function(
            "divide",
            DataType::Decimal128(10, 2),
            DataType::Decimal128(5, 1),
            Some(DataType::Decimal128(21, 8)),
        )?;
        extensions.functions.insert(1, "add:dec_dec".to_string());
        let inner = Expression {
            rex_type: Some(RexType::ScalarFunction(func.clone())),
        };
        func.function_reference = 1;
        func.output_type = None;
        func.arguments[0].arg_type = Some(ArgType::Value(inner));
        let consumer = DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
        let expr = consumer.consume_scalar_function(&func, &schema).await?;
        assert_eq!(expr.get_type(&schema)?, DataType::Decimal128(22, 8));
        Ok(())
    }

    #[tokio::test]
    async fn test_declared_decimal_execution() -> Result<()> {
        use datafusion::arrow::array::Decimal128Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        // Include cases whose *native type inference* fails because the
        // intermediate scale exceeds 38, as well as native execution overflow.
        for (name, left_type, right_type, output, left, right, expected) in [
            ("divide", (10, 2), (5, 1), (21, 8), 100, 30, 33_333_333),
            (
                "divide",
                (10, 2),
                (10, 2),
                (31, 13),
                100,
                300,
                3_333_333_333_333,
            ),
            ("divide", (18, 2), (18, 2), (38, 6), 100, 300, 333_333),
            (
                "divide",
                (38, 38),
                (38, 38),
                (38, 6),
                10_i128.pow(37),
                10_i128.pow(37),
                1_000_000,
            ),
            (
                "multiply",
                (38, 38),
                (38, 38),
                (38, 6),
                10_i128.pow(37),
                10_i128.pow(37),
                10_000,
            ),
            (
                "add",
                (38, 10),
                (38, 10),
                (38, 9),
                9 * 10_i128.pow(37),
                9 * 10_i128.pow(37),
                18 * 10_i128.pow(36),
            ),
            (
                "subtract",
                (38, 10),
                (38, 10),
                (38, 9),
                9 * 10_i128.pow(37),
                9 * 10_i128.pow(37) - 6,
                1,
            ),
            ("modulus", (10, 3), (5, 1), (5, 2), 12_345, 20, 35),
        ] {
            let left_type = DataType::Decimal128(left_type.0, left_type.1);
            let right_type = DataType::Decimal128(right_type.0, right_type.1);
            let (extensions, mut func, _) = decimal_function(
                name,
                left_type.clone(),
                right_type.clone(),
                Some(DataType::Decimal128(output.0, output.1)),
            )?;
            if let Some(super::Kind::Decimal(decimal)) =
                &mut func.output_type.as_mut().unwrap().kind
            {
                decimal.nullability =
                    substrait::proto::r#type::Nullability::Nullable as i32;
            }
            let ctx = SessionContext::new();
            let state = ctx.state();
            let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", left_type.clone(), true),
                Field::new("b", right_type.clone(), true),
            ]));
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
            let expr = consumer.consume_scalar_function(&func, &df_schema).await?;
            let DataType::Decimal128(lp, ls) = left_type else {
                unreachable!()
            };
            let DataType::Decimal128(rp, rs) = right_type else {
                unreachable!()
            };
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(
                        Decimal128Array::from(vec![Some(left), None, Some(-left)])
                            .with_precision_and_scale(lp, ls)?,
                    ),
                    Arc::new(
                        Decimal128Array::from(vec![Some(right), Some(0), Some(-right)])
                            .with_precision_and_scale(rp, rs)?,
                    ),
                ],
            )?;
            ctx.register_batch("decimals", batch)?;
            let batches = ctx
                .table("decimals")
                .await?
                .select(vec![expr])?
                .collect()
                .await?;
            assert_eq!(
                ScalarValue::try_from_array(batches[0].column(0), 0)?,
                ScalarValue::Decimal128(Some(expected), output.0, output.1),
                "{name}"
            );
            // NULL propagation must skip arithmetic, including division by 0.
            assert_eq!(
                ScalarValue::try_from_array(batches[0].column(0), 1)?,
                ScalarValue::Decimal128(None, output.0, output.1),
                "{name}"
            );
            let negative = if matches!(name, "divide" | "multiply") {
                expected
            } else {
                -expected
            };
            assert_eq!(
                ScalarValue::try_from_array(batches[0].column(0), 2)?,
                ScalarValue::Decimal128(Some(negative), output.0, output.1),
                "{name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_declared_decimal_scalar_and_array_arguments() -> Result<()> {
        use crate::logical_plan::producer::to_substrait_literal_expr;
        use datafusion::arrow::array::{Array, Decimal128Array};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        for scalar_left in [false, true] {
            for scalar_right in [false, true] {
                let (extensions, mut func, schema) = decimal_function(
                    "divide",
                    DataType::Decimal128(10, 2),
                    DataType::Decimal128(5, 1),
                    Some(DataType::Decimal128(21, 8)),
                )?;
                let mut producer = DefaultSubstraitProducer::new(&TEST_SESSION_STATE);
                for (index, scalar, value) in [
                    (0, scalar_left, ScalarValue::Decimal128(Some(200), 10, 2)),
                    (1, scalar_right, ScalarValue::Decimal128(Some(30), 5, 1)),
                ] {
                    if scalar {
                        func.arguments[index].arg_type = Some(ArgType::Value(
                            to_substrait_literal_expr(&mut producer, &value)?,
                        ));
                    }
                }
                let consumer =
                    DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);
                let expr = consumer.consume_scalar_function(&func, &schema).await?;
                let physical = TEST_SESSION_STATE.create_physical_expr(expr, &schema)?;
                for rows in [0, 3] {
                    let batch = RecordBatch::try_new(
                        Arc::new(schema.as_arrow().clone()),
                        vec![
                            Arc::new(
                                Decimal128Array::from(vec![200; rows])
                                    .with_precision_and_scale(10, 2)?,
                            ),
                            Arc::new(
                                Decimal128Array::from(vec![30; rows])
                                    .with_precision_and_scale(5, 1)?,
                            ),
                        ],
                    )?;
                    let result = physical.evaluate(&batch)?.into_array(rows)?;
                    assert_eq!(result.len(), rows);
                    if rows > 0 {
                        assert_eq!(
                            ScalarValue::try_from_array(&result, 2)?,
                            ScalarValue::Decimal128(Some(66_666_667), 21, 8)
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_output_types_survive_optimization() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, Decimal128Array};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let state = ctx.state();
        let mut expressions = Vec::new();
        for (scale, alias) in [(8, "eight"), (7, "seven")] {
            let (extensions, func, schema) = decimal_function(
                "divide",
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 1),
                Some(DataType::Decimal128(21, scale)),
            )?;
            let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
            expressions.push(
                consumer
                    .consume_scalar_function(&func, &schema)
                    .await?
                    .alias(alias),
            );
        }
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(
                    Decimal128Array::from(vec![100]).with_precision_and_scale(10, 2)?,
                ) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Decimal128Array::from(vec![30]).with_precision_and_scale(5, 1)?),
            ),
        ])?;
        ctx.register_batch("decimals", batch)?;
        let batches = ctx
            .table("decimals")
            .await?
            .select(expressions)?
            .collect()
            .await?;
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0)?,
            ScalarValue::Decimal128(Some(33_333_333), 21, 8)
        );
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(1), 0)?,
            ScalarValue::Decimal128(Some(3_333_333), 21, 7)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_overflow_option_survives_roundtrip() -> Result<()> {
        use crate::logical_plan::consumer::from_substrait_plan;
        use crate::logical_plan::producer::to_substrait_plan;
        use datafusion::arrow::array::{ArrayRef, Decimal128Array};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;
        use substrait::proto::FunctionOption;

        let ctx = SessionContext::new();
        let state = ctx.state();
        // The declared type matches Arrow's, but the explicit ERROR option
        // must still select checked arithmetic and survive serialization.
        let (extensions, mut func, schema) = decimal_function(
            "add",
            DataType::Decimal128(38, 0),
            DataType::Decimal128(38, 0),
            Some(DataType::Decimal128(38, 0)),
        )?;
        func.options = vec![FunctionOption {
            name: "overflow".into(),
            preference: vec!["ERROR".into()],
        }];
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let expr = consumer.consume_scalar_function(&func, &schema).await?;
        assert!(matches!(expr, Expr::ScalarFunction(_)));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(
                    Decimal128Array::from(vec![9 * 10_i128.pow(37)])
                        .with_precision_and_scale(38, 0)?,
                ) as ArrayRef,
            ),
            (
                "b",
                Arc::new(
                    Decimal128Array::from(vec![9 * 10_i128.pow(37)])
                        .with_precision_and_scale(38, 0)?,
                ),
            ),
        ])?;
        ctx.register_batch("decimals", batch)?;
        let df = ctx
            .table("decimals")
            .await?
            .select(vec![expr.alias("result")])?;
        let proto = to_substrait_plan(df.logical_plan(), &state)?;
        let reimported = from_substrait_plan(&state, &proto).await?;
        for df in [df, ctx.execute_logical_plan(reimported).await?] {
            let err = df.collect().await.unwrap_err();
            assert!(err.to_string().contains("Decimal overflow"), "{err}");
        }
        Ok(())
    }

    /// Test that large argument lists for binary operations do not crash the consumer
    #[tokio::test]
    async fn test_binary_op_large_argument_list() -> Result<()> {
        // Build substrait extensions (we are using only one function)
        let mut extensions = Extensions::default();
        extensions.functions.insert(0, String::from("or:bool_bool"));
        // Build substrait consumer
        let consumer = DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);

        // Build arguments for the function call, this is basically an OR(true, true, ..., true)
        let arg = FunctionArgument {
            arg_type: Some(ArgType::Value(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: false,
                    type_variation_reference: 0,
                    literal_type: Some(LiteralType::Boolean(true)),
                })),
            })),
        };
        let arguments = vec![arg; 50000];
        let func = ScalarFunction {
            function_reference: 0,
            arguments,
            ..Default::default()
        };
        // Trivial input schema
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let df_schema = DFSchema::try_from(schema).unwrap();

        // Consume the expression and ensure we don't crash
        let _ = consumer.consume_scalar_function(&func, &df_schema).await?;
        Ok(())
    }

    fn int64_literals(integers: &[i64]) -> Vec<Expr> {
        integers
            .iter()
            .map(|value| Expr::Literal(ScalarValue::Int64(Some(*value)), None))
            .collect()
    }

    #[test]
    fn arg_list_to_binary_op_tree_1_arg() -> Result<()> {
        let expr = arg_list_to_binary_op_tree(Operator::Or, int64_literals(&[1]))?;
        assert_snapshot!(expr.to_string(), @"Int64(1)");
        Ok(())
    }

    #[test]
    fn arg_list_to_binary_op_tree_2_args() -> Result<()> {
        let expr = arg_list_to_binary_op_tree(Operator::Or, int64_literals(&[1, 2]))?;
        assert_snapshot!(expr.to_string(), @"Int64(1) OR Int64(2)");
        Ok(())
    }

    #[test]
    fn arg_list_to_binary_op_tree_3_args() -> Result<()> {
        let expr = arg_list_to_binary_op_tree(Operator::Or, int64_literals(&[1, 2, 3]))?;
        assert_snapshot!(expr.to_string(), @"Int64(1) OR Int64(2) OR Int64(3)");
        Ok(())
    }

    #[test]
    fn arg_list_to_binary_op_tree_4_args() -> Result<()> {
        let expr =
            arg_list_to_binary_op_tree(Operator::Or, int64_literals(&[1, 2, 3, 4]))?;
        assert_snapshot!(expr.to_string(), @"Int64(1) OR Int64(2) OR Int64(3) OR Int64(4)");
        Ok(())
    }

    //Test that DataFusion can consume scalar functions that have a different name in Substrait
    #[tokio::test]
    async fn test_substrait_to_df_name_mapping() -> Result<()> {
        // Build substrait extensions (we are using only one function)
        let mut extensions = Extensions::default();
        //is_nan is one of the functions that has a different name in Substrait (mapping is in substrait_to_df_name())
        extensions.functions.insert(0, String::from("is_nan:fp32"));
        // Build substrait consumer
        let consumer = DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);

        // Build arguments for the function call
        let arg = FunctionArgument {
            arg_type: Some(ArgType::Value(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: false,
                    type_variation_reference: 0,
                    literal_type: Some(LiteralType::Fp32(1.0)),
                })),
            })),
        };
        let arguments = vec![arg];
        let func = ScalarFunction {
            function_reference: 0,
            arguments,
            ..Default::default()
        };
        // Trivial input schema
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let df_schema = DFSchema::try_from(schema).unwrap();

        // Consume the expression and ensure we don't get an error
        let _ = consumer.consume_scalar_function(&func, &df_schema).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_like_match_conversion() -> Result<()> {
        // 1. Setup the consumer with the "like_match" function registered
        let mut extensions = Extensions::default();
        extensions
            .functions
            .insert(0, "like_match:str_str".to_string());
        extensions
            .functions
            .insert(1, "like_not_match:str_str".to_string());
        extensions
            .functions
            .insert(2, "like_imatch:str_str".to_string());

        let consumer = DefaultSubstraitConsumer::new(&extensions, &TEST_SESSION_STATE);

        // 2. Create the arguments (column "a" and pattern "%foo%")
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from(schema).unwrap();

        let col_arg = FunctionArgument {
            arg_type: Some(ArgType::Value(Expression {
                rex_type: Some(RexType::Selection(Box::new(
                    substrait::proto::expression::FieldReference {
                        reference_type: Some(substrait::proto::expression::field_reference::ReferenceType::DirectReference(
                            substrait::proto::expression::ReferenceSegment {
                                reference_type: Some(substrait::proto::expression::reference_segment::ReferenceType::StructField(
                                    Box::new(substrait::proto::expression::reference_segment::StructField {
                                        field: 0,
                                        child: None,
                                    })
                                )),
                            }
                        )),
                        root_type: Some(substrait::proto::expression::field_reference::RootType::RootReference(
                            substrait::proto::expression::field_reference::RootReference {}
                        )),
                    }
                ))),
            })),
        };

        let pattern_arg = FunctionArgument {
            arg_type: Some(ArgType::Value(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: false,
                    type_variation_reference: 0,
                    literal_type: Some(LiteralType::String("foo".to_string())),
                })),
            })),
        };

        // 3. Test "like_match" (Standard LIKE)
        let func_like = ScalarFunction {
            function_reference: 0,
            arguments: vec![col_arg.clone(), pattern_arg.clone()],
            ..Default::default()
        };

        let result = consumer
            .consume_scalar_function(&func_like, &df_schema)
            .await?;

        if let Expr::Like(like) = result {
            assert!(!like.negated);
            assert!(!like.case_insensitive);
            assert_eq!(format!("{}", like.pattern), "Utf8(\"foo\")");
        } else {
            panic!("Expected Expr::Like, got {result:?}");
        }

        // 4. Test "like_not_match" (NOT LIKE)
        let func_not_like = ScalarFunction {
            function_reference: 1,
            arguments: vec![col_arg.clone(), pattern_arg.clone()],
            ..Default::default()
        };

        let result = consumer
            .consume_scalar_function(&func_not_like, &df_schema)
            .await?;

        if let Expr::Like(like) = result {
            assert!(like.negated);
            assert!(!like.case_insensitive);
        } else {
            panic!("Expected Expr::Like (negated), got {result:?}");
        }

        Ok(())
    }
}

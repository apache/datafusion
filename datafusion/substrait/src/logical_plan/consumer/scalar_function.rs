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

use super::{from_substrait_func_args, SubstraitConsumer};
use datafusion::common::{not_impl_err, plan_err, substrait_err, DFSchema, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{expr, BinaryExpr, Expr, Like, Operator};
use substrait::proto::expression::ScalarFunction;
use substrait::proto::function_argument::ArgType;

pub async fn from_scalar_function(
    consumer: &impl SubstraitConsumer,
    f: &ScalarFunction,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
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

    // try to first match the requested function into registered udfs, then built-in ops
    // and finally built-in expressions
    if let Ok(func) = consumer.get_function_registry().udf(fn_name) {
        Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
            func.to_owned(),
            args,
        )))
    } else if let Some(op) = name_to_op(fn_name) {
        if f.arguments.len() < 2 {
            return not_impl_err!(
                        "Expect at least two arguments for binary operator {op:?}, the provided number of operators is {:?}",
                       f.arguments.len()
                    );
        }
        // Some expressions are binary in DataFusion but take in a variadic number of args in Substrait.
        // In those cases we iterate through all the arguments, applying the binary expression against them all
        let combined_expr = args
            .into_iter()
            .fold(None, |combined_expr: Option<Expr>, arg: Expr| {
                Some(match combined_expr {
                    Some(expr) => Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(expr),
                        op,
                        right: Box::new(arg),
                    }),
                    None => arg,
                })
            })
            .unwrap();

        Ok(combined_expr)
    } else if let Some(builder) = BuiltinExprBuilder::try_from_name(fn_name) {
        builder.build(consumer, f, input_schema).await
    } else {
        not_impl_err!("Unsupported function name: {fn_name:?}")
    }
}

pub fn substrait_fun_name(name: &str) -> &str {
    let name = match name.rsplit_once(':') {
        // Since 0.32.0, Substrait requires the function names to be in a compound format
        // https://substrait.io/extensions/#function-signature-compound-names
        // for example, `add:i8_i8`.
        // On the consumer side, we don't really care about the signature though, just the name.
        Some((name, _)) => name,
        None => name,
    };
    name
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

/// Build [`Expr`] from its name and required inputs.
struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" | "negate" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub async fn build(
        self,
        consumer: &impl SubstraitConsumer,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        match self.expr_name.as_str() {
            "like" => Self::build_like_expr(consumer, false, f, input_schema).await,
            "ilike" => Self::build_like_expr(consumer, true, f, input_schema).await,
            "not" | "negative" | "negate" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" => {
                Self::build_unary_expr(consumer, &self.expr_name, f, input_schema).await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(
        consumer: &impl SubstraitConsumer,
        fn_name: &str,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        if f.arguments.len() != 1 {
            return substrait_err!("Expect one argument for {fn_name} expr");
        }
        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for {fn_name} expr");
        };
        let arg = consumer
            .consume_expression(expr_substrait, input_schema)
            .await?;
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

    async fn build_like_expr(
        consumer: &impl SubstraitConsumer,
        case_insensitive: bool,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if f.arguments.len() != 2 && f.arguments.len() != 3 {
            return substrait_err!("Expect two or three arguments for `{fn_name}` expr");
        }

        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let expr = consumer
            .consume_expression(expr_substrait, input_schema)
            .await?;
        let Some(ArgType::Value(pattern_substrait)) = &f.arguments[1].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let pattern = consumer
            .consume_expression(pattern_substrait, input_schema)
            .await?;

        // Default case: escape character is Literal(Utf8(None))
        let escape_char = if f.arguments.len() == 3 {
            let Some(ArgType::Value(escape_char_substrait)) = &f.arguments[2].arg_type
            else {
                return substrait_err!("Invalid arguments type for `{fn_name}` expr");
            };

            let escape_char_expr = consumer
                .consume_expression(escape_char_substrait, input_schema)
                .await?;

            match escape_char_expr {
                Expr::Literal(ScalarValue::Utf8(escape_char_string)) => {
                    // Convert Option<String> to Option<char>
                    escape_char_string.and_then(|s| s.chars().next())
                }
                _ => {
                    return substrait_err!(
                    "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
                )
                }
            }
        } else {
            None
        };

        Ok(Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char,
            case_insensitive,
        }))
    }
}

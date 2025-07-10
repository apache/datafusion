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

use crate::logical_plan::consumer::{from_substrait_func_args, SubstraitConsumer};
use datafusion::common::Result;
use datafusion::common::{
    not_impl_err, plan_err, substrait_err, DFSchema, DataFusionError, ScalarValue,
};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{expr, BinaryExpr, Expr, Like, Operator};
use std::vec::Drain;
use substrait::proto::expression::ScalarFunction;
use substrait::proto::function_argument::ArgType;

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

    // try to first match the requested function into registered udfs, then built-in ops
    // and finally built-in expressions
    if let Ok(func) = consumer.get_function_registry().udf(fn_name) {
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
        arg_list_to_binary_op_tree(op, args)
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
    ) -> Result<Expr> {
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
    ) -> Result<Expr> {
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
    ) -> Result<Expr> {
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
                Expr::Literal(ScalarValue::Utf8(escape_char_string), _) => {
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

#[cfg(test)]
mod tests {
    use super::arg_list_to_binary_op_tree;
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::tests::TEST_SESSION_STATE;
    use crate::logical_plan::consumer::{DefaultSubstraitConsumer, SubstraitConsumer};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{DFSchema, Result, ScalarValue};
    use datafusion::logical_expr::{Expr, Operator};
    use insta::assert_snapshot;
    use substrait::proto::expression::literal::LiteralType;
    use substrait::proto::expression::{Literal, RexType, ScalarFunction};
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::{Expression, FunctionArgument};

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
}

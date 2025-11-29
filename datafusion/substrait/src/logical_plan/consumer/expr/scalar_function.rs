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
use datafusion::logical_expr::{expr, Between, BinaryExpr, Expr, Like, Operator};
use std::vec::Drain;
use substrait::proto::expression::ScalarFunction;

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

    let udf_func = consumer.get_function_registry().udf(fn_name).or_else(|e| {
        if let Some(alt_name) = substrait_to_df_name(fn_name) {
            consumer.get_function_registry().udf(alt_name).or(Err(e))
        } else {
            Err(e)
        }
    });

    // try to first match the requested function into registered udfs, then built-in ops
    // and finally built-in expressions
    if let Ok(func) = udf_func {
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
        builder.build(consumer, f, args).await
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

    pub async fn build(
        self,
        consumer: &impl SubstraitConsumer,
        f: &ScalarFunction,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        match self.expr_name.as_str() {
            "like" => Self::build_like_expr(false, false, f, args).await,
            "ilike" => Self::build_like_expr(true, false, f, args).await,
            "like_match" => Self::build_like_expr(false, false, f, args).await,
            "like_imatch" => Self::build_like_expr(true, false, f, args).await,
            "like_not_match" => Self::build_like_expr(false, true, f, args).await,
            "like_not_imatch" => Self::build_like_expr(true, true, f, args).await,
            "not" | "negative" | "negate" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" => Self::build_unary_expr(&self.expr_name, args).await,
            "and_not" | "xor" => Self::build_binary_expr(&self.expr_name, args).await,
            "between" => Self::build_between_expr(&self.expr_name, args).await,
            "logb" => {
                Self::build_custom_handling_expr(consumer, &self.expr_name, args).await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let [arg] = match args.try_into() {
            Ok(args_arr) => args_arr,
            Err(_) => return substrait_err!("Expected one argument for {fn_name} expr"),
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

    async fn build_like_expr(
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
                )
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

    async fn build_binary_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let [a, b] = match args.try_into() {
            Ok(args_arr) => args_arr,
            Err(_) => {
                return substrait_err!("Expected two arguments for `{fn_name}` expr")
            }
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

    async fn build_between_expr(fn_name: &str, args: Vec<Expr>) -> Result<Expr> {
        let [expression, low, high] = match args.try_into() {
            Ok(args_arr) => args_arr,
            Err(_) => {
                return substrait_err!("Expected three arguments for `{fn_name}` expr")
            }
        };

        Ok(Expr::Between(Between {
            expr: Box::new(expression),
            negated: false,
            low: Box::new(low),
            high: Box::new(high),
        }))
    }

    //This handles any functions that require custom handling
    async fn build_custom_handling_expr(
        consumer: &impl SubstraitConsumer,
        fn_name: &str,
        args: Vec<Expr>,
    ) -> Result<Expr> {
        match fn_name {
            "logb" => Self::build_logb_expr(consumer, args).await,
            _ => not_impl_err!("Unsupported custom handled expression: {}", fn_name),
        }
    }

    async fn build_logb_expr(
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

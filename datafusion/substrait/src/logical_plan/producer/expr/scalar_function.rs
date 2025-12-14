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

use crate::logical_plan::producer::{to_substrait_literal_expr, SubstraitProducer};
use datafusion::common::{not_impl_err, DFSchemaRef, ScalarValue};
use datafusion::logical_expr::{expr, Between, BinaryExpr, Expr, Like, Operator};
use substrait::proto::expression::{RexType, ScalarFunction};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument};

pub fn from_scalar_function(
    producer: &mut impl SubstraitProducer,
    fun: &expr::ScalarFunction,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in &fun.args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }

    let arguments = custom_argument_handler(fun.name(), arguments);

    let function_anchor = producer.register_function(fun.name().to_string());
    #[expect(deprecated)]
    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            options: vec![],
            args: vec![],
        })),
    })
}

// Handle functions that require custom handling for their arguments (e.g. log)
pub fn custom_argument_handler(
    name: &str,
    args: Vec<FunctionArgument>,
) -> Vec<FunctionArgument> {
    match name {
        "log" => {
            if args.len() == 2 {
                let mut args = args;
                args.swap(0, 1);
                args
            } else {
                args
            }
        }
        _ => args,
    }
}

pub fn from_unary_expr(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let (fn_name, arg) = match expr {
        Expr::Not(arg) => ("not", arg),
        Expr::IsNull(arg) => ("is_null", arg),
        Expr::IsNotNull(arg) => ("is_not_null", arg),
        Expr::IsTrue(arg) => ("is_true", arg),
        Expr::IsFalse(arg) => ("is_false", arg),
        Expr::IsUnknown(arg) => ("is_unknown", arg),
        Expr::IsNotTrue(arg) => ("is_not_true", arg),
        Expr::IsNotFalse(arg) => ("is_not_false", arg),
        Expr::IsNotUnknown(arg) => ("is_not_unknown", arg),
        Expr::Negative(arg) => ("negate", arg),
        expr => not_impl_err!("Unsupported expression: {expr:?}")?,
    };
    to_substrait_unary_scalar_fn(producer, fn_name, arg, schema)
}

pub fn from_binary_expr(
    producer: &mut impl SubstraitProducer,
    expr: &BinaryExpr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let BinaryExpr { left, op, right } = expr;
    let l = producer.handle_expr(left, schema)?;
    let r = producer.handle_expr(right, schema)?;
    Ok(make_binary_op_scalar_func(producer, &l, &r, *op))
}

pub fn from_like(
    producer: &mut impl SubstraitProducer,
    like: &Like,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    } = like;
    make_substrait_like_expr(
        producer,
        *case_insensitive,
        *negated,
        expr,
        pattern,
        *escape_char,
        schema,
    )
}

fn make_substrait_like_expr(
    producer: &mut impl SubstraitProducer,
    ignore_case: bool,
    negated: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<char>,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let function_anchor = if ignore_case {
        producer.register_function("ilike".to_string())
    } else {
        producer.register_function("like".to_string())
    };
    let expr = producer.handle_expr(expr, schema)?;
    let pattern = producer.handle_expr(pattern, schema)?;
    let escape_char = to_substrait_literal_expr(
        producer,
        &ScalarValue::Utf8(escape_char.map(|c| c.to_string())),
    )?;
    let arguments = vec![
        FunctionArgument {
            arg_type: Some(ArgType::Value(expr)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(pattern)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(escape_char)),
        },
    ];

    #[expect(deprecated)]
    let substrait_like = Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    };

    if negated {
        let function_anchor = producer.register_function("not".to_string());

        #[expect(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_like)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_like)
    }
}

/// Util to generate substrait [RexType::ScalarFunction] with one argument
fn to_substrait_unary_scalar_fn(
    producer: &mut impl SubstraitProducer,
    fn_name: &str,
    arg: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let function_anchor = producer.register_function(fn_name.to_string());
    let substrait_expr = producer.handle_expr(arg, schema)?;

    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(substrait_expr)),
            }],
            output_type: None,
            options: vec![],
            ..Default::default()
        })),
    })
}

/// Return Substrait scalar function with two arguments
pub fn make_binary_op_scalar_func(
    producer: &mut impl SubstraitProducer,
    lhs: &Expression,
    rhs: &Expression,
    op: Operator,
) -> Expression {
    let function_anchor = producer.register_function(operator_to_name(op).to_string());
    #[expect(deprecated)]
    Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                FunctionArgument {
                    arg_type: Some(ArgType::Value(lhs.clone())),
                },
                FunctionArgument {
                    arg_type: Some(ArgType::Value(rhs.clone())),
                },
            ],
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    }
}

pub fn from_between(
    producer: &mut impl SubstraitProducer,
    between: &Between,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    if *negated {
        // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_low,
            Operator::Lt,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_high,
            &substrait_expr,
            Operator::Lt,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::Or,
        ))
    } else {
        // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_low,
            &substrait_expr,
            Operator::LtEq,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_high,
            Operator::LtEq,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::And,
        ))
    }
}

pub fn operator_to_name(op: Operator) -> &'static str {
    match op {
        Operator::Eq => "equal",
        Operator::NotEq => "not_equal",
        Operator::Lt => "lt",
        Operator::LtEq => "lte",
        Operator::Gt => "gt",
        Operator::GtEq => "gte",
        Operator::Plus => "add",
        Operator::Minus => "subtract",
        Operator::Multiply => "multiply",
        Operator::Divide => "divide",
        Operator::Modulo => "modulus",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::IsDistinctFrom => "is_distinct_from",
        Operator::IsNotDistinctFrom => "is_not_distinct_from",
        Operator::RegexMatch => "regex_match",
        Operator::RegexIMatch => "regex_imatch",
        Operator::RegexNotMatch => "regex_not_match",
        Operator::RegexNotIMatch => "regex_not_imatch",
        Operator::LikeMatch => "like_match",
        Operator::ILikeMatch => "like_imatch",
        Operator::NotLikeMatch => "like_not_match",
        Operator::NotILikeMatch => "like_not_imatch",
        Operator::BitwiseAnd => "bitwise_and",
        Operator::BitwiseOr => "bitwise_or",
        Operator::StringConcat => "str_concat",
        Operator::AtArrow => "at_arrow",
        Operator::ArrowAt => "arrow_at",
        Operator::Arrow => "arrow",
        Operator::LongArrow => "long_arrow",
        Operator::HashArrow => "hash_arrow",
        Operator::HashLongArrow => "hash_long_arrow",
        Operator::AtAt => "at_at",
        Operator::IntegerDivide => "integer_divide",
        Operator::HashMinus => "hash_minus",
        Operator::AtQuestion => "at_question",
        Operator::Question => "question",
        Operator::QuestionAnd => "question_and",
        Operator::QuestionPipe => "question_pipe",
        Operator::BitwiseXor => "bitwise_xor",
        Operator::BitwiseShiftRight => "bitwise_shift_right",
        Operator::BitwiseShiftLeft => "bitwise_shift_left",
    }
}

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

//! Declaration of built-in (higher order) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for array_transform, ([[f32]], v -> v*2) -> [f32], ([[f32]], v -> v > 3.0) -> [bool].
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::expressions::{LambdaExpr, Literal};

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::datatype::FieldExt;
use datafusion_common::utils::remove_list_null_values;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, internal_datafusion_err,
    internal_err, plan_err,
};
use datafusion_expr::type_coercion::functions::value_fields_with_higher_order_udf;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderUDF,
    LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility, expr_vec_fmt,
};

/// Physical expression of a higher order function
pub struct HigherOrderFunctionExpr {
    /// A shared instance of the higher-order function
    fun: Arc<dyn HigherOrderUDF>,
    /// The name of the higher-order function
    name: String,
    /// List of expressions to feed to the function as arguments
    ///
    /// For example, for `array_transform([2, 3], v -> v != 2)`, this will be:
    ///
    /// ```text
    /// ListExpression [2,3]
    /// LambdaExpression
    ///     parameters: ["v"]
    ///     body:
    ///         BinaryExpression (!=)
    ///             left:
    ///                 LambdaVariableExpression("v", Field::new("", Int32, false))
    ///             right:
    ///                 LiteralExpression(2)
    /// ```
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Positions in `args` where lambdas were top level arguments during try_new_with_schema
    /// but may have been wrapped by tree node rewrites via with_new_children, like:
    ///
    /// ```ignore
    /// expr.transform(|expr| Ok(Transformed::yes(Arc::new(DebugExpr::new(expr)))))
    /// ```
    ///
    /// For example, for `array_transform([2, 3], v -> v != 2)`, this will be `vec![1]`
    lambda_positions: Vec<usize>,
    /// The output field associated this expression
    ///
    /// For example, for `array_transform([2, 3], v -> v != 2)`, this will be
    /// `Field::new("", DataType::new_list(DataType::Boolean, true), true)`
    return_field: FieldRef,
    /// The config options at execution time
    config_options: Arc<ConfigOptions>,
}

impl Debug for HigherOrderFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("HigherOrderFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("lambda_positions", &self.lambda_positions)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl HigherOrderFunctionExpr {
    /// Create a new Higher Order function
    ///
    /// Note that lambda arguments must be present directly in args as [LambdaExpr],
    /// and not as a wrapped child of any arg
    pub fn try_new_with_schema(
        fun: Arc<dyn HigherOrderUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let name = fun.name().to_string();
        let mut lambda_positions = vec![];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, e)| match e.downcast_ref::<LambdaExpr>() {
                Some(lambda) => {
                    lambda_positions.push(i);

                    Ok(ValueOrLambda::Lambda(lambda.body().return_field(schema)?))
                }
                None => Ok(ValueOrLambda::Value(e.return_field(schema)?)),
            })
            .collect::<Result<Vec<_>>>()?;

        // verify that input data types is consistent with function's `HigherOrderTypeSignature`
        value_fields_with_higher_order_udf(&arg_fields, fun.as_ref())?;

        let arguments = args
            .iter()
            .map(|e| e.downcast_ref::<Literal>().map(|literal| literal.value()))
            .collect::<Vec<_>>();

        let ret_args = HigherOrderReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &arguments,
        };

        let return_field = fun.return_field_from_args(ret_args)?;

        Ok(Self {
            fun,
            name,
            args,
            lambda_positions,
            return_field,
            config_options,
        })
    }

    /// Get the higher order function implementation
    pub fn fun(&self) -> &dyn HigherOrderUDF {
        self.fun.as_ref()
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    pub fn nullable(&self) -> bool {
        self.return_field.is_nullable()
    }

    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    pub fn lambda_positions(&self) -> &[usize] {
        &self.lambda_positions
    }
}

impl fmt::Display for HigherOrderFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PartialEq for HigherOrderFunctionExpr {
    fn eq(&self, o: &Self) -> bool {
        if std::ptr::eq(self, o) {
            // The equality implementation is somewhat expensive, so let's short-circuit when possible.
            return true;
        }
        let Self {
            fun,
            name,
            args,
            lambda_positions,
            return_field,
            config_options,
        } = self;
        fun.eq(&o.fun)
            && name.eq(&o.name)
            && args.eq(&o.args)
            && lambda_positions.eq(&o.lambda_positions)
            && return_field.eq(&o.return_field)
            && (Arc::ptr_eq(config_options, &o.config_options)
                || sorted_config_entries(config_options)
                    == sorted_config_entries(&o.config_options))
    }
}
impl Eq for HigherOrderFunctionExpr {}
impl Hash for HigherOrderFunctionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            fun,
            name,
            args,
            lambda_positions,
            return_field,
            config_options: _, // expensive to hash, and often equal
        } = self;
        fun.hash(state);
        name.hash(state);
        args.hash(state);
        lambda_positions.hash(state);
        return_field.hash(state);
    }
}

fn sorted_config_entries(config_options: &ConfigOptions) -> Vec<ConfigEntry> {
    let mut entries = config_options.entries();
    entries.sort_by(|l, r| l.key.cmp(&r.key));
    entries
}

impl PhysicalExpr for HigherOrderFunctionExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg_fields = self
            .args
            .iter()
            .enumerate()
            .map(|(i, e)| {
                if self.lambda_positions.contains(&i) {
                    let lambda = wrapped_lambda(e).ok_or_else(|| {
                        exec_datafusion_err!(
                            "{} unable to unwrap lambda from {e} at position {i}",
                            self.name()
                        )
                    })?;

                    Ok(ValueOrLambda::Lambda(
                        lambda.body().return_field(batch.schema_ref())?,
                    ))
                } else {
                    Ok(ValueOrLambda::Value(e.return_field(batch.schema_ref())?))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let fields = arg_fields
            .iter()
            .map(|field| match field {
                ValueOrLambda::Lambda(field) => {
                    ValueOrLambda::Lambda(Some(Arc::clone(field)))
                }
                ValueOrLambda::Value(field) => ValueOrLambda::Value(Arc::clone(field)),
            })
            .collect::<Vec<_>>();

        // lambda_parameters refers only to lambdas and not to values, so instead
        // of zipping it with self.args, we iterate over self.args and only
        // consume from lambda_parameters when a given argument is a lambda
        // to reconstruct the arguments list with the correct order
        // this supports any value and lambda positioning including
        // multiple lambdas interleaved with values
        let mut lambda_parameters = match self.fun().lambda_parameters(0, &fields)? {
            LambdaParametersProgress::Partial(_) => {
                return plan_err!(
                    "{} lambda_parameters returned a partial result when the return type of all it's lambdas were provided",
                    self.name()
                );
            }
            LambdaParametersProgress::Complete(items) => items.into_iter(),
        };

        let num_lambdas = self.args.len() - fields.len();

        // functions can support multiple lambdas where some trailing ones are optional,
        // but to simplify the implementor, lambda_parameters returns the parameters of all of them,
        // so we can't do equality check. one example is spark reduce:
        // https://spark.apache.org/docs/latest/api/sql/index.html#reduce
        if lambda_parameters.len() < num_lambdas {
            return exec_err!(
                "{} invocation defined {num_lambdas} but lambda_parameters returned only {}",
                self.name(),
                lambda_parameters.len()
            );
        }

        let args = self
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                if self.lambda_positions.contains(&i) {
                    let lambda = wrapped_lambda(arg).ok_or_else(|| {
                        exec_datafusion_err!(
                            "{} unable to unwrap lambda from {arg} at position {i}",
                            self.name()
                        )
                    })?;

                    let lambda_params = lambda_parameters.next().ok_or_else(|| {
                        internal_datafusion_err!(
                            "params len should have been checked above"
                        )
                    })?;

                    if lambda.params().len() > lambda_params.len() {
                        return exec_err!(
                            "lambda defined {} params but higher-order function support only {}",
                            lambda.params().len(),
                            lambda_params.len()
                        );
                    }

                    let params = std::iter::zip(lambda.params(), lambda_params)
                        .map(|(name, param)| param.renamed(name.as_str()))
                        .collect();

                    Ok(ValueOrLambda::Lambda(LambdaArgument::new(
                        params,
                        Arc::clone(lambda.body()),
                    )))
                } else {
                    let value = arg.evaluate(batch)?;

                    let value =
                        if self.fun.clear_null_values() && matches!(value.data_type(), DataType::List(_) | DataType::LargeList(_)) {
                            ColumnarValue::Array(remove_list_null_values(
                                &value.into_array(batch.num_rows())?,
                            )?)
                        } else {
                            value
                        };

                    Ok(ValueOrLambda::Value(value))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ValueOrLambda::Value(ColumnarValue::Scalar(_))));

        // evaluate the function
        let output = self.fun.invoke_with_args(HigherOrderFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

        if let ColumnarValue::Array(array) = &output
            && array.len() != batch.num_rows()
        {
            // If the arguments are a non-empty slice of scalar values, we can assume that
            // returning a one-element array is equivalent to returning a scalar.
            let preserve_scalar = array.len() == 1 && !input_empty && input_all_scalar;
            return if preserve_scalar {
                ScalarValue::try_from_array(array, 0).map(ColumnarValue::Scalar)
            } else {
                internal_err!(
                    "higher-order function {} returned a different number of rows than expected. Expected: {}, Got: {}",
                    self.name,
                    batch.num_rows(),
                    array.len()
                )
            };
        }
        Ok(output)
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.args.len() {
            return internal_err!(
                "HigherOrderFunctionExpr expects exactly {} child, got {}",
                self.args.len(),
                children.len()
            );
        }

        for (i, child) in children.iter().enumerate() {
            if self.lambda_positions.contains(&i) {
                if wrapped_lambda(child).is_none() {
                    return plan_err!(
                        "{} unable to unwrap lambda from {} at position {i}",
                        &children[i],
                        self.name()
                    );
                }
            } else if child.is::<LambdaExpr>() {
                return plan_err!(
                    "{} received a lambda via with_new_children at position {i} that wasn't a lambda before",
                    self.name()
                );
            }
        }

        Ok(Arc::new(HigherOrderFunctionExpr {
            name: self.name.clone(),
            fun: Arc::clone(&self.fun),
            args: children,
            lambda_positions: self.lambda_positions.clone(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, expr) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, ")")
    }

    fn is_volatile_node(&self) -> bool {
        self.fun.signature().volatility == Volatility::Volatile
    }
}

fn wrapped_lambda(expr: &Arc<dyn PhysicalExpr>) -> Option<&LambdaExpr> {
    let mut current = expr;

    loop {
        if let Some(lambda) = current.downcast_ref::<LambdaExpr>() {
            return Some(lambda);
        } else if current.is::<HigherOrderFunctionExpr>() {
            return None;
        }

        match current.children().as_slice() {
            [single_child] => current = *single_child,
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::HigherOrderFunctionExpr;
    use crate::expressions::Column;
    use crate::expressions::NoOp;
    use crate::expressions::lambda;
    use crate::expressions::not;
    use arrow::array::NullArray;
    use arrow::array::RecordBatchOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_common::assert_contains;
    use datafusion_expr::{
        HigherOrderFunctionArgs, HigherOrderSignature, HigherOrderUDF,
    };
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::physical_expr::is_volatile;

    /// Test helper to create a mock UDF with a specific volatility
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockHigherOrderUDF {
        signature: HigherOrderSignature,
    }

    impl HigherOrderUDF for MockHigherOrderUDF {
        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &HigherOrderSignature {
            &self.signature
        }

        fn lambda_parameters(
            &self,
            _step: usize,
            _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
        ) -> Result<LambdaParametersProgress> {
            Ok(LambdaParametersProgress::Complete(vec![vec![Arc::new(
                Field::new("", DataType::Null, true),
            )]]))
        }

        fn return_field_from_args(
            &self,
            args: HigherOrderReturnFieldArgs,
        ) -> Result<FieldRef> {
            match &args.arg_fields[0] {
                ValueOrLambda::Lambda(field) | ValueOrLambda::Value(field) => {
                    Ok(Arc::clone(field))
                }
            }
        }

        fn invoke_with_args(
            &self,
            args: HigherOrderFunctionArgs,
        ) -> Result<ColumnarValue> {
            match &args.args[0] {
                ValueOrLambda::Lambda(lambda) => {
                    lambda.evaluate(&[&|| Ok(Arc::new(NullArray::new(args.number_rows)))])
                }
                ValueOrLambda::Value(value) => Ok(value.clone()),
            }
        }
    }

    #[test]
    fn test_higher_order_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(MockHigherOrderUDF {
            signature: HigherOrderSignature::variadic_any(Volatility::Volatile),
        });

        // Create a non-volatile UDF
        let stable_udf = Arc::new(MockHigherOrderUDF {
            signature: HigherOrderSignature::variadic_any(Volatility::Stable),
        });

        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let config_options = Arc::new(ConfigOptions::new());

        // Test volatile function
        let volatile_expr = HigherOrderFunctionExpr::try_new_with_schema(
            volatile_udf,
            args.clone(),
            &schema,
            Arc::clone(&config_options),
        )
        .unwrap();

        assert!(volatile_expr.is_volatile_node());
        let volatile_arc: Arc<dyn PhysicalExpr> = Arc::new(volatile_expr);
        assert!(is_volatile(&volatile_arc));

        // Test non-volatile function
        let stable_expr = HigherOrderFunctionExpr::try_new_with_schema(
            stable_udf,
            args,
            &schema,
            config_options,
        )
        .unwrap();

        assert!(!stable_expr.is_volatile_node());
        let stable_arc: Arc<dyn PhysicalExpr> = Arc::new(stable_expr);
        assert!(!is_volatile(&stable_arc));
    }

    #[test]
    fn test_higher_order_function_wrapped_lambda() {
        let fun = Arc::new(MockHigherOrderUDF {
            signature: HigherOrderSignature::variadic_any(Volatility::Stable),
        });

        let expected = ScalarValue::Int32(Some(42));

        let hof = HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            vec![lambda(["a"], Arc::new(Literal::new(expected.clone()))).unwrap()],
            &Schema::empty(),
            Arc::new(ConfigOptions::new()),
        )
        .unwrap();

        let new_children = vec![not(Arc::clone(&hof.args[0])).unwrap()];
        let wrapped = Arc::new(hof).with_new_children(new_children).unwrap();

        let result = wrapped
            .evaluate(
                &RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(0)),
                )
                .unwrap(),
            )
            .unwrap();

        let ColumnarValue::Scalar(result) = result else {
            unreachable!()
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_higher_order_function_badly_wrapped_lambda() {
        let fun = Arc::new(MockHigherOrderUDF {
            signature: HigherOrderSignature::variadic_any(Volatility::Stable),
        });

        let hof = HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            vec![
                not(
                    lambda(["a"], Arc::new(Literal::new(ScalarValue::Int32(Some(42)))))
                        .unwrap(),
                )
                .unwrap(),
            ],
            &Schema::empty(),
            Arc::new(ConfigOptions::new()),
        )
        .unwrap();

        let result = hof
            .evaluate(
                &RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(0)),
                )
                .unwrap(),
            )
            .unwrap_err();

        assert_contains!(
            result.to_string(),
            "LambdaExpr::evaluate() should not be called"
        );
    }

    #[test]
    fn test_higher_order_function_unexpected_lambda() {
        let fun = Arc::new(MockHigherOrderUDF {
            signature: HigherOrderSignature::variadic_any(Volatility::Stable),
        });

        let hof = HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            vec![Arc::new(NoOp::new())],
            &Schema::empty(),
            Arc::new(ConfigOptions::new()),
        )
        .unwrap();

        let result = Arc::new(hof)
            .with_new_children(vec![lambda(["a"], Arc::new(NoOp::new())).unwrap()])
            .unwrap_err();

        assert_contains!(
            result.to_string(),
            "mock_function received a lambda via with_new_children at position 0 that wasn't a lambda before"
        );
    }
}

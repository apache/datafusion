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
    Result, ScalarValue, exec_err, internal_datafusion_err, internal_err,
    plan_datafusion_err, plan_err,
};
use datafusion_expr::type_coercion::functions::value_fields_with_higher_order_udf;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderUDF,
    LambdaArgument, LambdaParametersProgress, ValueOrLambda, Volatility, expr_vec_fmt,
};

/// Per-argument classification cached at construction time.
///
/// Walking the wrapped lambda tree and scanning a `Vec<usize>` of lambda
/// positions used to be done on every `evaluate` call. Both costs collapse
/// to a single up-front pass by storing the classification (and the resolved
/// inner [`LambdaExpr`]) here.
enum ArgSlot {
    /// A regular value-producing expression at this position.
    Value,
    /// A lambda position. Stores the inner [`LambdaExpr`] pre-extracted from
    /// any wrapper expressions that may have been introduced via
    /// [`PhysicalExpr::with_new_children`] tree rewrites.
    Lambda(Arc<LambdaExpr>),
}

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
    /// Per-arg classification, parallel to `args`. Length always equals
    /// `args.len()`. Lambda variants carry the resolved inner [`LambdaExpr`]
    /// so `evaluate` doesn't walk through wrapper nodes.
    slots: Vec<ArgSlot>,
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
        let lambda_positions: Vec<_> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| matches!(slot, ArgSlot::Lambda(_)).then_some(i))
            .collect();
        f.debug_struct("HigherOrderFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("lambda_positions", &lambda_positions)
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
        let mut slots = Vec::with_capacity(args.len());
        let arg_fields = args
            .iter()
            .map(|e| match e.downcast_ref::<LambdaExpr>() {
                Some(lambda) => {
                    slots.push(ArgSlot::Lambda(Arc::new(lambda.clone())));
                    Ok(ValueOrLambda::Lambda(lambda.body().return_field(schema)?))
                }
                None => {
                    slots.push(ArgSlot::Value);
                    Ok(ValueOrLambda::Value(e.return_field(schema)?))
                }
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
            slots,
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

    /// Resolve every lambda's parameter list. Returns an empty `Vec` when
    /// there are no lambdas, avoiding the [`HigherOrderUDF::lambda_parameters`]
    /// virtual call entirely.
    fn resolve_lambda_parameters(
        &self,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<Vec<Vec<FieldRef>>> {
        let num_lambdas = self
            .slots
            .iter()
            .filter(|s| matches!(s, ArgSlot::Lambda(_)))
            .count();
        if num_lambdas == 0 {
            return Ok(Vec::new());
        }
        match self.fun().lambda_parameters(0, fields)? {
            LambdaParametersProgress::Partial(_) => plan_err!(
                "{} lambda_parameters returned a partial result when the return type of all it's lambdas were provided",
                self.name()
            ),
            LambdaParametersProgress::Complete(items) => {
                // functions can support multiple lambdas where some trailing ones are optional,
                // but to simplify the implementor, lambda_parameters returns the parameters of all of them,
                // so we can't do equality check. one example is spark reduce:
                // https://spark.apache.org/docs/latest/api/sql/index.html#reduce
                if items.len() < num_lambdas {
                    return exec_err!(
                        "{} invocation defined {num_lambdas} but lambda_parameters returned only {}",
                        self.name(),
                        items.len()
                    );
                }
                Ok(items)
            }
        }
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
        // `slots` is a deterministic function of `fun` and `args`, so it's
        // not part of the comparison.
        let Self {
            fun,
            name,
            args,
            slots: _,
            return_field,
            config_options,
        } = self;
        fun.eq(&o.fun)
            && name.eq(&o.name)
            && args.eq(&o.args)
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
            slots: _,
            return_field,
            config_options: _, // expensive to hash, and often equal
        } = self;
        fun.hash(state);
        name.hash(state);
        args.hash(state);
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
        let mut arg_fields = Vec::with_capacity(self.args.len());
        let mut fields = Vec::with_capacity(self.args.len());
        for (arg, slot) in self.args.iter().zip(&self.slots) {
            match slot {
                ArgSlot::Lambda(lambda) => {
                    let field = lambda.body().return_field(batch.schema_ref())?;
                    arg_fields.push(ValueOrLambda::Lambda(Arc::clone(&field)));
                    fields.push(ValueOrLambda::Lambda(Some(field)));
                }
                ArgSlot::Value => {
                    let field = arg.return_field(batch.schema_ref())?;
                    arg_fields.push(ValueOrLambda::Value(Arc::clone(&field)));
                    fields.push(ValueOrLambda::Value(field));
                }
            }
        }

        let mut lambda_parameters = self.resolve_lambda_parameters(&fields)?.into_iter();

        let args = self
            .args
            .iter()
            .zip(&self.slots)
            .map(|(arg, slot)| match slot {
                ArgSlot::Lambda(lambda) => {
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
                }
                ArgSlot::Value => {
                    let value = arg.evaluate(batch)?;

                    let value = if self.fun.clear_null_values()
                        && matches!(
                            value.data_type(),
                            DataType::List(_) | DataType::LargeList(_)
                        )
                    {
                        let arr = value.into_array(batch.num_rows())?;
                        if arr.null_count() == 0 {
                            ColumnarValue::Array(arr)
                        } else {
                            ColumnarValue::Array(remove_list_null_values(&arr)?)
                        }
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

        // Re-derive `slots` for the new children using the original slot kinds
        // as the source of truth for which positions must (still) be lambdas.
        let mut new_slots = Vec::with_capacity(children.len());
        for (i, child) in children.iter().enumerate() {
            match &self.slots[i] {
                ArgSlot::Lambda(_) => {
                    let lambda = wrapped_lambda(child).ok_or_else(|| {
                        plan_datafusion_err!(
                            "{} unable to unwrap lambda from {} at position {i}",
                            &children[i],
                            self.name()
                        )
                    })?;
                    new_slots.push(ArgSlot::Lambda(Arc::new(lambda.clone())));
                }
                ArgSlot::Value => {
                    if child.is::<LambdaExpr>() {
                        return plan_err!(
                            "{} received a lambda via with_new_children at position {i} that wasn't a lambda before",
                            self.name()
                        );
                    }
                    new_slots.push(ArgSlot::Value);
                }
            }
        }

        Ok(Arc::new(HigherOrderFunctionExpr {
            name: self.name.clone(),
            fun: Arc::clone(&self.fun),
            args: children,
            slots: new_slots,
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

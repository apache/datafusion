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

//! Declaration of built-in (lambda) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::expressions::{LambdaExpr, Literal};
use crate::PhysicalExpr;

use arrow::array::{Array, NullArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::{
    expr_vec_fmt, ColumnarValue, LambdaFunctionArgs, LambdaFunctionLambdaArg,
    LambdaReturnFieldArgs, LambdaUDF, ValueOrLambda, ValueOrLambdaField,
    ValueOrLambdaParameter, Volatility,
};

/// Physical expression of a lambda function
pub struct LambdaFunctionExpr {
    fun: Arc<dyn LambdaUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl Debug for LambdaFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("LambdaFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl LambdaFunctionExpr {
    /// Create a new Lambda function
    pub fn new(
        name: &str,
        fun: Arc<dyn LambdaUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_field: FieldRef,
        config_options: Arc<ConfigOptions>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_field,
            config_options,
        }
    }

    /// Create a new Lambda function
    pub fn try_new(
        fun: Arc<dyn LambdaUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let name = fun.name().to_string();
        let arg_fields = args
            .iter()
            .map(|e| {
                let field = e.return_field(schema)?;
                match e.as_any().downcast_ref::<LambdaExpr>() {
                    Some(_lambda) => Ok(ValueOrLambdaField::Lambda(field)),
                    None => Ok(ValueOrLambdaField::Value(field)),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: verify that input data types is consistent with function's `TypeSignature`

        let arguments = args
            .iter()
            .map(|e| {
                e.as_any()
                    .downcast_ref::<Literal>()
                    .map(|literal| literal.value())
            })
            .collect::<Vec<_>>();

        let ret_args = LambdaReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &arguments,
        };

        let return_field = fun.return_field_from_args(ret_args)?;

        Ok(Self {
            fun,
            name,
            args,
            return_field,
            config_options,
        })
    }

    /// Get the lambda function implementation
    pub fn fun(&self) -> &dyn LambdaUDF {
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

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.return_field = self
            .return_field
            .as_ref()
            .clone()
            .with_nullable(nullable)
            .into();
        self
    }

    pub fn nullable(&self) -> bool {
        self.return_field.is_nullable()
    }

    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Given an arbitrary PhysicalExpr attempt to downcast it to a LambdaFunctionExpr
    /// and verify that its inner function is of type T.
    /// If the downcast fails, or the function is not of type T, returns `None`.
    /// Otherwise returns `Some(LambdaFunctionExpr)`.
    pub fn try_downcast_func<T>(expr: &dyn PhysicalExpr) -> Option<&LambdaFunctionExpr>
    where
        T: 'static,
    {
        match expr.as_any().downcast_ref::<LambdaFunctionExpr>() {
            Some(lambda_expr)
                if lambda_expr.fun().as_any().downcast_ref::<T>().is_some() =>
            {
                Some(lambda_expr)
            }
            _ => None,
        }
    }
}

impl fmt::Display for LambdaFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PartialEq for LambdaFunctionExpr {
    fn eq(&self, o: &Self) -> bool {
        if std::ptr::eq(self, o) {
            // The equality implementation is somewhat expensive, so let's short-circuit when possible.
            return true;
        }
        let Self {
            fun,
            name,
            args,
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
impl Eq for LambdaFunctionExpr {}
impl Hash for LambdaFunctionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            fun,
            name,
            args,
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

impl PhysicalExpr for LambdaFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg_fields = self
            .args
            .iter()
            .map(|e| {
                let field = e.return_field(batch.schema_ref())?;
                match e.as_any().downcast_ref::<LambdaExpr>() {
                    Some(_lambda) => Ok(ValueOrLambdaField::Lambda(field)),
                    None => Ok(ValueOrLambdaField::Value(field)),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let args_metadata = arg_fields.iter()
            .map(|field| match field {
                ValueOrLambdaField::Value(field) => ValueOrLambdaParameter::Value(Arc::clone(field)),
                ValueOrLambdaField::Lambda(_field) => ValueOrLambdaParameter::Lambda,
            })
            .collect::<Vec<_>>();

        let params = self.fun().lambdas_parameters(&args_metadata)?;

        let args = std::iter::zip(&self.args, params)
            .map(|(arg, lambda_params)| {
                match (arg.as_any().downcast_ref::<LambdaExpr>(), lambda_params) {
                    (Some(lambda), Some(lambda_params)) => {
                        if lambda.params().len() > lambda_params.len() {
                            return exec_err!(
                                "lambda defined {} params but UDF support only {}",
                                lambda.params().len(),
                                lambda_params.len()
                            );
                        }

                        let captures = lambda.captures();

                        let params = std::iter::zip(lambda.params(), lambda_params)
                            .map(|(name, param)| Arc::new(param.with_name(name)))
                            .collect();

                        let captures = if !captures.is_empty() {
                            let (fields, columns): (Vec<_>, _) = std::iter::zip(
                                batch.schema_ref().fields(),
                                batch.columns(),
                            )
                            .enumerate()
                            .map(|(column_index, (field, column))| {
                                if captures.contains(&column_index) {
                                    (Arc::clone(field), Arc::clone(column))
                                } else {
                                    (
                                        Arc::new(Field::new(
                                            field.name(),
                                            DataType::Null,
                                            false,
                                        )),
                                        Arc::new(NullArray::new(column.len())) as _,
                                    )
                                }
                            })
                            .unzip();

                            let schema = Arc::new(Schema::new(fields));

                            Some(RecordBatch::try_new(schema, columns)?)
                        } else {
                            None
                        };

                        Ok(ValueOrLambda::Lambda(LambdaFunctionLambdaArg {
                            params,
                            body: Arc::clone(lambda.body()),
                            captures,
                        }))
                    }
                    (Some(_lambda), None) => exec_err!(
                        "{} don't reported the parameters of one of it's lambdas",
                        self.fun.name()
                    ),
                    (None, Some(_lambda_params)) => exec_err!(
                        "{} reported parameters for an argument that is not a lambda",
                        self.fun.name()
                    ),
                    (None, None) => Ok(ValueOrLambda::Value(arg.evaluate(batch)?)),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ValueOrLambda::Value(ColumnarValue::Scalar(_))));

        // evaluate the function
        let output = self.fun.invoke_with_args(LambdaFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

        if let ColumnarValue::Array(array) = &output {
            if array.len() != batch.num_rows() {
                // If the arguments are a non-empty slice of scalar values, we can assume that
                // returning a one-element array is equivalent to returning a scalar.
                let preserve_scalar =
                    array.len() == 1 && !input_empty && input_all_scalar;
                return if preserve_scalar {
                    ScalarValue::try_from_array(array, 0).map(ColumnarValue::Scalar)
                } else {
                    internal_err!("UDF {} returned a different number of rows than expected. Expected: {}, Got: {}",
                            self.name, batch.num_rows(), array.len())
                };
            }
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
        Ok(Arc::new(LambdaFunctionExpr::new(
            &self.name,
            Arc::clone(&self.fun),
            children,
            Arc::clone(&self.return_field),
            Arc::clone(&self.config_options),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.fun.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.fun.propagate_constraints(interval, children)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let sort_properties = self.fun.output_ordering(children)?;
        let preserves_lex_ordering = self.fun.preserves_lex_ordering(children)?;
        let children_range = children
            .iter()
            .map(|props| &props.range)
            .collect::<Vec<_>>();
        let range = self.fun().evaluate_bounds(&children_range)?;

        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering,
        })
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use super::*;
    use crate::expressions::Column;
    use crate::LambdaFunctionExpr;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_expr::{LambdaFunctionArgs, LambdaUDF, Signature};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_physical_expr_common::physical_expr::is_volatile;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    /// Test helper to create a mock UDF with a specific volatility
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockLambdaUDF {
        signature: Signature,
    }

    impl LambdaUDF for MockLambdaUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_field_from_args(
            &self,
            _args: LambdaReturnFieldArgs,
        ) -> Result<FieldRef> {
            Ok(Arc::new(Field::new("", DataType::Int32, false)))
        }

        fn invoke_with_args(&self, _args: LambdaFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))))
        }
    }

    #[test]
    fn test_lambda_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(MockLambdaUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        });

        // Create a non-volatile UDF
        let stable_udf = Arc::new(MockLambdaUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        });

        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let config_options = Arc::new(ConfigOptions::new());

        // Test volatile function
        let volatile_expr = LambdaFunctionExpr::try_new(
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
        let stable_expr =
            LambdaFunctionExpr::try_new(stable_udf, args, &schema, config_options)
                .unwrap();

        assert!(!stable_expr.is_volatile_node());
        let stable_arc: Arc<dyn PhysicalExpr> = Arc::new(stable_expr);
        assert!(!is_volatile(&stable_arc));
    }
}

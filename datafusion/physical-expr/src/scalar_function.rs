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

//! Declaration of built-in (scalar) functions.
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

use crate::expressions::Literal;
use crate::PhysicalExpr;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::data_types_with_scalar_udf;
use datafusion_expr::{
    expr_vec_fmt, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
};
use datafusion_physical_expr_common::physical_expr::{DynEq, DynHash};
use itertools::Itertools;

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: Arc<ScalarUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: Arc<ScalarUDF>,
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

    /// Create a new Scalar function
    pub fn try_new(
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let name = fun.name().to_string();
        let arg_fields = args
            .iter()
            .map(|e| e.return_field(schema))
            .collect::<Result<Vec<_>>>()?;

        // verify that input data types is consistent with function's `TypeSignature`
        let arg_types = arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        data_types_with_scalar_udf(&arg_types, &fun)?;

        let arguments = args
            .iter()
            .map(|e| {
                e.as_any()
                    .downcast_ref::<Literal>()
                    .map(|literal| literal.value())
            })
            .collect::<Vec<_>>();
        let ret_args = ReturnFieldArgs {
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

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
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
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl DynEq for ScalarFunctionExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>().is_some_and(|o| {
            self.fun.eq(&o.fun)
                && self.name.eq(&o.name)
                && self.args.eq(&o.args)
                && self.return_field.eq(&o.return_field)
                && self
                    .config_options
                    .entries()
                    .iter()
                    .sorted_by(|&l, &r| l.key.cmp(&r.key))
                    .zip(
                        o.config_options
                            .entries()
                            .iter()
                            .sorted_by(|&l, &r| l.key.cmp(&r.key)),
                    )
                    .filter(|(l, r)| l.ne(r))
                    .count()
                    == 0
        })
    }
}

impl DynHash for ScalarFunctionExpr {
    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.fun.hash(&mut state);
        self.name.hash(&mut state);
        self.args.hash(&mut state);
        self.return_field.hash(&mut state);
        self.config_options.entries().hash(&mut state);
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
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
        let args = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let arg_fields = self
            .args
            .iter()
            .map(|e| e.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

        // evaluate the function
        let output = self.fun.invoke_with_args(ScalarFunctionArgs {
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
        Ok(Arc::new(ScalarFunctionExpr::new(
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
}

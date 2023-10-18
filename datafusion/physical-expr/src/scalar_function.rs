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

use crate::functions::out_ordering;
use crate::physical_expr::down_cast_any_ref;
use crate::sort_properties::SortProperties;
use crate::utils::expr_list_eq_strict_order;
use crate::PhysicalExpr;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr::expr_vec_fmt;
use datafusion_expr::BuiltinScalarFunction;
use datafusion_expr::ColumnarValue;
use datafusion_expr::FuncMonotonicity;
use datafusion_expr::ScalarFunctionImplementation;
use std::any::Any;
use std::fmt::Debug;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: ScalarFunctionImplementation,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
    // Keeps monotonicity information of the function.
    // FuncMonotonicity vector is one to one mapped to `args`,
    // and it specifies the effect of an increase or decrease in
    // the corresponding `arg` to the function value.
    monotonicity: Option<FuncMonotonicity>,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: ScalarFunctionImplementation,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
        monotonicity: Option<FuncMonotonicity>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type: return_type.clone(),
            monotonicity,
        }
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarFunctionImplementation {
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
        &self.return_type
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // evaluate the arguments, if there are no arguments we'll instead pass in a null array
        // indicating the batch size (as a convention)
        let inputs = match (self.args.len(), self.name.parse::<BuiltinScalarFunction>()) {
            // MakeArray support zero argument but has the different behavior from the array with one null.
            (0, Ok(scalar_fun))
                if scalar_fun.supports_zero_argument()
                    && scalar_fun != BuiltinScalarFunction::MakeArray =>
            {
                vec![ColumnarValue::create_null_array(batch.num_rows())]
            }
            _ => self
                .args
                .iter()
                .map(|e| e.evaluate(batch))
                .collect::<Result<Vec<_>>>()?,
        };

        // evaluate the function
        let fun = self.fun.as_ref();
        (fun)(&inputs)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::new(
            &self.name,
            self.fun.clone(),
            children,
            self.return_type(),
            self.monotonicity.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.name.hash(&mut s);
        self.args.hash(&mut s);
        self.return_type.hash(&mut s);
        // Add `self.fun` when hash is available
    }

    fn get_ordering(&self, children: &[SortProperties]) -> SortProperties {
        self.monotonicity
            .as_ref()
            .map(|monotonicity| out_ordering(monotonicity, children))
            .unwrap_or(SortProperties::Unordered)
    }
}

impl PartialEq<dyn Any> for ScalarFunctionExpr {
    /// Comparing name, args and return_type
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && expr_list_eq_strict_order(&self.args, &x.args)
                    && self.return_type == x.return_type
            })
            .unwrap_or(false)
    }
}

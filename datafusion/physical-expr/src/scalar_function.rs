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
use std::ops::Neg;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use datafusion_common::{internal_err, DFSchema, Result};
use datafusion_expr::type_coercion::functions::data_types;
use datafusion_expr::{
    expr_vec_fmt, ColumnarValue, Expr, FuncMonotonicity, ScalarFunctionDefinition,
    ScalarUDF,
};

use crate::physical_expr::{down_cast_any_ref, physical_exprs_equal};
use crate::sort_properties::SortProperties;
use crate::PhysicalExpr;

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: ScalarFunctionDefinition,
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
            .field("monotonicity", &self.monotonicity)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: ScalarFunctionDefinition,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
        monotonicity: Option<FuncMonotonicity>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type,
            monotonicity,
        }
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarFunctionDefinition {
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

    /// Monotonicity information of the function
    pub fn monotonicity(&self) -> &Option<FuncMonotonicity> {
        &self.monotonicity
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        match self.fun {
            ScalarFunctionDefinition::UDF(ref fun) => {
                let output = match self.args.is_empty() {
                    true => fun.invoke_no_args(batch.num_rows()),
                    false => fun.invoke(&inputs),
                }?;

                if let ColumnarValue::Array(array) = &output {
                    if array.len() != batch.num_rows() {
                        return internal_err!("UDF returned a different number of rows than expected. Expected: {}, Got: {}",
                        batch.num_rows(), array.len());
                    }
                }
                Ok(output)
            }
        }
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
            self.return_type().clone(),
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
                    && physical_exprs_equal(&self.args, &x.args)
                    && self.return_type == x.return_type
            })
            .unwrap_or(false)
    }
}

/// Create a physical expression of the UDF.
///
/// Arguments:
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    args: &[Expr],
    input_dfschema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_expr_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // verify that input data types is consistent with function's `TypeSignature`
    data_types(&input_expr_types, fun.signature())?;

    // Since we have arg_types, we dont need args and schema.
    let return_type =
        fun.return_type_from_exprs(args, input_dfschema, &input_expr_types)?;

    let fun_def = ScalarFunctionDefinition::UDF(Arc::new(fun.clone()));
    Ok(Arc::new(ScalarFunctionExpr::new(
        fun.name(),
        fun_def,
        input_phy_exprs.to_vec(),
        return_type,
        fun.monotonicity()?,
    )))
}

/// Determines a [ScalarFunctionExpr]'s monotonicity for the given arguments
/// and the function's behavior depending on its arguments.
///
/// [ScalarFunctionExpr]: crate::scalar_function::ScalarFunctionExpr
pub fn out_ordering(
    func: &FuncMonotonicity,
    arg_orderings: &[SortProperties],
) -> SortProperties {
    func.iter().zip(arg_orderings).fold(
        SortProperties::Singleton,
        |prev_sort, (item, arg)| {
            let current_sort = func_order_in_one_dimension(item, arg);

            match (prev_sort, current_sort) {
                (_, SortProperties::Unordered) => SortProperties::Unordered,
                (SortProperties::Singleton, SortProperties::Ordered(_)) => current_sort,
                (SortProperties::Ordered(prev), SortProperties::Ordered(current))
                    if prev.descending != current.descending =>
                {
                    SortProperties::Unordered
                }
                _ => prev_sort,
            }
        },
    )
}

/// This function decides the monotonicity property of a [ScalarFunctionExpr] for a single argument (i.e. across a single dimension), given that argument's sort properties.
///
/// [ScalarFunctionExpr]: crate::scalar_function::ScalarFunctionExpr
fn func_order_in_one_dimension(
    func_monotonicity: &Option<bool>,
    arg: &SortProperties,
) -> SortProperties {
    if *arg == SortProperties::Singleton {
        SortProperties::Singleton
    } else {
        match func_monotonicity {
            None => SortProperties::Unordered,
            Some(false) => {
                if let SortProperties::Ordered(_) = arg {
                    arg.neg()
                } else {
                    SortProperties::Unordered
                }
            }
            Some(true) => {
                if let SortProperties::Ordered(_) = arg {
                    *arg
                } else {
                    SortProperties::Unordered
                }
            }
        }
    }
}

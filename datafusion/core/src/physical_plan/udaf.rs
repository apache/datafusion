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

//! This module contains functions and structs supporting user-defined aggregate functions.

use fmt::Debug;
use std::any::Any;
use std::fmt;

use arrow::{
    datatypes::Field,
    datatypes::{DataType, Schema},
};

use super::{
    expressions::format_state_name, type_coercion::coerce, Accumulator, AggregateExpr,
};
use crate::error::Result;
use crate::physical_plan::PhysicalExpr;
pub use datafusion_expr::AggregateUDF;

use std::sync::Arc;

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    // coerce
    let coerced_phy_exprs = coerce(input_phy_exprs, input_schema, &fun.signature)?;

    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(AggregateFunctionExpr {
        fun: fun.clone(),
        args: coerced_phy_exprs.clone(),
        data_type: (fun.return_type)(&coerced_exprs_types)?.as_ref().clone(),
        name: name.into(),
    }))
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Output / return type of this aggregate
    data_type: DataType,
    name: String,
}

impl AggregateExpr for AggregateFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let fields = (self.fun.state_type)(&self.data_type)?
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(
                    &format_state_name(&self.name, &format!("{}", i)),
                    data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<Field>>();

        Ok(fields)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        (self.fun.accumulator)(&self.data_type)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

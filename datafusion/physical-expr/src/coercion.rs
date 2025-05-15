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

//! This module contains utilities for applying type coercion during physical planning

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expressions::CastExpr;

/// Applies type coercion to physical expressions for aggregate functions based on
/// the function's `coerce_types` implementation.
///
/// This ensures that the type coercion defined during logical planning is
/// properly applied during physical planning, eliminating the need for explicit
/// casting during execution.
///
/// # Arguments
///
/// * `func` - The aggregate function to apply coercion for
/// * `physical_args` - Original physical expressions for the function arguments
/// * `input_schema` - Schema for evaluating the expressions
///
/// # Returns
///
/// A new vector of physical expressions with type coercion applied where needed,
/// or the original expressions if no coercion is needed.
pub fn apply_aggregate_coercion(
    func: &Arc<AggregateUDF>,
    physical_args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let arg_types: Vec<DataType> = physical_args
        .iter()
        .map(|expr| expr.data_type(input_schema))
        .collect::<Result<_>>()?;

    if let Ok(coerced_types) = func.inner().coerce_types(&arg_types) {
        let needs_coercion = arg_types
            .iter()
            .zip(coerced_types.iter())
            .any(|(orig, coerced)| orig != coerced);

        if needs_coercion {
            let coerced_args = physical_args
                .iter()
                .zip(coerced_types.iter())
                .map(|(arg, coerced_type)| {
                    let expr_type = arg.data_type(input_schema)?;
                    if &expr_type == coerced_type {
                        // No coercion needed for this argument
                        Ok(Arc::<dyn PhysicalExpr>::clone(arg))
                    } else {
                        // Create a cast expression for type coercion
                        Ok(Arc::new(CastExpr::new(
                            Arc::<dyn PhysicalExpr>::clone(arg),
                            coerced_type.clone(),
                            None,
                        )) as Arc<dyn PhysicalExpr>)
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            return Ok(coerced_args);
        }
    }

    Ok(physical_args.to_vec())
}

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

//! Support the coercion rule for aggregate function.

use crate::expressions::try_cast;
use crate::PhysicalExpr;
use arrow::datatypes::Schema;
use datafusion_common::Result;
use datafusion_expr::{aggregate_function, AggregateFunction, Signature};
use std::sync::Arc;

/// Returns the coerced exprs for each `input_exprs`.
/// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
/// data type of `input_exprs` need to be coerced.
pub fn coerce_exprs(
    agg_fun: &AggregateFunction,
    input_exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    signature: &Signature,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let input_types = input_exprs
        .iter()
        .map(|e| e.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    // get the coerced data types
    let coerced_types =
        aggregate_function::coerce_types(agg_fun, &input_types, signature)?;

    // try cast if need
    input_exprs
        .iter()
        .zip(coerced_types.into_iter())
        .map(|(expr, coerced_type)| try_cast(expr.clone(), schema, coerced_type))
        .collect::<Result<Vec<_>>>()
}

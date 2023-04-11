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

//! Utilities used in aggregates

use crate::AggregateExpr;
use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// Convert scalar values from an accumulator into arrays.
pub fn get_accum_scalar_values_as_arrays(
    accum: &dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    Ok(accum
        .state()?
        .iter()
        .map(|s| s.to_array_of_size(1))
        .collect::<Vec<_>>())
}

/// Downcast the [`Box<dyn AggregateExpr>`] or [`Arc<dyn AggregateExpr>`] and return
/// the inner trait object as [`Any`](std::any::Any) so that it can be downcast to a specific implementation.
/// This method is used when implementing the [`PartialEq<dyn Any>'] for aggregation expressions and allows
/// comparing the equality between the trait objects.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn AggregateExpr>>() {
        any.downcast_ref::<Arc<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn AggregateExpr>>() {
        any.downcast_ref::<Box<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

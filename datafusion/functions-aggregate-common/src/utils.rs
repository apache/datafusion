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

use std::{any::Any, sync::Arc};

use arrow::{
    compute::SortOptions,
    datatypes::{DataType, Field},
};
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr_common::signature::TypeSignature;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use crate::expr::AggregateExpr;

/// Downcast a `Box<dyn AggregateExpr>` or `Arc<dyn AggregateExpr>`
/// and return the inner trait object as [`Any`] so
/// that it can be downcast to a specific implementation.
///
/// This method is used when implementing the `PartialEq<dyn Any>`
/// for [`AggregateExpr`] aggregation expressions and allows comparing the equality
/// between the trait objects.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if let Some(obj) = any.downcast_ref::<Arc<dyn AggregateExpr>>() {
        obj.as_any()
    } else if let Some(obj) = any.downcast_ref::<Box<dyn AggregateExpr>>() {
        obj.as_any()
    } else {
        any
    }
}

/// Construct corresponding fields for lexicographical ordering requirement expression
pub fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<Field> {
    ordering_req
        .iter()
        .zip(data_types.iter())
        .map(|(sort_expr, dtype)| {
            Field::new(
                sort_expr.expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .collect()
}

/// Selects the sort option attribute from all the given `PhysicalSortExpr`s.
pub fn get_sort_options(ordering_req: &[PhysicalSortExpr]) -> Vec<SortOptions> {
    ordering_req.iter().map(|item| item.options).collect()
}

/// Validate the length of `input_types` matches the `signature` for `agg_fun`.
///
/// This method DOES NOT validate the argument types - only that (at least one,
/// in the case of [`TypeSignature::OneOf`]) signature matches the desired
/// number of input types.
pub fn check_arg_count(
    func_name: &str,
    input_types: &[DataType],
    signature: &TypeSignature,
) -> Result<()> {
    match signature {
        TypeSignature::Uniform(agg_count, _) | TypeSignature::Any(agg_count) => {
            if input_types.len() != *agg_count {
                return plan_err!(
                    "The function {func_name} expects {:?} arguments, but {:?} were provided",
                    agg_count,
                    input_types.len()
                );
            }
        }
        TypeSignature::Exact(types) => {
            if types.len() != input_types.len() {
                return plan_err!(
                    "The function {func_name} expects {:?} arguments, but {:?} were provided",
                    types.len(),
                    input_types.len()
                );
            }
        }
        TypeSignature::OneOf(variants) => {
            let ok = variants
                .iter()
                .any(|v| check_arg_count(func_name, input_types, v).is_ok());
            if !ok {
                return plan_err!(
                    "The function {func_name} does not accept {:?} function arguments.",
                    input_types.len()
                );
            }
        }
        TypeSignature::VariadicAny => {
            if input_types.is_empty() {
                return plan_err!(
                    "The function {func_name} expects at least one argument"
                );
            }
        }
        _ => {
            return internal_err!(
                "Aggregate functions do not support this {signature:?}"
            );
        }
    }
    Ok(())
}

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

use std::ops::Neg;

use datafusion_expr::FuncMonotonicity;

use crate::sort_properties::SortProperties;

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

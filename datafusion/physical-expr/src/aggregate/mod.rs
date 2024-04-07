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

use std::sync::Arc;

use crate::expressions::{NthValueAgg, OrderSensitiveArrayAgg};

pub use datafusion_physical_expr_common::aggregate::AggregateExpr;

mod hyperloglog;
mod tdigest;

pub(crate) mod approx_distinct;
pub(crate) mod approx_median;
pub(crate) mod approx_percentile_cont;
pub(crate) mod approx_percentile_cont_with_weight;
pub(crate) mod array_agg;
pub(crate) mod array_agg_distinct;
pub(crate) mod array_agg_ordered;
pub(crate) mod average;
pub(crate) mod bit_and_or_xor;
pub(crate) mod bool_and_or;
pub(crate) mod correlation;
pub(crate) mod count;
pub(crate) mod count_distinct;
pub(crate) mod covariance;
pub(crate) mod grouping;
pub(crate) mod median;
pub(crate) mod nth_value;
pub(crate) mod string_agg;
#[macro_use]
pub(crate) mod min_max;
pub(crate) mod groups_accumulator;
pub(crate) mod regr;
pub(crate) mod stats;
pub(crate) mod stddev;
pub(crate) mod sum;
pub(crate) mod sum_distinct;
pub(crate) mod variance;

pub mod build_in;
pub mod moving_min_max;
pub mod utils;

/// Checks whether the given aggregate expression is order-sensitive.
/// For instance, a `SUM` aggregation doesn't depend on the order of its inputs.
/// However, an `ARRAY_AGG` with `ORDER BY` depends on the input ordering.
pub fn is_order_sensitive(aggr_expr: &Arc<dyn AggregateExpr>) -> bool {
    aggr_expr.as_any().is::<OrderSensitiveArrayAgg>()
        || aggr_expr.as_any().is::<NthValueAgg>()
}

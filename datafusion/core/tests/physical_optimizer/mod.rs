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

//! Physical Optimizer integration tests

#[expect(clippy::needless_pass_by_value)]
mod aggregate_statistics;
mod combine_partial_final_agg;
// `enforce_distribution`, `enforce_sorting`, `enforce_sorting_monotonicity`,
// and `replace_with_order_preserving_variants` have moved to
// `datafusion/physical-optimizer/tests/` so they live alongside the rules
// they exercise. See <https://github.com/apache/datafusion/pull/21976>.
mod filter_pushdown;
mod join_selection;
#[expect(clippy::needless_pass_by_value)]
mod limit_pushdown;
mod limited_distinct_aggregation;
mod partition_statistics;
mod projection_pushdown;
mod pushdown_sort;
mod sanity_checker;
// `test_utils` still hosts shared helpers for the remaining integration
// tests in this directory. Some helpers were only consumed by the four
// modules that moved to `physical-optimizer/tests/`, so the unused ones
// are dead from this binary's perspective — silence the lint here until
// the rest of these tests move alongside their rules.
#[expect(clippy::needless_pass_by_value, dead_code)]
mod test_utils;
mod window_optimize;
mod window_topn;

mod pushdown_utils;

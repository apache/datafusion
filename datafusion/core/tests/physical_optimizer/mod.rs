// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Physical Optimizer integration tests

#[expect(clippy::needless_pass_by_value)]
mod aggregate_statistics;
mod combine_partial_final_agg;
mod eliminate_constant_join;
#[expect(clippy::needless_pass_by_value)]
mod enforce_distribution;
mod enforce_sorting;
mod enforce_sorting_monotonicity;
mod ensure_requirements;
mod filter_pushdown;
mod join_selection;
#[expect(clippy::needless_pass_by_value)]
mod limit_pushdown;
mod limited_distinct_aggregation;
mod output_requirements;
mod partition_statistics;
mod projection_pushdown;
mod pushdown_sort;
mod replace_with_order_preserving_variants;
mod sanity_checker;
#[expect(clippy::needless_pass_by_value)]
mod test_utils;
mod update_aggr_exprs;
mod window_optimize;
mod window_topn;

mod pushdown_utils;

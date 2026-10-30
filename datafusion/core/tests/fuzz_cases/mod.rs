// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[expect(clippy::needless_pass_by_value)]
mod aggregate_fuzz;
mod distinct_count_string_fuzz;
#[expect(clippy::needless_pass_by_value)]
mod join_fuzz;
mod merge_fuzz;
mod null_aware_join_fuzz;
#[expect(clippy::needless_pass_by_value)]
mod sort_fuzz;
#[expect(clippy::needless_pass_by_value)]
mod sort_query_fuzz;
mod topk_filter_pushdown;

mod aggregation_fuzzer;
#[expect(clippy::needless_pass_by_value)]
mod equivalence;

mod pruning;

mod leaf_pushdown_fuzz;
mod limit_fuzz;
#[expect(clippy::needless_pass_by_value)]
mod sort_preserving_repartition_fuzz;
mod window_fuzz;

// Utility modules
mod once_exec;
mod record_batch_generator;
mod scalar_value_fuzz;
mod spilling_fuzz_in_memory_constrained_env;

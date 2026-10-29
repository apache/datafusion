// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

pub mod aggregate_statistics;
pub mod combine_partial_final_agg;
pub mod eliminate_constant_join;
pub mod ensure_coop;
pub mod ensure_requirements;
// `enforce_distribution` and `enforce_sorting` are now internal implementation
// details of `ensure_requirements`. Re-export at the crate root so external test
// modules keep their public paths.
pub use ensure_requirements::{enforce_distribution, enforce_sorting};
pub mod filter_pushdown;
pub mod join_selection;
pub mod limit_pushdown;
pub mod limit_pushdown_past_window;
pub mod limited_distinct_aggregation;
pub mod optimizer;
pub mod output_requirements;
pub mod projection_pushdown;
pub use datafusion_pruning as pruning;
pub mod hash_join_buffering;
pub mod pushdown_sort;
pub mod sanity_checker;
pub mod topk_aggregation;
pub mod topk_repartition;
pub mod update_aggr_exprs;
pub mod utils;
pub mod window_topn;

pub use optimizer::{ConfigOnlyContext, PhysicalOptimizerContext, PhysicalOptimizerRule};

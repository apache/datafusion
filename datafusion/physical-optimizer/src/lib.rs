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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
// https://github.com/apache/datafusion/issues/18503
#![deny(clippy::needless_pass_by_value)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

pub mod aggregate_statistics;
pub mod coalesce_batches;
pub mod combine_partial_final_agg;
pub mod enforce_distribution;
pub mod enforce_sorting;
pub mod ensure_coop;
pub mod filter_pushdown;
pub mod join_selection;
pub mod limit_pushdown;
pub mod limit_pushdown_past_window;
pub mod limited_distinct_aggregation;
pub mod optimizer;
pub mod output_requirements;
pub mod projection_pushdown;
pub use datafusion_pruning as pruning;
pub mod sanity_checker;
pub mod topk_aggregation;
pub mod update_aggr_exprs;
pub mod utils;

pub use optimizer::{OptimizerContext, PhysicalOptimizerRule};

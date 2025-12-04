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

// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

pub mod access_plan;
pub mod file_format;
pub mod metadata;
mod metrics;
mod opener;
mod page_filter;
mod reader;
mod row_filter;
mod row_group_filter;
pub mod source;
mod writer;

pub use access_plan::{ParquetAccessPlan, RowGroupAccess};
pub use file_format::*;
pub use metrics::ParquetFileMetrics;
pub use page_filter::PagePruningAccessPlanFilter;
pub use reader::*; // Expose so downstream crates can use it
pub use row_filter::build_row_filter;
pub use row_filter::can_expr_be_pushed_down_with_schemas;
pub use row_group_filter::RowGroupAccessPlanFilter;
pub use writer::plan_to_parquet;

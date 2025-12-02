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
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
// https://github.com/apache/datafusion/issues/18881
#![deny(clippy::allow_attributes)]

//! Traits for physical query plan, supporting parallel execution for partitioned relations.
//!
//! Entrypoint of this crate is trait [ExecutionPlan].

pub use datafusion_common::hash_utils;
pub use datafusion_common::utils::project_schema;
pub use datafusion_common::{internal_err, ColumnStatistics, Statistics};
pub use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
pub use datafusion_expr::{Accumulator, ColumnarValue};
pub use datafusion_physical_expr::window::WindowExpr;
use datafusion_physical_expr::PhysicalSortExpr;
pub use datafusion_physical_expr::{
    expressions, Distribution, Partitioning, PhysicalExpr,
};

pub use crate::display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
pub use crate::execution_plan::{
    collect, collect_partitioned, displayable, execute_input_stream, execute_stream,
    execute_stream_partitioned, get_plan_string, with_new_children_if_necessary,
    ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
pub use crate::metrics::Metric;
pub use crate::ordering::InputOrderMode;
pub use crate::stream::EmptyRecordBatchStream;
pub use crate::topk::TopK;
pub use crate::visitor::{accept, visit_execution_plan, ExecutionPlanVisitor};
pub use crate::work_table::WorkTable;
pub use spill::spill_manager::SpillManager;

mod ordering;
mod render_tree;
mod topk;
mod visitor;

pub mod aggregates;
pub mod analyze;
pub mod async_func;
pub mod coalesce;
pub mod coalesce_batches;
pub mod coalesce_partitions;
pub mod common;
pub mod coop;
pub mod display;
pub mod empty;
pub mod execution_plan;
pub mod explain;
pub mod filter;
pub mod filter_pushdown;
pub mod joins;
pub mod limit;
pub mod memory;
pub mod metrics;
pub mod placeholder_row;
pub mod projection;
pub mod recursive_query;
pub mod repartition;
pub mod sorts;
pub mod spill;
pub mod stream;
pub mod streaming;
pub mod tree_node;
pub mod union;
pub mod unnest;
pub mod windows;
pub mod work_table;
pub mod udaf {
    pub use datafusion_expr::StatisticsArgs;
    pub use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
}

pub mod test;

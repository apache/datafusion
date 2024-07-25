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

// Make cheap clones clear: https://github.com/apache/datafusion/issues/11143

#![deny(clippy::clone_on_ref_ptr)]

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
    expressions, functions, udf, AggregateExpr, Distribution, Partitioning, PhysicalExpr,
};

pub use crate::display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
pub(crate) use crate::execution_plan::execution_mode_from_children;
pub use crate::execution_plan::{
    collect, collect_partitioned, displayable, execute_input_stream, execute_stream,
    execute_stream_partitioned, get_plan_string, with_new_children_if_necessary,
    ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
pub use crate::metrics::Metric;
pub use crate::ordering::InputOrderMode;
pub use crate::stream::EmptyRecordBatchStream;
pub use crate::topk::TopK;
pub use crate::visitor::{accept, visit_execution_plan, ExecutionPlanVisitor};

mod ordering;
mod topk;
mod visitor;

pub mod aggregates;
pub mod analyze;
pub mod coalesce_batches;
pub mod coalesce_partitions;
pub mod common;
pub mod display;
pub mod empty;
pub mod execution_plan;
pub mod explain;
pub mod filter;
pub mod insert;
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
pub mod values;
pub mod windows;
pub mod work_table;

pub mod udaf {
    pub use datafusion_physical_expr_functions_aggregate::aggregate::AggregateFunctionExpr;
}

pub mod coalesce;
#[cfg(test)]

mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow_schema::{Schema, SchemaRef};

    use datafusion_common::{Result, Statistics};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};

    use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

    #[derive(Debug)]
    pub struct EmptyExec;

    impl EmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for EmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    pub struct RenamedEmptyExec;

    impl RenamedEmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for RenamedEmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for RenamedEmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn static_name() -> &'static str
        where
            Self: Sized,
        {
            "MyRenamedEmptyExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[test]
    fn test_execution_plan_name() {
        let schema1 = Arc::new(Schema::empty());
        let default_name_exec = EmptyExec::new(schema1);
        assert_eq!(default_name_exec.name(), "EmptyExec");

        let schema2 = Arc::new(Schema::empty());
        let renamed_exec = RenamedEmptyExec::new(schema2);
        assert_eq!(renamed_exec.name(), "MyRenamedEmptyExec");
        assert_eq!(RenamedEmptyExec::static_name(), "MyRenamedEmptyExec");
    }

    /// A compilation test to ensure that the `ExecutionPlan::name()` method can
    /// be called from a trait object.
    /// Related ticket: https://github.com/apache/datafusion/pull/11047
    #[allow(dead_code)]
    fn use_execution_plan_as_trait_object(plan: &dyn ExecutionPlan) {
        let _ = plan.name();
    }
}

pub mod test;

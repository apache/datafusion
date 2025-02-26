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

use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{Constraints, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// Common behaviors in Data Sources for both from Files and Memory.
/// See `DataSourceExec` for physical plan implementation
pub trait DataSource: Send + Sync {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream>;
    fn as_any(&self) -> &dyn Any;
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;

    /// Return a copy of this DataSource with a new partitioning scheme
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
    ) -> datafusion_common::Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn output_partitioning(&self) -> Partitioning;
    fn eq_properties(&self) -> EquivalenceProperties;
    fn statistics(&self) -> datafusion_common::Result<Statistics>;
    /// Return a copy of this DataSource with a new fetch limit
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>>;
    fn fetch(&self) -> Option<usize>;
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>>;
}

impl Debug for dyn DataSource {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DataSource: ")
    }
}

/// Unified data source for file formats like JSON, CSV, AVRO, ARROW, PARQUET
#[derive(Clone, Debug)]
pub struct DataSourceExec {
    data_source: Arc<dyn DataSource>,
    cache: PlanProperties,
}

impl DisplayAs for DataSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DataSourceExec: ")?;
        self.data_source.fmt_as(t, f)
    }
}

impl ExecutionPlan for DataSourceExec {
    fn name(&self) -> &'static str {
        "DataSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let data_source = self.data_source.repartitioned(
            target_partitions,
            config.optimizer.repartition_file_min_size,
            self.properties().eq_properties.output_ordering(),
        )?;

        if let Some(source) = data_source {
            let output_partitioning = source.output_partitioning();
            let plan = self
                .clone()
                .with_data_source(source)
                // Changing source partitioning may invalidate output partitioning. Update it also
                .with_partitioning(output_partitioning);
            Ok(Some(Arc::new(plan)))
        } else {
            Ok(Some(Arc::new(self.clone())))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.data_source.open(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.data_source.metrics().clone_inner())
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        self.data_source.statistics()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let data_source = self.data_source.with_fetch(limit)?;
        let cache = self.cache.clone();

        Some(Arc::new(Self { data_source, cache }))
    }

    fn fetch(&self) -> Option<usize> {
        self.data_source.fetch()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        self.data_source.try_swapping_with_projection(projection)
    }
}

impl DataSourceExec {
    pub fn new(data_source: Arc<dyn DataSource>) -> Self {
        let cache = Self::compute_properties(Arc::clone(&data_source));
        Self { data_source, cache }
    }

    /// Return the source object
    pub fn data_source(&self) -> &Arc<dyn DataSource> {
        &self.data_source
    }

    pub fn with_data_source(mut self, data_source: Arc<dyn DataSource>) -> Self {
        self.cache = Self::compute_properties(Arc::clone(&data_source));
        self.data_source = data_source;
        self
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.cache = self.cache.with_constraints(constraints);
        self
    }

    /// Assign output partitioning
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.cache = self.cache.with_partitioning(partitioning);
        self
    }

    fn compute_properties(data_source: Arc<dyn DataSource>) -> PlanProperties {
        PlanProperties::new(
            data_source.eq_properties(),
            data_source.output_partitioning(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

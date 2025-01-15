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

use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use datafusion_common::config::ConfigOptions;
use datafusion_common::Statistics;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};

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
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
        _exec: DataSourceExec,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
    fn statistics(&self) -> datafusion_common::Result<Statistics>;
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }
    fn fetch(&self) -> Option<usize> {
        None
    }
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }
    fn properties(&self) -> PlanProperties;
}

impl Debug for dyn DataSource {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DataSource: ")
    }
}

/// Unified data source for file formats like JSON, CSV, AVRO, ARROW, PARQUET
#[derive(Clone, Debug)]
pub struct DataSourceExec {
    source: Arc<dyn DataSource>,
    cache: PlanProperties,
}

impl DisplayAs for DataSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DataSourceExec: ")?;
        self.source.fmt_as(t, f)
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
        self.source
            .repartitioned(target_partitions, config, self.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.source.open(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.source.metrics().clone_inner())
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        self.source.statistics()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut source = Arc::clone(&self.source);
        source = source.with_fetch(limit)?;
        let cache = source.properties().clone();

        Some(Arc::new(Self { source, cache }))
    }

    fn fetch(&self) -> Option<usize> {
        self.source.fetch()
    }
}

impl DataSourceExec {
    pub fn new(source: Arc<dyn DataSource>) -> Self {
        let cache = source.properties().clone();
        Self { source, cache }
    }

    /// Return the source object
    #[allow(unused)]
    pub fn source(&self) -> Arc<dyn DataSource> {
        Arc::clone(&self.source)
    }

    pub fn with_source(mut self, source: Arc<dyn DataSource>) -> Self {
        self.cache = source.properties();
        self.source = source;
        self
    }
}

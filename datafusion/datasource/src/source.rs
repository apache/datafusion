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

//! [`DataSource`] and [`DataSourceExec`]

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

use crate::file_scan_config::FileScanConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Constraints, Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::filter_pushdown::{
    filter_pushdown_not_supported, FilterDescription, FilterPushdownResult,
    FilterPushdownSupport,
};

/// A source of data, typically a list of files or memory
///
/// This trait provides common behaviors for abstract sources of data. It has
/// two common implementations:
///
/// 1. [`FileScanConfig`]: lists of files
/// 2. [`MemorySourceConfig`]: in memory list of `RecordBatch`
///
/// File format specific behaviors are defined by [`FileSource`]
///
/// # See Also
/// * [`FileSource`] for file format specific implementations (Parquet, Json, etc)
/// * [`DataSourceExec`]: The [`ExecutionPlan`] that reads from a `DataSource`
///
/// # Notes
///
/// Requires `Debug` to assist debugging
///
/// [`FileScanConfig`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html
/// [`MemorySourceConfig`]: https://docs.rs/datafusion/latest//datafusion/datasource/memory/struct.MemorySourceConfig.html
/// [`FileSource`]: crate::file::FileSource
pub trait DataSource: Send + Sync + Debug {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;
    fn as_any(&self) -> &dyn Any;
    /// Format this source for display in explain plans
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;

    /// Return a copy of this DataSource with a new partitioning scheme
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn output_partitioning(&self) -> Partitioning;
    fn eq_properties(&self) -> EquivalenceProperties;
    fn statistics(&self) -> Result<Statistics>;
    /// Return a copy of this DataSource with a new fetch limit
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>>;
    fn fetch(&self) -> Option<usize>;
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
    /// Try to push down filters into this DataSource.
    /// See [`ExecutionPlan::try_pushdown_filters`] for more details.
    fn try_pushdown_filters(
        &self,
        fd: FilterDescription,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownResult<Arc<dyn DataSource>>> {
        Ok(filter_pushdown_not_supported(fd))
    }
}

/// [`ExecutionPlan`] that reads one or more files
///
/// `DataSourceExec` implements common functionality such as applying
/// projections, and caching plan properties.
///
/// The [`DataSource`] describes where to find the data for this data source
/// (for example in files or what in memory partitions).
///
/// For file based [`DataSource`]s, format specific behavior is implemented in
/// the [`FileSource`] trait.
///
/// [`FileSource`]: crate::file::FileSource
#[derive(Clone, Debug)]
pub struct DataSourceExec {
    /// The source of the data -- for example, `FileScanConfig` or `MemorySourceConfig`
    data_source: Arc<dyn DataSource>,
    /// Cached plan properties such as sort order
    cache: PlanProperties,
}

impl DisplayAs for DataSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DataSourceExec: ")?;
            }
            DisplayFormatType::TreeRender => {}
        }
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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
    ) -> Result<SendableRecordBatchStream> {
        self.data_source.open(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.data_source.metrics().clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.data_source.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(partition) = partition {
            let mut statistics = Statistics::new_unknown(&self.schema());
            if let Some(file_config) =
                self.data_source.as_any().downcast_ref::<FileScanConfig>()
            {
                if let Some(file_group) = file_config.file_groups.get(partition) {
                    if let Some(stat) = file_group.file_statistics(None) {
                        statistics = stat.clone();
                    }
                }
            }
            Ok(statistics)
        } else {
            Ok(self.data_source.statistics()?)
        }
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
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.data_source.try_swapping_with_projection(projection)
    }

    fn try_pushdown_filters(
        &self,
        fd: FilterDescription,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownResult<Arc<dyn ExecutionPlan>>> {
        let FilterPushdownResult {
            support,
            remaining_description,
        } = self.data_source.try_pushdown_filters(fd, config)?;

        match support {
            FilterPushdownSupport::Supported {
                child_descriptions,
                op,
                revisit,
            } => {
                let new_exec = Arc::new(DataSourceExec::new(op));

                debug_assert!(child_descriptions.is_empty());
                debug_assert!(!revisit);

                Ok(FilterPushdownResult {
                    support: FilterPushdownSupport::Supported {
                        child_descriptions,
                        op: new_exec,
                        revisit,
                    },
                    remaining_description,
                })
            }
            FilterPushdownSupport::NotSupported => {
                Ok(filter_pushdown_not_supported(remaining_description))
            }
        }
    }
}

impl DataSourceExec {
    pub fn from_data_source(data_source: impl DataSource + 'static) -> Arc<Self> {
        Arc::new(Self::new(Arc::new(data_source)))
    }

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

    /// Downcast the `DataSourceExec`'s `data_source` to a specific file source
    ///
    /// Returns `None` if
    /// 1. the datasource is not scanning files (`FileScanConfig`)
    /// 2. The [`FileScanConfig::file_source`] is not of type `T`
    pub fn downcast_to_file_source<T: 'static>(&self) -> Option<(&FileScanConfig, &T)> {
        self.data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .and_then(|file_scan_conf| {
                file_scan_conf
                    .file_source()
                    .as_any()
                    .downcast_ref::<T>()
                    .map(|source| (file_scan_conf, source))
            })
    }
}

/// Create a new `DataSourceExec` from a `DataSource`
impl<S> From<S> for DataSourceExec
where
    S: DataSource + 'static,
{
    fn from(source: S) -> Self {
        Self::new(Arc::new(source))
    }
}

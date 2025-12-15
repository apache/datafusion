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

use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion_physical_plan::metrics::SplitMetrics;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::stream::BatchSplitStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use itertools::Itertools;

use crate::file_scan_config::FileScanConfig;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Constraints, Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
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
/// [`MemorySourceConfig`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemorySourceConfig.html
/// [`FileSource`]: crate::file::FileSource
/// [`FileFormat``]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/index.html
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
///
/// The following diagram shows how DataSource, FileSource, and DataSourceExec are related
/// ```text
///                       ┌─────────────────────┐                              -----► execute path
///                       │                     │                              ┄┄┄┄┄► init path
///                       │   DataSourceExec    │  
///                       │                     │    
///                       └───────▲─────────────┘
///                               ┊  │
///                               ┊  │
///                       ┌──────────▼──────────┐                            ┌──────────-──────────┐
///                       │                     │                            |                     |
///                       │  DataSource(trait)  │                            | TableProvider(trait)|
///                       │                     │                            |                     |
///                       └───────▲─────────────┘                            └─────────────────────┘
///                               ┊  │                                                  ┊
///               ┌───────────────┿──┴────────────────┐                                 ┊
///               |   ┌┄┄┄┄┄┄┄┄┄┄┄┘                   |                                 ┊
///               |   ┊                               |                                 ┊
///    ┌──────────▼──────────┐             ┌──────────▼──────────┐                      ┊
///    │                     │             │                     │           ┌──────────▼──────────┐
///    │   FileScanConfig    │             │ MemorySourceConfig  │           |                     |
///    │                     │             │                     │           |  FileFormat(trait)  |
///    └──────────────▲──────┘             └─────────────────────┘           |                     |
///               │   ┊                                                      └─────────────────────┘
///               │   ┊                                                                 ┊
///               │   ┊                                                                 ┊
///    ┌──────────▼──────────┐                                               ┌──────────▼──────────┐
///    │                     │                                               │     ArrowSource     │
///    │ FileSource(trait)   ◄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄│          ...        │
///    │                     │                                               │    ParquetSource    │
///    └─────────────────────┘                                               └─────────────────────┘
///               │
///               │
///               │
///               │
///    ┌──────────▼──────────┐
///    │     ArrowSource     │
///    │          ...        │
///    │    ParquetSource    │
///    └─────────────────────┘
///               |
/// FileOpener (called by FileStream)
///               │
///    ┌──────────▼──────────┐
///    │                     │
///    │     RecordBatch     │
///    │                     │
///    └─────────────────────┘
/// ```
pub trait DataSource: Send + Sync + Debug {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;
    fn as_any(&self) -> &dyn Any;
    /// Format this source for display in explain plans
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;

    /// Return a copy of this DataSource with a new partitioning scheme.
    ///
    /// Returns `Ok(None)` (the default) if the partitioning cannot be changed.
    /// Refer to [`ExecutionPlan::repartitioned`] for details on when None should be returned.
    ///
    /// Repartitioning should not change the output ordering, if this ordering exists.
    /// Refer to [`MemorySourceConfig::repartition_preserving_order`](crate::memory::MemorySourceConfig)
    /// and the FileSource's
    /// [`FileGroupPartitioner::repartition_file_groups`](crate::file_groups::FileGroupPartitioner::repartition_file_groups)
    /// for examples.
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
    fn scheduling_type(&self) -> SchedulingType {
        SchedulingType::NonCooperative
    }

    /// Returns statistics for a specific partition, or aggregate statistics
    /// across all partitions if `partition` is `None`.
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics>;

    /// Returns aggregate statistics across all partitions.
    ///
    /// # Deprecated
    /// Use [`Self::partition_statistics`] instead, which provides more fine-grained
    /// control over statistics retrieval (per-partition or aggregate).
    #[deprecated(since = "51.0.0", note = "Use partition_statistics instead")]
    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    /// Return a copy of this DataSource with a new fetch limit
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>>;
    fn fetch(&self) -> Option<usize>;
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>>;
    /// Try to push down filters into this DataSource.
    /// See [`ExecutionPlan::handle_child_pushdown_result`] for more details.
    ///
    /// [`ExecutionPlan::handle_child_pushdown_result`]: datafusion_physical_plan::ExecutionPlan::handle_child_pushdown_result
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
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

    /// Implementation of [`ExecutionPlan::repartitioned`] which relies upon the inner [`DataSource::repartitioned`].
    ///
    /// If the data source does not support changing its partitioning, returns `Ok(None)` (the default). Refer
    /// to [`ExecutionPlan::repartitioned`] for more details.
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

        Ok(data_source.map(|source| {
            let output_partitioning = source.output_partitioning();
            let plan = self
                .clone()
                .with_data_source(source)
                // Changing source partitioning may invalidate output partitioning. Update it also
                .with_partitioning(output_partitioning);
            Arc::new(plan) as _
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.data_source.open(partition, Arc::clone(&context))?;
        let batch_size = context.session_config().batch_size();
        log::debug!(
            "Batch splitting enabled for partition {partition}: batch_size={batch_size}"
        );
        let metrics = self.data_source.metrics();
        let split_metrics = SplitMetrics::new(&metrics, partition);
        Ok(Box::pin(BatchSplitStream::new(
            stream,
            batch_size,
            split_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.data_source.metrics().clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.data_source.partition_statistics(partition)
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
        match self
            .data_source
            .try_swapping_with_projection(projection.projection_expr())?
        {
            Some(new_data_source) => {
                Ok(Some(Arc::new(DataSourceExec::new(new_data_source))))
            }
            None => Ok(None),
        }
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Push any remaining filters into our data source
        let parent_filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|f| f.filter)
            .collect_vec();
        let res = self
            .data_source
            .try_pushdown_filters(parent_filters, config)?;
        match res.updated_node {
            Some(data_source) => {
                let mut new_node = self.clone();
                new_node.data_source = data_source;
                // Re-compute properties since we have new filters which will impact equivalence info
                new_node.cache = Self::compute_properties(&new_node.data_source);

                Ok(FilterPushdownPropagation {
                    filters: res.filters,
                    updated_node: Some(Arc::new(new_node)),
                })
            }
            None => Ok(FilterPushdownPropagation {
                filters: res.filters,
                updated_node: None,
            }),
        }
    }
}

impl DataSourceExec {
    pub fn from_data_source(data_source: impl DataSource + 'static) -> Arc<Self> {
        Arc::new(Self::new(Arc::new(data_source)))
    }

    // Default constructor for `DataSourceExec`, setting the `cooperative` flag to `true`.
    pub fn new(data_source: Arc<dyn DataSource>) -> Self {
        let cache = Self::compute_properties(&data_source);
        Self { data_source, cache }
    }

    /// Return the source object
    pub fn data_source(&self) -> &Arc<dyn DataSource> {
        &self.data_source
    }

    pub fn with_data_source(mut self, data_source: Arc<dyn DataSource>) -> Self {
        self.cache = Self::compute_properties(&data_source);
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

    fn compute_properties(data_source: &Arc<dyn DataSource>) -> PlanProperties {
        PlanProperties::new(
            data_source.eq_properties(),
            data_source.output_partitioning(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(data_source.scheduling_type())
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

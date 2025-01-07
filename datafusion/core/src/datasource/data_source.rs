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

//! DataSource trait implementation

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{
    ArrowConfig, AvroConfig, CsvConfig, FileGroupPartitioner, FileOpener, FileScanConfig,
    FileStream, JsonConfig,
};
#[cfg(feature = "parquet")]
use crate::datasource::physical_plan::ParquetConfig;

use arrow_schema::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Statistics;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_physical_plan::source::{DataSource, DataSourceExec};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use itertools::Itertools;
use object_store::ObjectStore;

/// Common behaviors that every `FileSourceConfig` needs to implement.
/// Being stored as source_config in `DataSourceExec`.
pub trait DataSourceFileConfig: Send + Sync {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: datafusion_common::Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion_common::Result<Arc<dyn FileOpener>>;

    /// Any
    fn as_any(&self) -> &dyn Any;

    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn DataSourceFileConfig>;
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn DataSourceFileConfig>;
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn DataSourceFileConfig>;
}

/// Holds generic file configuration, and common behaviors.
/// Can be initialized with a `FileScanConfig`
/// and a `dyn DataSourceFileConfig` type such as `CsvConfig`, `ParquetConfig`, `AvroConfig`, etc.
#[derive(Clone)]
pub struct FileSourceConfig {
    source_config: Arc<dyn DataSourceFileConfig>,
    base_config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Statistics,
    cache: PlanProperties,
}

impl DataSource for FileSourceConfig {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url);

        let source_config = self
            .source_config
            .with_batch_size(context.session_config().batch_size())
            .with_schema(Arc::clone(&self.base_config.file_schema))
            .with_projection(&self.base_config);

        let opener = source_config.create_file_opener(
            object_store,
            &self.base_config,
            partition,
        )?;

        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.base_config.fmt_as(t, f)?;
        self.fmt_source_config(f)?;

        if let Some(csv_conf) = self.source_config.as_any().downcast_ref::<CsvConfig>() {
            return write!(f, ", has_header={}", csv_conf.has_header)
        }

        #[cfg(feature = "parquet")]
        if cfg!(feature = "parquet") {
            if let Some(parquet_conf) =
                self.source_config.as_any().downcast_ref::<ParquetConfig>()
            {
                match t {
                    DisplayFormatType::Default | DisplayFormatType::Verbose => {
                        let predicate_string = parquet_conf
                            .predicate()
                            .map(|p| format!(", predicate={p}"))
                            .unwrap_or_default();

                        let pruning_predicate_string = parquet_conf
                            .pruning_predicate()
                            .map(|pre| {
                                let mut guarantees = pre
                                    .literal_guarantees()
                                    .iter()
                                    .map(|item| format!("{}", item))
                                    .collect_vec();
                                guarantees.sort();
                                format!(
                                    ", pruning_predicate={}, required_guarantees=[{}]",
                                    pre.predicate_expr(),
                                    guarantees.join(", ")
                                )
                            })
                            .unwrap_or_default();

                        return write!(f, "{}{}", predicate_string, pruning_predicate_string,)
                    }
                }
            }
        }
        Ok(())
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
        exec: DataSourceExec,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if !self.supports_repartition() {
            return Ok(None);
        }
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;

        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(self.cache().output_ordering().is_some())
            .repartition_file_groups(&self.base_config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let plan = Arc::new(exec.with_source(Arc::new(
                self.clone().with_file_groups(repartitioned_file_groups),
            )));
            return Ok(Some(plan));
        }
        Ok(None)
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {

        #[cfg(not(feature = "parquet"))]
        let stats = self.projected_statistics.clone();

        #[cfg(feature = "parquet")]
        let stats =
            if let Some(parquet_config) = self.source_config.as_any().downcast_ref::<ParquetConfig>() {
                // When filters are pushed down, we have no way of knowing the exact statistics.
                // Note that pruning predicate is also a kind of filter pushdown.
                // (bloom filters use `pruning_predicate` too)
                if parquet_config.pruning_predicate().is_some()
                    || parquet_config.page_pruning_predicate().is_some()
                    || (parquet_config.predicate().is_some()
                    && parquet_config.pushdown_filters())
                {
                    self.projected_statistics.clone().to_inexact()
                } else {
                    self.projected_statistics.clone()
                }
            } else {
                self.projected_statistics.clone()
            };

        Ok(stats)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let config = self.base_config.clone().with_limit(limit);
        Some(Arc::new(Self {
            source_config: Arc::clone(&self.source_config),
            base_config: config,
            metrics: self.metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            cache: self.cache(),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.base_config.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn properties(&self) -> PlanProperties {
        self.cache()
    }
}

impl FileSourceConfig {
    /// Returns a new [`DataSourceExec`] from file configurations
    pub fn new_exec(
        base_config: FileScanConfig,
        source_config: Arc<dyn DataSourceFileConfig>,
    ) -> Arc<DataSourceExec> {
        let source = Arc::new(Self::new(base_config, source_config));

        Arc::new(DataSourceExec::new(source))
    }

    /// Initialize a new `FileSourceConfig` instance with metrics, cache, and statistics.
    pub fn new(
        base_config: FileScanConfig,
        source_config: Arc<dyn DataSourceFileConfig>,
    ) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            &base_config,
        );
        let mut metrics = ExecutionPlanMetricsSet::new();

        #[cfg(feature = "parquet")]
        if let Some(parquet_config) =
            source_config.as_any().downcast_ref::<ParquetConfig>()
        {
            metrics = parquet_config.metrics();
            let _predicate_creation_errors = MetricBuilder::new(&metrics)
                .global_counter("num_predicate_creation_errors");
        };

        Self {
            source_config,
            base_config,
            metrics,
            projected_statistics,
            cache,
        }
    }

    /// Write the data_type based on source_config
    fn fmt_source_config(&self, f: &mut Formatter) -> fmt::Result {
        let mut data_type = if self
            .source_config
            .as_any()
            .downcast_ref::<AvroConfig>()
            .is_some()
        {
            "avro"
        } else if self
            .source_config
            .as_any()
            .downcast_ref::<ArrowConfig>()
            .is_some()
        {
            "arrow"
        } else if self
            .source_config
            .as_any()
            .downcast_ref::<CsvConfig>()
            .is_some()
        {
            "csv"
        } else if self
            .source_config
            .as_any()
            .downcast_ref::<JsonConfig>()
            .is_some()
        {
            "json"
        } else {
            "unknown"
        };
        #[cfg(feature = "parquet")]
        if self.source_config.as_any().downcast_ref::<ParquetConfig>().is_some() {
            data_type = "parquet";
        }
        write!(f, ", file_type={}", data_type)
    }

    /// Returns the base_config
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Returns the source_config
    pub fn source_config(&self) -> &Arc<dyn DataSourceFileConfig> {
        &self.source_config
    }

    /// Returns the `PlanProperties` of the plan
    pub(crate) fn cache(&self) -> PlanProperties {
        self.cache.clone()
    }

    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups;
        // Changing file groups may invalidate output partitioning. Update it also
        let output_partitioning = Self::output_partitioning_helper(&self.base_config);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }

    fn supports_repartition(&self) -> bool {
        !(self.base_config.file_compression_type.is_compressed()
            || self.base_config.new_lines_in_values
            || self
                .source_config
                .as_any()
                .downcast_ref::<AvroConfig>()
                .is_some())
    }
}

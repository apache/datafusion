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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::avro_to_arrow::Reader as AvroReader;

use datafusion_common::error::Result;

use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraints, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use datafusion_physical_plan::statistics::PartitionedStatistics;
use object_store::ObjectStore;

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
pub struct AvroExec {
    inner: DataSourceExec,
    base_config: FileScanConfig,
}

#[allow(unused, deprecated)]
impl AvroExec {
    /// Create a new Avro reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (
            projected_schema,
            projected_constraints,
            projected_statistics,
            projected_output_ordering,
        ) = base_config.project();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            projected_constraints,
            &base_config,
        );
        let base_config = base_config.with_source(Arc::new(AvroSource::default()));
        Self {
            inner: DataSourceExec::new(Arc::new(base_config.clone())),
            base_config,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings)
            .with_constraints(constraints);
        let n_partitions = file_scan_config.file_groups.len();

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(n_partitions), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for AvroExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for AvroExec {
    fn name(&self) -> &'static str {
        "AvroExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        self.inner.statistics_by_partition()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit)
    }
}

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone, Default)]
pub struct AvroSource {
    schema: Option<SchemaRef>,
    batch_size: Option<usize>,
    projection: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl AvroSource {
    /// Initialize an AvroSource with default values
    pub fn new() -> Self {
        Self::default()
    }

    fn open<R: std::io::Read>(&self, reader: R) -> Result<AvroReader<'static, R>> {
        AvroReader::try_new(
            reader,
            Arc::clone(self.schema.as_ref().expect("Schema must set before open")),
            self.batch_size.expect("Batch size must set before open"),
            self.projection.clone(),
        )
    }
}

impl FileSource for AvroSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(private::AvroOpener {
            config: Arc::new(self.clone()),
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.schema = Some(schema);
        Arc::new(conf)
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projection = config.projected_file_column_names();
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "avro"
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        Ok(None)
    }
}

mod private {
    use super::*;

    use bytes::Buf;
    use datafusion_datasource::{file_meta::FileMeta, file_stream::FileOpenFuture};
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            let object_store = Arc::clone(&self.object_store);
            Ok(Box::pin(async move {
                let r = object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }
}

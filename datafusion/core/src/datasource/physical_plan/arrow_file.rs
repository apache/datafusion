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

//! Execution plan for reading Arrow files

use std::any::Any;
use std::sync::Arc;

use crate::datasource::data_source::FileSource;
use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, JsonSource,
};
use crate::error::Result;

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::FileDecoder;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Constraints, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::source::DataSourceExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use futures::StreamExt;
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// Execution plan for scanning Arrow data source
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
pub struct ArrowExec {
    inner: DataSourceExec,
    base_config: FileScanConfig,
}

#[allow(unused, deprecated)]
impl ArrowExec {
    /// Create a new Arrow reader execution plan provided base configurations
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
        let arrow = ArrowSource::default();
        let base_config = base_config.with_source(Arc::new(arrow));
        Self {
            inner: DataSourceExec::new(Arc::new(base_config.clone())),
            base_config,
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    fn file_scan_config(&self) -> FileScanConfig {
        let source = self.inner.source();
        source
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap()
            .clone()
    }

    fn json_source(&self) -> JsonSource {
        let source = self.file_scan_config();
        source
            .file_source()
            .as_any()
            .downcast_ref::<JsonSource>()
            .unwrap()
            .clone()
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        output_ordering: &[LexOrdering],
        constraints: Constraints,
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties =
            EquivalenceProperties::new_with_orderings(schema, output_ordering)
                .with_constraints(constraints);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups.clone();
        let mut file_source = self.file_scan_config();
        file_source = file_source.with_file_groups(file_groups);
        self.inner = self.inner.with_source(Arc::new(file_source));
        self
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for ArrowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for ArrowExec {
    fn name(&self) -> &'static str {
        "ArrowExec"
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

    /// Redistribute files across partitions according to their size
    /// See comments on `FileGroupPartitioner` for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.repartitioned(target_partitions, config)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }
    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit)
    }
}

/// Arrow configuration struct that is given to DataSourceExec
/// Does not hold anything special, since [`FileScanConfig`] is sufficient for arrow
#[derive(Clone, Default)]
pub struct ArrowSource {
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl FileSource for ArrowSource {
    fn create_file_opener(
        &self,
        object_store: Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(ArrowOpener {
            object_store: object_store?,
            projection: base_config.file_column_projection_indices(),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
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
        "arrow"
    }

    fn supports_repartition(&self, config: &FileScanConfig) -> bool {
        !(config.file_compression_type.is_compressed() || config.new_lines_in_values)
    }
}

/// The struct arrow that implements `[FileOpener]` trait
pub struct ArrowOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub projection: Option<Vec<usize>>,
}

impl FileOpener for ArrowOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let range = file_meta.range.clone();
            match range {
                None => {
                    let r = object_store.get(file_meta.location()).await?;
                    match r.payload {
                        GetResultPayload::File(file, _) => {
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                file, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader).boxed())
                        }
                        GetResultPayload::Stream(_) => {
                            let bytes = r.bytes().await?;
                            let cursor = std::io::Cursor::new(bytes);
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                cursor, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader).boxed())
                        }
                    }
                }
                Some(range) => {
                    // range is not none, the file maybe split into multiple parts to scan in parallel
                    // get footer_len firstly
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10)),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(file_meta.location(), get_option)
                        .await?;
                    let footer_len_buf = get_result.bytes().await?;
                    let footer_len = arrow_ipc::reader::read_footer_length(
                        footer_len_buf[..].try_into().unwrap(),
                    )?;
                    // read footer according to footer_len
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10 + footer_len)),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(file_meta.location(), get_option)
                        .await?;
                    let footer_buf = get_result.bytes().await?;
                    let footer = arrow_ipc::root_as_footer(
                        footer_buf[..footer_len].try_into().unwrap(),
                    )
                    .map_err(|err| {
                        arrow::error::ArrowError::ParseError(format!(
                            "Unable to get root as footer: {err:?}"
                        ))
                    })?;
                    // build decoder according to footer & projection
                    let schema =
                        arrow_ipc::convert::fb_to_schema(footer.schema().unwrap());
                    let mut decoder = FileDecoder::new(schema.into(), footer.version());
                    if let Some(projection) = projection {
                        decoder = decoder.with_projection(projection);
                    }
                    let dict_ranges = footer
                        .dictionaries()
                        .iter()
                        .flatten()
                        .map(|block| {
                            let block_len = block.bodyLength() as usize
                                + block.metaDataLength() as usize;
                            let block_offset = block.offset() as usize;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();
                    let dict_results = object_store
                        .get_ranges(file_meta.location(), &dict_ranges)
                        .await?;
                    for (dict_block, dict_result) in
                        footer.dictionaries().iter().flatten().zip(dict_results)
                    {
                        decoder
                            .read_dictionary(dict_block, &Buffer::from(dict_result))?;
                    }

                    // filter recordbatches according to range
                    let recordbatches = footer
                        .recordBatches()
                        .iter()
                        .flatten()
                        .filter(|block| {
                            let block_offset = block.offset() as usize;
                            block_offset >= range.start as usize
                                && block_offset < range.end as usize
                        })
                        .copied()
                        .collect_vec();

                    let recordbatch_ranges = recordbatches
                        .iter()
                        .map(|block| {
                            let block_len = block.bodyLength() as usize
                                + block.metaDataLength() as usize;
                            let block_offset = block.offset() as usize;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();

                    let recordbatch_results = object_store
                        .get_ranges(file_meta.location(), &recordbatch_ranges)
                        .await?;

                    Ok(futures::stream::iter(
                        recordbatches
                            .into_iter()
                            .zip(recordbatch_results)
                            .filter_map(move |(block, data)| {
                                decoder
                                    .read_record_batch(&block, &Buffer::from(data))
                                    .transpose()
                            }),
                    )
                    .boxed())
                }
            }
        }))
    }
}

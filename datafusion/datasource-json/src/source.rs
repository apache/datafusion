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

//! Execution plan for reading line-delimited JSON files

use std::any::Any;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use crate::file_format::JsonDecoder;

use datafusion_common::error::{DataFusionError, Result};
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::decoder::{deserialize_stream, DecoderDeserializer};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::{
    as_file_source, calculate_range, ListingTableUrl, PartitionedFile, RangeCalculation,
};
use datafusion_physical_plan::projection::ProjectionExprs;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow::json::ReaderBuilder;
use arrow::{datatypes::SchemaRef, json};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl JsonOpener {
    /// Returns a  [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            file_compression_type,
            object_store,
        }
    }
}

/// JsonSource holds the extra configuration that is necessary for [`JsonOpener`]
#[derive(Clone)]
pub struct JsonSource {
    table_schema: datafusion_datasource::TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    projection: SplitProjection,
}

impl JsonSource {
    /// Initialize a JsonSource with the provided schema
    pub fn new(table_schema: impl Into<datafusion_datasource::TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
        }
    }
}

impl From<JsonSource> for Arc<dyn FileSource> {
    fn from(source: JsonSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for JsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Get the projected file schema for JsonOpener
        let file_schema = self.table_schema.file_schema();
        let projected_schema =
            Arc::new(file_schema.project(&self.projection.file_indices)?);

        let mut opener = Arc::new(JsonOpener {
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating opener"),
            projected_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
        }) as Arc<dyn FileOpener>;

        // Wrap with ProjectionOpener
        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

impl FileOpener for JsonOpener {
    /// Open a partitioned NDJSON file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// Else `file_meta.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range(&partitioned_file, &store, None).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    )
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes = match partitioned_file.range {
                        None => file_compression_type.convert_read(file)?,
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            file_compression_type.convert_read(file.take(limit as u64))?
                        }
                    };

                    let reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(BufReader::new(bytes))?;

                    Ok(futures::stream::iter(reader)
                        .map(|r| r.map_err(Into::into))
                        .boxed())
                }
                GetResultPayload::Stream(s) => {
                    let s = s.map_err(DataFusionError::from);

                    let decoder = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build_decoder()?;
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    let stream = deserialize_stream(
                        input,
                        DecoderDeserializer::new(JsonDecoder::new(decoder)),
                    );
                    Ok(stream.map_err(Into::into).boxed())
                }
            }
        }))
    }
}

pub async fn plan_to_json(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let writer_buffer_size = task_ctx
        .session_config()
        .options()
        .execution
        .objectstore_writer_buffer_size;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.json", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer =
                BufWriter::with_capacity(storeref, file.clone(), writer_buffer_size);

            let mut buffer = Vec::with_capacity(1024);
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = json::LineDelimitedWriter::new(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
            }

            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

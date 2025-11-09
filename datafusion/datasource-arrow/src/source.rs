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
use std::sync::Arc;

use datafusion_datasource::as_file_source;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;

use arrow::buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use datafusion_common::error::Result;
use datafusion_common::{exec_datafusion_err, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use futures::StreamExt;
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// Arrow configuration struct that is given to DataSourceExec
/// Does not hold anything special, since [`FileScanConfig`] is sufficient for arrow
#[derive(Clone)]
pub struct ArrowSource {
    table_schema: datafusion_datasource::TableSchema,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl ArrowSource {
    /// Initialize an ArrowSource with the provided schema
    pub fn new(table_schema: impl Into<datafusion_datasource::TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            schema_adapter_factory: None,
        }
    }
}

impl From<ArrowSource> for Arc<dyn FileSource> {
    fn from(source: ArrowSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for ArrowSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(ArrowOpener {
            object_store,
            projection: base_config.file_column_projection_indices(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
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

/// The struct arrow that implements `[FileOpener]` trait
pub struct ArrowOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub projection: Option<Vec<usize>>,
}

impl FileOpener for ArrowOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let range = partitioned_file.range.clone();
            match range {
                None => {
                    let r = object_store
                        .get(&partitioned_file.object_meta.location)
                        .await?;
                    match r.payload {
                        #[cfg(not(target_arch = "wasm32"))]
                        GetResultPayload::File(file, _) => {
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                file, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader)
                                .map(|r| r.map_err(Into::into))
                                .boxed())
                        }
                        GetResultPayload::Stream(_) => {
                            let bytes = r.bytes().await?;
                            let cursor = std::io::Cursor::new(bytes);
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                cursor, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader)
                                .map(|r| r.map_err(Into::into))
                                .boxed())
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
                        .get_opts(&partitioned_file.object_meta.location, get_option)
                        .await?;
                    let footer_len_buf = get_result.bytes().await?;
                    let footer_len = arrow_ipc::reader::read_footer_length(
                        footer_len_buf[..].try_into().unwrap(),
                    )?;
                    // read footer according to footer_len
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10 + (footer_len as u64))),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(&partitioned_file.object_meta.location, get_option)
                        .await?;
                    let footer_buf = get_result.bytes().await?;
                    let footer = arrow_ipc::root_as_footer(
                        footer_buf[..footer_len].try_into().unwrap(),
                    )
                    .map_err(|err| {
                        exec_datafusion_err!("Unable to get root as footer: {err:?}")
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
                            let block_len =
                                block.bodyLength() as u64 + block.metaDataLength() as u64;
                            let block_offset = block.offset() as u64;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();
                    let dict_results = object_store
                        .get_ranges(&partitioned_file.object_meta.location, &dict_ranges)
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
                            let block_offset = block.offset() as u64;
                            block_offset >= range.start as u64
                                && block_offset < range.end as u64
                        })
                        .copied()
                        .collect_vec();

                    let recordbatch_ranges = recordbatches
                        .iter()
                        .map(|block| {
                            let block_len =
                                block.bodyLength() as u64 + block.metaDataLength() as u64;
                            let block_offset = block.offset() as u64;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();

                    let recordbatch_results = object_store
                        .get_ranges(
                            &partitioned_file.object_meta.location,
                            &recordbatch_ranges,
                        )
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
                    .map(|r| r.map_err(Into::into))
                    .boxed())
                }
            }
        }))
    }
}

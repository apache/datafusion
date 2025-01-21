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

use crate::datasource::data_source::{FileSource, FileType};
use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig,
};
use crate::error::Result;

use arrow::buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use arrow_schema::SchemaRef;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use datafusion_common::Statistics;
use futures::StreamExt;
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// Arrow configuration struct that is given to DataSourceExec
/// Does not hold anything special, since [`FileScanConfig`] is sufficient for arrow
#[derive(Clone, Default)]
pub struct ArrowConfig {
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl FileSource for ArrowConfig {
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

    fn file_type(&self) -> FileType {
        FileType::Arrow
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
                        arrow_schema::ArrowError::ParseError(format!(
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
                        decoder.read_dictionary(
                            dict_block,
                            &Buffer::from_bytes(dict_result.into()),
                        )?;
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
                                    .read_record_batch(
                                        &block,
                                        &Buffer::from_bytes(data.into()),
                                    )
                                    .transpose()
                            }),
                    )
                    .boxed())
                }
            }
        }))
    }
}

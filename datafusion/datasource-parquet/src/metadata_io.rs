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

//! Shared metadata decoder driver for all file access implementations.
use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use parquet::DecodeResult;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
use std::{future::Future, ops::Range, sync::Arc};

pub(crate) async fn decode_metadata<F, Fut>(
    mut decoder: ParquetMetaDataPushDecoder,
    file_size: u64,
    hint: Option<usize>,
    mut read: F,
) -> Result<Arc<ParquetMetaData>>
where
    F: FnMut(Vec<Range<u64>>) -> Fut,
    Fut: Future<Output = Result<Vec<Bytes>>>,
{
    if let Some(hint) = hint.filter(|hint| *hint > 0) {
        let range = file_size.saturating_sub(hint as u64)..file_size;
        let ranges = vec![range];
        let buffers = read(ranges.clone()).await?;
        decoder.push_ranges(ranges, buffers)?;
    }
    loop {
        match decoder.try_decode()? {
            DecodeResult::Data(metadata) => return Ok(Arc::new(metadata)),
            DecodeResult::NeedsData(ranges) => {
                let buffers = read(ranges.clone()).await?;
                decoder.push_ranges(ranges, buffers)?;
            }
            DecodeResult::Finished => {
                return Err(DataFusionError::Internal(
                    "Parquet metadata decoder finished without producing metadata".into(),
                ));
            }
        }
    }
}

/// Adapt a file reader to the Parquet page-index fetch interface.
pub(crate) struct FileReaderFetch<'a>(&'a dyn datafusion_storage::FileReader);
impl<'a> FileReaderFetch<'a> {
    pub(crate) fn new(reader: &'a dyn datafusion_storage::FileReader) -> Self {
        Self(reader)
    }
}
impl parquet::arrow::async_reader::MetadataFetch for FileReaderFetch<'_> {
    fn fetch(
        &mut self,
        range: Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            self.0
                .read_range(range.into())
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
        })
    }
}
